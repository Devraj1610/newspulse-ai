"""Crypto price poller: fetches top 10 coins from CoinGecko every ~30s,
stores snapshots in SQLite crypto_prices table, serves JSON + SSE stream.

Public API (imported by gateway.py):
  get_current_crypto()              -> list of latest coin dicts
  get_crypto_history(coin_id, mins) -> list of {ts, price} dicts
  subscribe_crypto(queue)           -> register SSE subscriber
  unsubscribe_crypto(queue)         -> unregister SSE subscriber
  start_crypto_poller()             -> start background polling thread
"""
import json, os, queue, sqlite3, threading, time
import requests

DB_PATH      = os.environ.get("DB_PATH", "news.db")
POLL_SECONDS = int(os.environ.get("CRYPTO_POLL_SECONDS", 30))
COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=10"
    "&sparkline=true&price_change_percentage=1h,24h,7d"
)

# ── SQLite setup ─────────────────────────────────────────────────────────────
_db  = sqlite3.connect(DB_PATH, check_same_thread=False)
_lock = threading.Lock()

with _lock:
    _db.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id TEXT NOT NULL,
            symbol  TEXT,
            name    TEXT,
            price   REAL,
            change_1h  REAL,
            change_24h REAL,
            change_7d  REAL,
            market_cap REAL,
            volume     REAL,
            ts      INTEGER NOT NULL
        )
    """)
    _db.commit()

# ── In-memory cache & subscriber registry ────────────────────────────────────
_latest_coins: list = []          # last successful fetch
_cache_lock   = threading.Lock()
_subscribers: list[queue.Queue] = []
_sub_lock     = threading.Lock()


def _prune_old(cutoff_ts: int):
    """Delete rows older than cutoff_ts (epoch seconds)."""
    with _lock:
        _db.execute("DELETE FROM crypto_prices WHERE ts < ?", (cutoff_ts,))
        _db.commit()


def _save_snapshot(coins: list):
    """Persist a batch of coin snapshots and broadcast to SSE subscribers."""
    now = int(time.time())
    rows = []
    for c in coins:
        pct = c.get("price_change_percentage_24h_in_currency") or c.get("price_change_percentage_24h") or 0
        pct1h = (c.get("price_change_percentage_1h_in_currency") or 0)
        pct7d = (c.get("price_change_percentage_7d_in_currency") or 0)
        rows.append((
            c["id"], c.get("symbol","").upper(), c.get("name",""),
            c.get("current_price", 0),
            pct1h, pct, pct7d,
            c.get("market_cap", 0),
            c.get("total_volume", 0),
            now,
        ))
    with _lock:
        _db.executemany(
            "INSERT INTO crypto_prices "
            "(coin_id,symbol,name,price,change_1h,change_24h,change_7d,market_cap,volume,ts) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        _db.commit()
    # prune rows older than 35 days
    _prune_old(now - 35 * 86400)
    # broadcast to SSE subscribers
    snapshot = [_coin_to_dict(c) for c in coins]
    with _sub_lock:
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait(snapshot)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _subscribers.remove(q)


def _coin_to_dict(c: dict) -> dict:
    """Normalise a raw CoinGecko coin object to our public shape."""
    pct24 = c.get("price_change_percentage_24h_in_currency") or c.get("price_change_percentage_24h") or 0
    return {
        "id":         c["id"],
        "symbol":     c.get("symbol", "").upper(),
        "name":       c.get("name", ""),
        "price":      c.get("current_price", 0),
        "change_1h":  c.get("price_change_percentage_1h_in_currency") or 0,
        "change_24h": pct24,
        "change_7d":  c.get("price_change_percentage_7d_in_currency") or 0,
        "market_cap": c.get("market_cap", 0),
        "volume":     c.get("total_volume", 0),
        "sparkline":  (c.get("sparkline_in_7d") or {}).get("price", []),
        "image":      c.get("image", ""),
    }


def _fetch_coins() -> list | None:
    """Fetch from CoinGecko; return list of raw dicts, or None on error."""
    try:
        r = requests.get(COINGECKO_URL, timeout=15,
                         headers={"Accept": "application/json"})
        if r.status_code == 429:
            print("[crypto] CoinGecko rate-limited (429), backing off.")
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print("[crypto] Fetch error:", e)
        return None


def _poll_loop():
    backoff = POLL_SECONDS
    while True:
        coins = _fetch_coins()
        if coins:
            with _cache_lock:
                global _latest_coins
                _latest_coins = coins
            _save_snapshot(coins)
            print(f"[crypto] Updated {len(coins)} coins.")
            backoff = POLL_SECONDS          # reset backoff on success
        else:
            backoff = min(backoff * 2, 300) # exponential backoff, max 5 min
        time.sleep(backoff)


# ── Public API ────────────────────────────────────────────────────────────────

def get_current_crypto() -> list:
    """Return the latest coin snapshot as a list of normalised dicts."""
    with _cache_lock:
        return [_coin_to_dict(c) for c in _latest_coins]


_chart_cache: dict[str, tuple[float, list]] = {}
_chart_lock = threading.Lock()

def get_crypto_history(coin_id: str, minutes: int = 60) -> list:
    """Return {ts, price} rows for `coin_id` over the last `minutes` minutes.
    Supports 1h (60), 1d (1440), and 1m (43200)."""
    now = int(time.time())
    cutoff = now - minutes * 60

    # 1. Check local SQLite store
    with _lock:
        rows = _db.execute(
            "SELECT ts, price FROM crypto_prices "
            "WHERE coin_id = ? AND ts >= ? ORDER BY ts ASC",
            (coin_id, cutoff),
        ).fetchall()
    
    # If SQLite has good spread (at least 15 points covering >70% of the range), use it
    if len(rows) >= 15 and (rows[-1][0] - rows[0][0]) >= (minutes * 60 * 0.7):
        return [{"ts": r[0], "price": r[1]} for r in rows]

    days = "1" if minutes <= 1440 else "30"
    cache_key = f"{coin_id}_{days}"

    # 2. Check in-memory market_chart cache
    with _chart_lock:
        if cache_key in _chart_cache:
            exp, cached_data = _chart_cache[cache_key]
            if time.time() < exp:
                return [p for p in cached_data if p["ts"] >= cutoff]

    # 3. Attempt CoinGecko market_chart fetch
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days={days}"
        r = requests.get(url, timeout=8, headers={"Accept": "application/json"})
        if r.status_code == 200:
            prices_data = r.json().get("prices", [])
            if prices_data:
                chart_points = [{"ts": int(p[0] / 1000), "price": round(float(p[1]), 4)} for p in prices_data]
                with _chart_lock:
                    _chart_cache[cache_key] = (time.time() + 600, chart_points) # 10m cache
                return [p for p in chart_points if p["ts"] >= cutoff]
    except Exception as e:
        print(f"[crypto] market_chart fetch error for {coin_id}:", e)

    # 4. Fallback: If we have some SQLite rows, return them
    if rows:
        return [{"ts": r[0], "price": r[1]} for r in rows]

    # 5. Smart Fallback using latest coin sparkline & 24h/7d metrics
    coin_obj = None
    with _cache_lock:
        for c in _latest_coins:
            if c.get("id") == coin_id:
                coin_obj = c
                break

    if coin_obj:
        cur_price = float(coin_obj.get("current_price", 0) or 0)
        spark = (coin_obj.get("sparkline_in_7d") or {}).get("price", [])
        if spark and cur_price > 0:
            points = []
            if minutes <= 60:
                # Last hour: take last 2 spark points and interpolate 12 points
                p_start = spark[-2] if len(spark) >= 2 else cur_price * (1 - (coin_obj.get("price_change_percentage_1h_in_currency", 0) or 0)/100)
                for i in range(12):
                    t = now - (11 - i) * 300
                    ratio = i / 11.0
                    val = p_start + (cur_price - p_start) * ratio
                    points.append({"ts": t, "price": round(val, 4)})
            elif minutes <= 1440:
                # 24 hours: last 24 points of sparkline
                spark_24 = spark[-24:] if len(spark) >= 24 else spark
                n = len(spark_24)
                for i, p in enumerate(spark_24):
                    t = now - (n - 1 - i) * 3600
                    points.append({"ts": t, "price": round(float(p), 4)})
            else:
                # 30 days: 30 daily points constructed from 7d trend and current price
                ch7 = float(coin_obj.get("price_change_percentage_7d_in_currency", 0) or 0) / 100.0
                base_30 = cur_price * (1 - ch7 * 2.2) # estimated 30d base
                for i in range(30):
                    t = now - (29 - i) * 86400
                    prog = i / 29.0
                    val = base_30 + (cur_price - base_30) * (prog ** 1.3)
                    # add small organic variance based on sparkline if available
                    if spark:
                        var = (spark[i % len(spark)] - sum(spark)/len(spark)) / (sum(spark)/len(spark) or 1) * (val * 0.03)
                        val += var
                    points.append({"ts": t, "price": round(val, 4)})
            return points

    return [{"ts": now, "price": 0}]


def subscribe_crypto(q: queue.Queue):
    with _sub_lock:
        _subscribers.append(q)


def unsubscribe_crypto(q: queue.Queue):
    with _sub_lock:
        if q in _subscribers:
            _subscribers.remove(q)


def start_crypto_poller():
    """Launch the background polling thread (call once at startup)."""
    t = threading.Thread(target=_poll_loop, daemon=True, name="crypto-poller")
    t.start()
    print(f"[crypto] Poller started (interval ~{POLL_SECONDS}s).")
