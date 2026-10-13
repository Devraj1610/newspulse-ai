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
    # prune rows older than 24 h
    _prune_old(now - 86400)
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


def get_crypto_history(coin_id: str, minutes: int = 60) -> list:
    """Return {ts, price} rows for `coin_id` over the last `minutes` minutes."""
    cutoff = int(time.time()) - minutes * 60
    with _lock:
        rows = _db.execute(
            "SELECT ts, price FROM crypto_prices "
            "WHERE coin_id = ? AND ts >= ? ORDER BY ts ASC",
            (coin_id, cutoff),
        ).fetchall()
    return [{"ts": r[0], "price": r[1]} for r in rows]


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
