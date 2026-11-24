"""Live news headlines backend: polls Hacker News + publisher RSS feeds (TechCrunch, The Verge, Ars Technica),
stores in SQLite, serves JSON + live SSE stream.

Run:  python news_backend.py        (needs: pip install requests)
API:  GET /headlines?limit=20       newest first, JSON
      GET /headlines/stream         Server-Sent Events, one event per new headline
"""
import datetime, email.utils, hashlib, json, os, queue, sqlite3, threading, time
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import requests

POLL_SECONDS     = int(os.environ.get("POLL_SECONDS", 30))    # how often to check HN
RSS_POLL_SECONDS = int(os.environ.get("RSS_POLL_SECONDS", 300))  # how often to check RSS feeds (5 min)
RSS_MAX_AGE_DAYS = int(os.environ.get("RSS_MAX_AGE_DAYS", 7))    # keep recent articles up to 7 days
MAX_STORIES      = int(os.environ.get("MAX_STORIES", 100))    # maximum stories to keep in feed
TOP_N    = int(os.environ.get("TOP_N", 30))                   # how many top HN stories to track
FEED_URL = f"https://hn.algolia.com/api/v1/search?tags=front_page&hitsPerPage={TOP_N}"
PORT     = int(os.environ.get("PORT", 8000))
DB_PATH  = os.environ.get("DB_PATH", "news.db")

# Publisher RSS feeds: TechCrunch category feeds (Startups, Venture, Security, AI, Main) + broad tech coverage
RSS_FEEDS = [
    ("TechCrunch Startups", "https://techcrunch.com/category/startups/feed/"),
    ("TechCrunch Venture",  "https://techcrunch.com/category/venture/feed/"),
    ("TechCrunch Security", "https://techcrunch.com/category/security/feed/"),
    ("TechCrunch AI",       "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("TechCrunch",          "https://techcrunch.com/feed/"),
    ("The Verge",           "https://www.theverge.com/rss/index.xml"),
    ("Ars Technica",        "https://feeds.arstechnica.com/arstechnica/index"),
]

db = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
db_lock = threading.Lock()
try:
    db.execute("PRAGMA journal_mode=WAL;")
except Exception:
    pass

# Create table — add `source` column if the DB was created by an older version
db.execute("""CREATE TABLE IF NOT EXISTS headlines (
    id INTEGER PRIMARY KEY, title TEXT, url TEXT, time INTEGER, score INTEGER,
    author TEXT, source TEXT)""")
db.commit()

# Migrate: add `source` column to existing DBs that don't have it yet
try:
    db.execute("ALTER TABLE headlines ADD COLUMN source TEXT")
    db.commit()
except Exception:
    pass  # column already exists — fine

# Backfill any existing NULL/empty source fields
try:
    with db_lock:
        db.execute("UPDATE headlines SET source = 'TechCrunch' WHERE url LIKE '%techcrunch.com%' AND (source IS NULL OR source = '')")
        db.execute("UPDATE headlines SET source = 'The Verge' WHERE url LIKE '%theverge.com%' AND (source IS NULL OR source = '')")
        db.execute("UPDATE headlines SET source = 'Ars Technica' WHERE url LIKE '%arstechnica.com%' AND (source IS NULL OR source = '')")
        db.execute("UPDATE headlines SET source = 'HN' WHERE source IS NULL OR source = ''")
        db.commit()
except Exception:
    pass

subscribers = []                # one queue per connected live client
sub_lock    = threading.Lock()
_last_rss_poll = 0              # tracks when we last polled RSS feeds


def _parse_pub_date(date_str: str) -> int:
    """Parse various RSS/Atom date formats into unix epoch seconds."""
    if not date_str:
        return int(time.time())
    try:
        return int(email.utils.parsedate_to_datetime(date_str).timestamp())
    except Exception:
        pass
    try:
        dt = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return int(time.time())


def _rss_id(url: str) -> int:
    """Generate a positive integer hash from URL so it safely fits SQLite INTEGER PRIMARY KEY."""
    return int(hashlib.md5(url.encode("utf-8")).hexdigest()[:13], 16)


def fetch_json(url):
    r = requests.get(url, headers={"User-Agent": "NewsPulse/2.0"}, timeout=15)
    r.raise_for_status()
    return r.json()


def fetch_rss_feed(source_name: str, feed_url: str) -> list:
    """Fetch and parse RSS 2.0 or Atom feeds into headline dicts. Skips articles older than RSS_MAX_AGE_DAYS."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NewsPulse/2.0"}
    r = requests.get(feed_url, headers=headers, timeout=12)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    parsed_items = []
    cutoff = int(time.time()) - RSS_MAX_AGE_DAYS * 86400   # only keep recent articles

    # RSS 2.0 format (TechCrunch, Ars Technica)
    if "rss" in root.tag.lower():
        channel = root.find("channel")
        for item in (channel.findall("item") if channel is not None else []):
            title    = (item.findtext("title") or "").strip()
            link     = (item.findtext("link") or "").strip()
            pub_date = item.findtext("pubDate") or ""
            author   = (item.findtext("{http://purl.org/dc/elements/1.1/}creator") or
                        item.findtext("author") or source_name).strip()
            pub_ts   = _parse_pub_date(pub_date)
            if title and link and pub_ts >= cutoff:
                parsed_items.append({
                    "id":     _rss_id(link),
                    "title":  title,
                    "url":    link,
                    "time":   pub_ts,
                    "score":  1,
                    "by":     author or source_name,
                    "source": source_name,
                })

    # Atom format (The Verge)
    elif "feed" in root.tag.lower():
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall("atom:entry", ns):
            title   = (entry.findtext("atom:title", namespaces=ns) or "").strip()
            link_el = entry.find("atom:link[@rel='alternate']", namespaces=ns)
            if link_el is None:
                link_el = entry.find("atom:link", namespaces=ns)
            link     = (link_el.get("href", "") if link_el is not None else "").strip()
            pub_date = (entry.findtext("atom:published", namespaces=ns) or
                        entry.findtext("atom:updated",   namespaces=ns) or "")
            author_el = entry.find("atom:author/atom:name", ns)
            author    = (author_el.text if author_el is not None and author_el.text else source_name).strip()
            pub_ts    = _parse_pub_date(pub_date)
            if title and link and pub_ts >= cutoff:
                parsed_items.append({
                    "id":     _rss_id(link),
                    "title":  title,
                    "url":    link,
                    "time":   pub_ts,
                    "score":  1,
                    "by":     author or source_name,
                    "source": source_name,
                })

    return parsed_items


def save(item):
    """Insert one story; if new, enforce MAX_STORIES (100) limit by pushing out / removing the oldest."""
    row = (item["id"], item.get("title"), item.get("url"), item.get("time"),
           item.get("score"), item.get("by"), item.get("source", "HN"))
    with db_lock:
        cur = db.execute("INSERT OR IGNORE INTO headlines (id, title, url, time, score, author, source) VALUES (?,?,?,?,?,?,?)", row)
        db.commit()
        if cur.rowcount == 0:
            return None
        # Enforce max 100 stories: whenever a new story is added, push out the 100th+ story
        db.execute(f"""
            DELETE FROM headlines WHERE id NOT IN (
                SELECT id FROM headlines ORDER BY time DESC LIMIT {MAX_STORIES}
            )
        """)
        db.commit()
    return dict(zip(("id", "title", "url", "time", "score", "author", "source"), row))


def poll_once(include_rss=True):
    """Poll Algolia Hacker News AND (optionally) publisher RSS feeds."""
    new = []

    # 1. Poll Hacker News front page (every POLL_SECONDS)
    try:
        data = fetch_json(FEED_URL)
        for hit in data.get("hits", []):
            if not hit.get("title") or not hit.get("objectID"):
                continue
            headline = save({
                "id":     int(hit["objectID"]),
                "title":  hit["title"],
                "url":    hit.get("url"),
                "time":   hit.get("created_at_i"),
                "score":  hit.get("points"),
                "by":     hit.get("author"),
                "source": "HN",
            })
            if headline:
                new.append(headline)
    except Exception as e:
        print("[poll] HN error:", e)

    # 2. Poll Publisher RSS Feeds (every RSS_POLL_SECONDS — much less frequent)
    if include_rss:
        for source_name, feed_url in RSS_FEEDS:
            try:
                feed_items = fetch_rss_feed(source_name, feed_url)
                for it in feed_items:
                    headline = save(it)
                    if headline:
                        new.append(headline)
            except Exception as e:
                print(f"[poll] {source_name} error:", e)

    # Broadcast genuinely new stories to SSE subscribers
    if new:
        with sub_lock:
            for h in new:
                for q in subscribers:
                    q.put(h)
    return new


def poll_loop():
    global _last_rss_poll
    first_run = True
    while True:
        now = time.time()
        include_rss = first_run or (now - _last_rss_poll >= RSS_POLL_SECONDS)
        try:
            new = poll_once(include_rss=include_rss)
            src_tag = " (HN+RSS)" if include_rss else " (HN only)"
            print(f"[poll] {len(new)} new headline(s){src_tag}")
        except Exception as e:
            print("[poll] error:", e)
        if include_rss:
            _last_rss_poll = time.time()
        first_run = False
        time.sleep(POLL_SECONDS)


def latest(limit=100):
    with db_lock:
        rows = db.execute(
            "SELECT id,title,url,time,score,author,source FROM headlines "
            "ORDER BY time DESC LIMIT ?", (min(limit, MAX_STORIES),)
        ).fetchall()
    return [dict(zip(("id", "title", "url", "time", "score", "author", "source"), r)) for r in rows]


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/headlines":
            try:
                limit = max(1, min(MAX_STORIES, int(parse_qs(u.query).get("limit", [str(MAX_STORIES)])[0])))
            except ValueError:
                limit = MAX_STORIES
            body = json.dumps(latest(limit)).encode()
            self.send_response(200); self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body))); self.end_headers()
            self.wfile.write(body)
        elif u.path == "/headlines/stream":
            self.send_response(200); self._cors()
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache"); self.end_headers()
            q = queue.Queue()
            with sub_lock:
                subscribers.append(q)
            try:
                while True:
                    try:
                        h = q.get(timeout=15)
                        self.wfile.write(f"data: {json.dumps(h)}\n\n".encode())
                    except queue.Empty:
                        self.wfile.write(b": keepalive\n\n")   # keeps the connection open
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, ConnectionError, OSError):
                pass
            finally:
                with sub_lock:
                    subscribers.remove(q)
        else:
            self.send_response(404); self.end_headers()

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    threading.Thread(target=poll_loop, daemon=True).start()
    print(f"Serving on http://localhost:{PORT}/headlines  (polling every {POLL_SECONDS}s, RSS every {RSS_POLL_SECONDS}s)")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
