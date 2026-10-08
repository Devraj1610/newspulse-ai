"""Live news headlines backend: polls Hacker News, stores in SQLite, serves JSON + live stream.

Run:  python news_backend.py        (needs: pip install requests)
API:  GET /headlines?limit=20       newest first, JSON
      GET /headlines/stream         Server-Sent Events, one event per new headline
"""
import json, os, queue, sqlite3, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import requests

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", 30))   # how often to check for new stories
TOP_N = int(os.environ.get("TOP_N", 30))                  # how many top stories to track
FEED_URL = f"https://hn.algolia.com/api/v1/search?tags=front_page&hitsPerPage={TOP_N}"
PORT = int(os.environ.get("PORT", 8000))
DB_PATH = os.environ.get("DB_PATH", "news.db")

db = sqlite3.connect(DB_PATH, check_same_thread=False)
db_lock = threading.Lock()
db.execute("""CREATE TABLE IF NOT EXISTS headlines (
    id INTEGER PRIMARY KEY, title TEXT, url TEXT, time INTEGER, score INTEGER, author TEXT)""")
db.commit()

subscribers = []                # one queue per connected live client
sub_lock = threading.Lock()


def fetch_json(url):
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()


def save(item):
    """Insert one story; returns the headline dict if it was new, else None."""
    row = (item["id"], item.get("title"), item.get("url"), item.get("time"),
           item.get("score"), item.get("by"))
    with db_lock:
        cur = db.execute("INSERT OR IGNORE INTO headlines VALUES (?,?,?,?,?,?)", row)
        db.commit()
        if cur.rowcount == 0:
            return None
    return dict(zip(("id", "title", "url", "time", "score", "author"), row))


def poll_once():
    """One request returns the whole current front page with full details."""
    data = fetch_json(FEED_URL)
    new = []
    for hit in data.get("hits", []):
        if not hit.get("title") or not hit.get("objectID"):
            continue
        headline = save({"id": int(hit["objectID"]), "title": hit["title"], "url": hit.get("url"),
                         "time": hit.get("created_at_i"), "score": hit.get("points"),
                         "by": hit.get("author")})      # save() ignores IDs already stored
        if headline:
            new.append(headline)
    with sub_lock:
        for h in new:
            for q in subscribers:
                q.put(h)
    return new


def poll_loop():
    while True:
        try:
            new = poll_once()
            print(f"[poll] {len(new)} new headline(s)")
        except Exception as e:                      # keep running if the source hiccups
            print("[poll] error:", e)
        time.sleep(POLL_SECONDS)


def latest(limit):
    with db_lock:
        rows = db.execute("SELECT id,title,url,time,score,author FROM headlines "
                          "ORDER BY time DESC LIMIT ?", (limit,)).fetchall()
    return [dict(zip(("id", "title", "url", "time", "score", "author"), r)) for r in rows]


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/headlines":
            try:
                limit = max(1, min(200, int(parse_qs(u.query).get("limit", ["20"])[0])))
            except ValueError:
                limit = 20
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
            except (BrokenPipeError, ConnectionResetError):
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
    print(f"Serving on http://localhost:{PORT}/headlines  (polling every {POLL_SECONDS}s)")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
