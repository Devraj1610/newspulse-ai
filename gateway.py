"""Middleware: one address for everything.
  /                           -> the dashboard page (index.html)
  /api/headlines?limit=N      -> proxied to the news service
  /api/headlines/stream       -> live Server-Sent Events, proxied
  /api/health                 -> is the gateway / news service up?
  /api/bot-events             -> last 50 Cosmitude bot-flow triggers (GET)
  /api/bot-event              -> bot.py reports a flow trigger here (POST)
  /api/summarize              -> AI summary for a story (GET/POST)
  /api/explain                -> AI ELI15 explanation (GET/POST)
  /api/news/summary           -> AI summary of top 5-10 headlines (POST)
  /api/crypto                 -> current top 10 coins (GET)
  /api/crypto/history         -> price series for a coin (GET)
  /api/crypto/stream          -> live SSE of price updates (GET)
  /api/crypto/summary         -> AI market summary (POST)
  /api/crypto/eli15           -> AI crypto ELI15 (POST)
  /api/ask                    -> general Q&A with news+crypto context (POST)
The dashboard and the Cosmitude bot both talk to this, never to the news service directly.
"""
import hashlib, json, os, queue, time, threading
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import requests

# ── Tiny .env loader (stdlib only, no python-dotenv) ─────────────────────────
def _load_dotenv(path=".env"):
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip(); v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass

_load_dotenv()

BACKEND       = os.environ.get("BACKEND_URL", "http://127.0.0.1:8000")
PORT          = int(os.environ.get("GATEWAY_PORT", 8080))
INDEX         = Path(__file__).parent / "index.html"
GEMINI_KEY    = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL  = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_URL    = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
GEMINI_CACHE_TTL = 300   # seconds

# ── Try to import crypto_backend (starts when available) ─────────────────────
try:
    import crypto_backend as _crypto
    _crypto.start_crypto_poller()
    _HAS_CRYPTO = True
except ImportError:
    _HAS_CRYPTO = False
    print("[gateway] crypto_backend not found — crypto endpoints disabled.")

# ── In-memory stores ─────────────────────────────────────────────────────────
_bot_events  = deque(maxlen=50)
_events_lock = threading.Lock()

_ai_cache: dict[str, tuple[float, object]] = {}  # hash -> (expires_at, result)
_cache_lock = threading.Lock()

# ── GitHub Trending Pool & Cache (Emerging Tech & Breakthrough Projects) ─────
_github_pages = {}  # page -> (expires, repos)
_github_current_page = 1
_github_lock = threading.Lock()

def get_github_trending(force_refresh=False, page=1):
    """Fetch newly created & rapidly growing repositories in emerging technologies.
    Cycles through pages and diverse technology queries so every refresh shows brand new repos.
    Cached per page for 90 seconds to stay safely under GitHub API limits."""
    global _github_current_page
    with _github_lock:
        if force_refresh:
            _github_current_page = (_github_current_page % 5) + 1
            target_page = _github_current_page
        else:
            target_page = page

        cached = _github_pages.get(target_page)
        if cached and time.time() < cached[0] and cached[1] and not force_refresh:
            return cached[1]

    import datetime, urllib.parse
    cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=45)).strftime('%Y-%m-%d')

    # Query variations rotated by target page to ensure completely fresh domains
    query_variants = [
        f'created:>{cutoff} ai OR llm OR agents OR neural stars:>20',
        f'created:>{cutoff} "deep learning" OR diffusion OR vision stars:>15',
        f'created:>{cutoff} robotics OR autonomous OR agentic stars:>15',
        f'created:>{cutoff} quantum OR webgpu OR compiler stars:>15',
        f'created:>{cutoff} machine-learning OR gpt OR generative stars:>20',
    ]
    chosen_query = query_variants[(target_page - 1) % len(query_variants)]
    api_page = ((target_page - 1) // len(query_variants)) + 1

    url = (
        'https://api.github.com/search/repositories?'
        f'q={urllib.parse.quote(chosen_query)}&sort=stars&order=desc&per_page=6&page={api_page}'
    )

    try:
        r = requests.get(
            url,
            timeout=10,
            headers={
                'Accept': 'application/vnd.github.v3+json',
                'User-Agent': 'NewsPulse-EmergingTech/2.0'
            }
        )
        if r.status_code == 200:
            items = r.json().get('items', [])
            if items:
                repos = [{
                    'name': item.get('full_name', ''),
                    'description': item.get('description', '') or 'Cutting-edge technology repository.',
                    'stars': item.get('stargazers_count', 0),
                    'language': item.get('language', '') or 'Tech',
                    'url': item.get('html_url', ''),
                    'created_at': (item.get('created_at', '') or '')[:10],
                } for item in items]
                with _github_lock:
                    _github_pages[target_page] = (time.time() + 90, repos)
                return repos
        elif r.status_code in (403, 429):
            print("[github] API rate-limited; serving cached page if available.")
    except Exception as e:
        print("[github] fetch error:", e)

    # Return any previously cached page or fallback
    with _github_lock:
        if target_page in _github_pages and _github_pages[target_page][1]:
            return _github_pages[target_page][1]
        for p in _github_pages:
            if _github_pages[p][1]:
                return _github_pages[p][1]
        return []


# ── Gemini AI helper ─────────────────────────────────────────────────────────
_AI_OFFLINE = not bool(GEMINI_KEY)

def _gemini_call(prompt: str) -> str | None:
    """Call Gemini REST API. Returns text or None on failure."""
    if not GEMINI_KEY:
        return None
    h = hashlib.sha256(prompt.encode()).hexdigest()
    with _cache_lock:
        if h in _ai_cache:
            exp, val = _ai_cache[h]
            if time.time() < exp:
                return val
    try:
        r = requests.post(
            GEMINI_URL,
            headers={"x-goog-api-key": GEMINI_KEY, "Content-Type": "application/json"},
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=30,
        )
        r.raise_for_status()
        cand = r.json().get("candidates", [{}])[0]
        parts = cand.get("content", {}).get("parts", [])
        text = "".join(p.get("text", "") for p in parts if "text" in p)
        if not text:
            return None
        with _cache_lock:
            _ai_cache[h] = (time.time() + GEMINI_CACHE_TTL, text)
        return text
    except Exception as e:
        print("[gemini] error:", e)
        return None

# ── Rule-based fallbacks ──────────────────────────────────────────────────────
def generate_summary(title, url=""):
    clean_title = title.strip()
    t_lower = clean_title.lower()
    if any(k in t_lower for k in ['ai', 'llm', 'gpt', 'openai', 'model', 'claude', 'gemini', 'deepmind', 'machine learning']):
        topic  = "Artificial Intelligence"
        impact = "Affects developers, tech companies, and automation workflows by advancing AI capabilities."
        points = [
            "Introduces state-of-the-art model optimizations or benchmark results.",
            "Changes how developers build intelligent applications.",
            "Accelerates industry competition across foundation model providers."
        ]
    elif any(k in t_lower for k in ['security', 'vulnerability', 'cve', 'hack', 'breach', 'exploit', 'quantum']):
        topic  = "Cybersecurity & Infrastructure"
        impact = "Critical for systems engineers and security analysts."
        points = [
            "Highlights new security vector or architectural vulnerability.",
            "Requires system admins to review dependency versions.",
            "Emphasizes the necessity of proactive defense."
        ]
    elif any(k in t_lower for k in ['linux', 'kernel', 'rust', 'python', 'git', 'sql', 'compiler', 'code']):
        topic  = "Software Engineering & Developer Tooling"
        impact = "Directly improves developer productivity, performance, or system stability."
        points = [
            "Refines core developer APIs or system performance primitives.",
            "Reduces overhead and memory footprints for production deployments.",
            "Modernizes codebases with safety and speed improvements."
        ]
    else:
        topic  = "Technology & Tech Industry"
        impact = "Provides key insights into tech trends, open-source innovations, and digital infrastructure."
        points = [
            f"Discusses key developments regarding '{clean_title}'.",
            "Reflects ongoing architectural shifts in modern technology ecosystems.",
            "Warrants observation for software builders and tech practitioners."
        ]
    return {
        "title": clean_title,
        "summary": f"Recent update in {topic} regarding: '{clean_title}'.",
        "why_it_matters": impact,
        "key_points": points,
        "source": url or "Hacker News",
        "ai_mode": False,
    }

def generate_eli15(title):
    clean_title = title.strip()
    t_lower = clean_title.lower()
    if "quantum" in t_lower:
        simple = "Scientists are building super-fast new computers, so we need stronger digital locks that those computers can't break."
    elif any(k in t_lower for k in ["llm", "gpt", "ai"]):
        simple = "Imagine an ultra-smart reading assistant that read the entire internet and can now answer complex questions, write code, or solve problems for you."
    elif any(k in t_lower for k in ["kernel", "linux"]):
        simple = "The kernel is like the engine of an operating system. This news is about giving the computer engine a tune-up so it runs faster and safer."
    elif any(k in t_lower for k in ["security", "cve", "vulnerability"]):
        simple = "A secret backdoor or hole was found in a computer program, and engineers are patching it so hackers can't get in."
    elif any(k in t_lower for k in ["rust", "compiler"]):
        simple = "Programmers are using newer programming languages that catch human mistakes automatically before the software ever runs."
    else:
        simple = f"In simple terms: Engineers are improving how '{clean_title}' works to make tech faster, safer, or easier to use."
    return {"title": clean_title, "simple_explanation": simple, "ai_mode": False}


# ── Gateway HTTP handler ──────────────────────────────────────────────────────
class Gateway(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length)) if length else {}

    # ── GET ───────────────────────────────────────────────────────────────────
    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)

        if u.path in ("/", "/index.html"):
            self._send(200, INDEX.read_bytes(), "text/html; charset=utf-8")

        elif u.path == "/api/health":
            try:
                requests.get(f"{BACKEND}/headlines?limit=1", timeout=3).raise_for_status()
                self._send(200, {"gateway": "ok", "news_service": "ok", "ai": not _AI_OFFLINE, "crypto": _HAS_CRYPTO})
            except Exception:
                self._send(200, {"gateway": "ok", "news_service": "down", "ai": not _AI_OFFLINE, "crypto": _HAS_CRYPTO})

        elif u.path == "/api/headlines/stream":
            self._stream_news()

        elif u.path == "/api/headlines":
            try:
                r = requests.get(f"{BACKEND}/headlines", params=u.query and dict(
                    p.split("=", 1) for p in u.query.split("&") if "=" in p), timeout=10)
                self._send(r.status_code, r.content)
            except Exception:
                self._send(502, {"error": "news service unreachable"})

        elif u.path == "/api/bot-events":
            with _events_lock:
                self._send(200, list(_bot_events))

        elif u.path == "/api/summarize":
            title = q.get("title", [""])[0]
            url   = q.get("url",   [""])[0]
            self._send(200, self._do_summarize(title, url))

        elif u.path == "/api/explain":
            title = q.get("title", [""])[0]
            self._send(200, self._do_explain(title))

        elif u.path == "/api/crypto":
            if not _HAS_CRYPTO:
                self._send(503, {"error": "crypto service not running"})
            else:
                self._send(200, _crypto.get_current_crypto())

        elif u.path == "/api/crypto/history":
            if not _HAS_CRYPTO:
                self._send(503, {"error": "crypto service not running"})
            else:
                coin    = q.get("coin", ["bitcoin"])[0]
                minutes = int(q.get("minutes", ["60"])[0])
                self._send(200, _crypto.get_crypto_history(coin, minutes))

        elif u.path == "/api/crypto/stream":
            if not _HAS_CRYPTO:
                self._send(503, {"error": "crypto service not running"})
            else:
                self._stream_crypto()

        elif u.path == "/api/github":
            force = q.get("refresh", ["0"])[0] in ("1", "true")
            page = int(q.get("page", ["1"])[0]) if q.get("page", ["1"])[0].isdigit() else 1
            self._send(200, get_github_trending(force_refresh=force, page=page))

        else:
            self._send(404, {"error": "not found"})

    # ── POST ──────────────────────────────────────────────────────────────────
    def do_POST(self):
        u = urlparse(self.path)

        if u.path == "/api/bot-event":
            try:
                event = self._read_json()
                event.setdefault("ts", time.time())
                with _events_lock:
                    _bot_events.appendleft(event)
                print(f"[bot-event] flow={event.get('flow')!r} mobile={event.get('mobile')!r}")
                self._send(200, {"ok": True})
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        elif u.path == "/api/summarize":
            try:
                body = self._read_json()
                self._send(200, self._do_summarize(body.get("title", ""), body.get("url", "")))
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        elif u.path == "/api/explain":
            try:
                body = self._read_json()
                self._send(200, self._do_explain(body.get("title", "")))
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        elif u.path == "/api/news/summary":
            try:
                body = self._read_json()
                headlines = body.get("headlines", [])
                self._send(200, self._do_news_summary(headlines))
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        elif u.path == "/api/crypto/summary":
            try:
                body = self._read_json()
                self._send(200, self._do_crypto_summary(body.get("coins", [])))
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        elif u.path == "/api/crypto/eli15":
            try:
                body = self._read_json()
                self._send(200, self._do_crypto_eli15(body.get("coins", [])))
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        elif u.path == "/api/ask":
            try:
                body = self._read_json()
                self._send(200, self._do_ask(body.get("question", ""), body.get("history", [])))
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        else:
            self._send(404, {"error": "not found"})

    # ── AI action helpers ─────────────────────────────────────────────────────
    def _do_summarize(self, title, url=""):
        if not title:
            return {"error": "title required"}
        if GEMINI_KEY:
            prompt = (
                f"Summarize this tech news headline in a structured way:\n"
                f"Headline: {title}\nURL: {url or 'N/A'}\n\n"
                "Return JSON with keys: title, summary (2-3 sentences), why_it_matters (1 sentence), "
                "key_points (list of 3 strings). No markdown fences, just raw JSON."
            )
            text = _gemini_call(prompt)
            if text:
                try:
                    clean = text.strip().lstrip('```json').lstrip('```').rstrip('```').strip()
                    result = json.loads(clean)
                    result["ai_mode"] = True
                    result["source"] = url or "Hacker News"
                    return result
                except Exception:
                    pass  # fall through to rule-based
        result = generate_summary(title, url)
        result["ai_offline"] = not bool(GEMINI_KEY)
        return result

    def _do_explain(self, title):
        if not title:
            return {"error": "title required"}
        if GEMINI_KEY:
            prompt = (
                f"Explain this tech news headline to a curious 15-year-old in 2-3 sentences:\n"
                f"Headline: {title}\n"
                "Be friendly, use simple analogies, avoid jargon. Return plain text only."
            )
            text = _gemini_call(prompt)
            if text:
                return {"title": title, "simple_explanation": text.strip(), "ai_mode": True}
        result = generate_eli15(title)
        result["ai_offline"] = not bool(GEMINI_KEY)
        return result

    def _do_news_summary(self, headlines: list):
        if not headlines:
            return {"error": "no headlines provided"}
        items_text = "\n".join(f"- {h}" for h in headlines[:10])
        if GEMINI_KEY:
            prompt = (
                "You are a tech news editor. Summarize the following top news headlines into a "
                "cohesive 3-4 sentence news brief for a busy reader. Mention key themes, companies, "
                "and trends. Conclude with 1 sentence about what to watch next.\n\n"
                f"Headlines:\n{items_text}\n\nReturn plain text only."
            )
            text = _gemini_call(prompt)
            if text:
                return {"summary": text.strip(), "count": len(headlines), "ai_mode": True}
        # fallback
        summary = f"Today's top {len(headlines)} stories cover: " + "; ".join(str(h)[:60] for h in headlines[:5]) + "."
        return {"summary": summary, "count": len(headlines), "ai_mode": False, "ai_offline": not bool(GEMINI_KEY)}

    def _do_crypto_summary(self, coins: list):
        if not coins:
            coins = _crypto.get_current_crypto() if _HAS_CRYPTO else []
        if not coins:
            return {"error": "no crypto data available"}
        coin_lines = "\n".join(
            f"- {c['name']} ({c['symbol']}): ${c['price']:,.2f}, 24h {c['change_24h']:+.2f}%"
            for c in coins[:10]
        )
        if GEMINI_KEY:
            prompt = (
                "You are a neutral crypto market analyst. Summarize the following live price data "
                "in 3-4 sentences. Note which coins are gaining or losing, overall market sentiment, "
                "and any notable moves. Add a disclaimer that this is not financial advice.\n\n"
                f"Data:\n{coin_lines}\n\nReturn plain text only."
            )
            text = _gemini_call(prompt)
            if text:
                return {"summary": text.strip(), "ai_mode": True}
        # fallback
        gainers = [c for c in coins if c.get("change_24h", 0) > 0]
        losers  = [c for c in coins if c.get("change_24h", 0) < 0]
        summary = (
            f"Market snapshot: {len(gainers)} coins up, {len(losers)} coins down in the last 24h. "
            f"Not financial advice."
        )
        return {"summary": summary, "ai_mode": False, "ai_offline": not bool(GEMINI_KEY)}

    def _do_crypto_eli15(self, coins: list):
        if not coins:
            coins = _crypto.get_current_crypto() if _HAS_CRYPTO else []
        if not coins:
            return {"error": "no crypto data available"}
        top = coins[0] if coins else {}
        if GEMINI_KEY:
            prompt = (
                f"Explain cryptocurrency and today's market to a curious 15-year-old. "
                f"The top coin right now is {top.get('name','Bitcoin')} at ${top.get('price',0):,.0f} "
                f"(24h: {top.get('change_24h',0):+.2f}%). "
                "Use a simple analogy, 2-3 sentences. This is not financial advice."
            )
            text = _gemini_call(prompt)
            if text:
                return {"explanation": text.strip(), "ai_mode": True}
        return {
            "explanation": (
                f"Cryptocurrency is like digital money that lives on the internet. "
                f"Right now, {top.get('name','Bitcoin')} is worth ${top.get('price',0):,.0f}. "
                "Prices go up and down a lot — this is not financial advice."
            ),
            "ai_mode": False,
            "ai_offline": not bool(GEMINI_KEY),
        }

    def _do_ask(self, question: str, history: list):
        if not question:
            return {"error": "question required"}
        if not GEMINI_KEY:
            return {
                "answer": "AI is offline. Please set GEMINI_API_KEY in your .env file.",
                "ai_mode": False,
                "ai_offline": True,
            }
        # Build context from latest news + crypto
        news_ctx = ""
        try:
            r = requests.get(f"{BACKEND}/headlines?limit=10", timeout=5)
            if r.ok:
                headlines = [h.get("title", "") for h in r.json()[:10]]
                news_ctx = "Latest news headlines:\n" + "\n".join(f"- {h}" for h in headlines)
        except Exception:
            pass

        crypto_ctx = ""
        if _HAS_CRYPTO:
            try:
                coins = _crypto.get_current_crypto()
                crypto_ctx = "\nLive crypto prices:\n" + "\n".join(
                    f"- {c['name']}: ${c['price']:,.2f} (24h {c['change_24h']:+.2f}%)"
                    for c in coins[:5]
                )
            except Exception:
                pass

        history_ctx = ""
        if history:
            history_ctx = "\nPrevious conversation:\n" + "\n".join(
                f"{'User' if m.get('role')=='user' else 'Assistant'}: {m.get('content','')}"
                for m in history[-6:]
            )

        prompt = (
            "You are News Pulse AI — a helpful assistant for tech news and crypto. "
            "Answer the user's question using the provided context. "
            "If you don't know something, say so. Never give financial advice. "
            "Always add a brief disclaimer when discussing crypto prices.\n\n"
            f"{news_ctx}\n{crypto_ctx}\n{history_ctx}\n\nUser question: {question}\n"
            "Answer concisely in 2-4 sentences."
        )
        text = _gemini_call(prompt)
        if text:
            return {"answer": text.strip(), "ai_mode": True}
        return {"answer": "Sorry, I couldn't get a response right now. Please try again.", "ai_mode": False}

    # ── SSE streams ───────────────────────────────────────────────────────────
    def _stream_news(self):
        try:
            r = requests.get(f"{BACKEND}/headlines/stream", stream=True, timeout=(5, 60))
        except Exception:
            return self._send(502, {"error": "news service unreachable"})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        try:
            for line in r.iter_lines(chunk_size=1):
                self.wfile.write(line + b"\n")
                self.wfile.flush()
        except Exception:
            pass
        finally:
            r.close()

    def _stream_crypto(self):
        q = queue.Queue(maxsize=20)
        _crypto.subscribe_crypto(q)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        # push current state immediately
        try:
            current = _crypto.get_current_crypto()
            if current:
                data = json.dumps(current)
                self.wfile.write(f"data: {data}\n\n".encode())
                self.wfile.flush()
        except Exception:
            pass
        # then wait for updates
        try:
            while True:
                try:
                    coins = q.get(timeout=15)
                    data = json.dumps(coins)
                    self.wfile.write(f"data: {data}\n\n".encode())
                    self.wfile.flush()
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            _crypto.unsubscribe_crypto(q)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    ai_status = "Gemini AI active" if GEMINI_KEY else "AI offline (set GEMINI_API_KEY)"
    print(f"Gateway on http://localhost:{PORT}  ->  news at {BACKEND}  |  {ai_status}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Gateway).serve_forever()
