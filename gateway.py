"""Middleware: one address for everything.
  /                       -> the dashboard page (index.html)
  /api/headlines?limit=N  -> proxied to the news service
  /api/headlines/stream   -> live Server-Sent Events, proxied
  /api/health             -> is the gateway / news service up?
  /api/bot-events         -> last 50 Cosmitude bot-flow triggers (GET)
  /api/bot-event          -> bot.py reports a flow trigger here (POST)
  /api/summarize          -> generates 30s summary + key points for a story (POST/GET)
  /api/explain            -> generates ELI15 simple explanation for a story (POST/GET)
The dashboard and the Cosmitude bot both talk to this, never to the news service directly.
"""
import json, os, time, re
from collections import deque
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import requests

BACKEND = os.environ.get("BACKEND_URL", "http://127.0.0.1:8000")
PORT    = int(os.environ.get("GATEWAY_PORT", 8080))
INDEX   = Path(__file__).parent / "index.html"

# ── In-memory Cosmitude bot-event store (newest first, max 50) ────
_bot_events  = deque(maxlen=50)
_events_lock = threading.Lock()
# ──────────────────────────────────────────────────────────────────

def generate_summary(title, url=""):
    """Generate structured 30-second summary, why it matters, and key points."""
    clean_title = title.strip()
    
    # Topic detection for contextual summaries
    t_lower = clean_title.lower()
    
    if any(k in t_lower for k in ['ai', 'llm', 'gpt', 'openai', 'model', 'claude', 'gemini', 'deepmind', 'machine learning']):
        topic = "Artificial Intelligence"
        impact = "Affects developers, tech companies, and automation workflows by advancing AI capabilities."
        points = [
            "Introduces state-of-the-art model optimizations or benchmark results.",
            "Changes how developers build intelligent applications and automation scripts.",
            "Accelerates industry competition across foundation model providers."
        ]
    elif any(k in t_lower for k in ['security', 'vulnerability', 'cve', 'hack', 'breach', 'exploit', 'auth', 'cipher', 'quantum']):
        topic = "Cybersecurity & Infrastructure"
        impact = "Critical for systems engineers and security analysts to audit access control and patch flaws."
        points = [
            "Highlights new security vector or architectural vulnerability.",
            "Requires system admins to review dependency versions and security rules.",
            "Emphasizes the necessity of proactive defense and defense-in-depth."
        ]
    elif any(k in t_lower for k in ['linux', 'kernel', 'rust', 'python', 'git', 'db', 'sql', 'compiler', 'code']):
        topic = "Software Engineering & Developer Tooling"
        impact = "Directly improves developer productivity, performance, or system stability."
        points = [
            "Refines core developer APIs or system performance primitives.",
            "Reduces overhead and memory footprints for production deployments.",
            "Modernizes codebases with safety and speed improvements."
        ]
    else:
        topic = "Technology & Tech Industry"
        impact = "Provides key insights into tech trends, open-source innovations, and digital infrastructure."
        points = [
            f"Discusses key developments regarding '{clean_title}'.",
            "Reflects ongoing architectural shifts in modern technology ecosystems.",
            "Warrants observation for software builders and tech practitioners."
        ]

    summary_text = f"Recent update in {topic} regarding: '{clean_title}'."
    
    return {
        "title": clean_title,
        "summary": f"📝 30-Second Summary:\n{summary_text}\n\n💡 Why it matters:\n{impact}",
        "why_it_matters": impact,
        "key_points": points,
        "source": url or "Hacker News / Web"
    }

def generate_eli15(title):
    """Generate 'Explain Like I'm 15' simple explanation."""
    clean_title = title.strip()
    t_lower = clean_title.lower()

    if "quantum" in t_lower:
        simple = "Scientists are building super-fast new computers, so we need stronger digital locks that those new super-computers can't break."
    elif "llm" in t_lower or "gpt" in t_lower or "ai" in t_lower:
        simple = "Imagine an ultra-smart reading assistant that read the entire internet and can now answer complex questions, write code, or solve problems for you."
    elif "kernel" in t_lower or "linux" in t_lower:
        simple = "The kernel is like the engine of an operating system. This news is about giving the computer engine a tune-up so it runs faster and safer."
    elif "security" in t_lower or "cve" in t_lower or "vulnerability" in t_lower:
        simple = "A secret backdoor or hole was found in a computer program, and engineers are patching it so sneaky hackers can't get in."
    elif "rust" in t_lower or "compiler" in t_lower:
        simple = "Programmers are using newer programming languages that catch human mistakes automatically before the software ever runs."
    else:
        simple = f"In simple terms: Engineers are improving how '{clean_title}' works to make tech faster, safer, or easier to use."

    return {
        "title": clean_title,
        "simple_explanation": simple
    }


class Gateway(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ── GET ────────────────────────────────────────────────────────
    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)

        if u.path in ("/", "/index.html"):
            self._send(200, INDEX.read_bytes(), "text/html; charset=utf-8")

        elif u.path == "/api/health":
            try:
                requests.get(f"{BACKEND}/headlines?limit=1", timeout=3).raise_for_status()
                self._send(200, {"gateway": "ok", "news_service": "ok"})
            except Exception:
                self._send(200, {"gateway": "ok", "news_service": "down"})

        elif u.path == "/api/headlines/stream":
            self._stream()

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
            url = q.get("url", [""])[0]
            self._send(200, generate_summary(title, url))

        elif u.path == "/api/explain":
            title = q.get("title", [""])[0]
            self._send(200, generate_eli15(title))

        else:
            self._send(404, {"error": "not found"})

    # ── POST ───────────────────────────────────────────────────────
    def do_POST(self):
        u = urlparse(self.path)
        if u.path == "/api/bot-event":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body   = self.rfile.read(length)
                event  = json.loads(body)
                event.setdefault("ts", time.time())
                with _events_lock:
                    _bot_events.appendleft(event)
                print(f"[bot-event] flow={event.get('flow')!r} mobile={event.get('mobile')!r}")
                self._send(200, {"ok": True})
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        elif u.path == "/api/summarize":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                self._send(200, generate_summary(body.get("title", ""), body.get("url", "")))
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        elif u.path == "/api/explain":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                self._send(200, generate_eli15(body.get("title", "")))
            except Exception as exc:
                self._send(400, {"error": str(exc)})

        else:
            self._send(404, {"error": "not found"})

    # ── SSE proxy ──────────────────────────────────────────────────
    def _stream(self):
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

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    print(f"Gateway on http://localhost:{PORT}  ->  news service at {BACKEND}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Gateway).serve_forever()
