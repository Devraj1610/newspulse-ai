# 📰 News Pulse
Real-time Technology News Engine, AI Summarizer, ELI15 Translator, and Cosmitude Bridge Bot Integration

News Pulse automatically polls top technology news, persists articles in a local SQLite database, streams live updates to a Google News-style web dashboard, and integrates with the Cosmitude Bridge platform for automated multi-channel push notifications.

---

## 🎯 The Problem

1. **You have to keep refreshing web pages:** Most news websites make you click refresh over and over again to see if any new story has come out.
2. **Tech articles are full of complicated jargon:** Technology news often uses hard-to-understand words, making it frustrating to read quickly.
3. **News stays stuck on the website:** Websites don't automatically send new breaking headlines to your chat apps or bot devices when they happen.

---

## ⚙️ What News Pulse Does

| Phase | What Happens |
|---|---|
| **1. Poll** | `news_backend.py` periodically fetches top stories from the Hacker News Algolia Search API in a single HTTP request. |
| **2. Persist** | New articles are deduplicated and saved into a thread-safe SQLite database (`news.db`). |
| **3. Stream** | Server-Sent Events (SSE) broadcast new stories live to connected clients via `gateway.py`. |
| **4. AI Intelligence** | Generates 30-second structured summaries, key points, and "Explain Like I'm 15" (ELI15) plain-English translations via `gateway.py` endpoints. |
| **5. Bridge Sync** | `bot.py` authenticates with Cosmitude Bridge (`ca.cosmitude.com`) and listens to incoming form flows and requests. |
| **6. Smart Notify** | `bot.py` evaluates keywords (AI, Cybersecurity, Bitcoin, OpenAI) on incoming stories and auto-pushes targeted alerts to Cosmitude Bridge. |

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start the full stack (Backend + Gateway + Cosmitude Bot + Dashboard)
python run.py

# 3. Open the web dashboard in your browser
http://localhost:8080

# Optional: Run without the Cosmitude Bot
python run.py --no-bot
```

---

## 📂 Project Structure

```
news-system/
├── news_backend.py                       # Hacker News polling loop, SQLite storage, SSE stream provider (Port 8000)
├── gateway.py                            # Middleware HTTP proxy, AI Summarizer/ELI15 API, bot event store (Port 8080)
├── bot.py                                # Cosmitude Bridge device bot, live stream listener, smart keyword alerts
├── run.py                                # Process orchestrator & full-stack runner
├── index.html                            # News Pulse responsive dashboard (Google News layout, dark mode)
├── news.db                               # SQLite database storing headlines & metadata
├── requirements.txt                      # Python dependencies (requests)
├── NewsSystemTemplate_...Admin.txt       # Cosmitude forum template JSON cache
└── README.md                             # Project documentation
```

---

## 🛠️ Configuration

Configure the system using environment variables or set defaults in your environment:

| Variable | Default | Purpose |
|---|---|---|
| `POLL_SECONDS` | `30` | How often `news_backend.py` checks for new stories (seconds) |
| `TOP_N` | `30` | Number of top stories to track from Hacker News |
| `PORT` | `8000` | Port for the core news backend service |
| `GATEWAY_PORT` | `8080` | Port for the gateway server & web dashboard |
| `BACKEND_URL` | `http://127.0.0.1:8000` | Gateway backend endpoint target |
| `GATEWAY_URL` | `http://localhost:8080` | Gateway URL used by `bot.py` for API calls |
| `DB_PATH` | `news.db` | SQLite database file location |

---

## 📊 Dashboard Features

The web dashboard ([`index.html`](file:///c:/Users/devra/Downloads/news-system/news-system/index.html)) provides a Google News-style interface:

* **📰 Formal Google News Aesthetic:** Responsive 3-column layout (Desktop) / 1-column layout (Mobile) with dark mode support.
* **✨ AI News Summarizer:** Generates 30-second overviews, "Why It Matters" context, and key bullet points inline.
* **✨ Auto-Summarize Top 5:** One-click batch summarization of the top 5 trending stories.
* **🧠 Explain Like I'm 15 (ELI15):** Translates complex technical headlines into simple, accessible English.
* **🔔 Smart Notifications Sidebar:** Topic filter preferences for AI, Cybersecurity, Crypto, and OpenAI.
* **📡 Live Activity Stream:** Right sidebar providing real-time logs of Cosmitude bot triggers and automated news pushes.

---

## 🛡️ Architecture & Safety Design

* **Zero-Blocking Concurrency:** Thread-safe SQLite connections protected by `threading.Lock` across worker loops.
* **Isolated Middleware:** The web dashboard and Cosmitude bot communicate strictly through `gateway.py` (Port 8080), keeping `news_backend.py` protected.
* **Resilient Polling:** Exception-swallowing poll loops prevent transient network hiccups from crashing the server.
* **Prefix-Aware Flow Routing:** `bot.py` resolves Cosmitude forum flow names dynamically (`NewsSystemTemplate_RealTimeUpdates` $\rightarrow$ `RealTimeUpdates`).

---

## 📄 License

Open Source / MIT License (Free to use, modify, and distribute).
