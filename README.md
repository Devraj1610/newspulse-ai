# News Pulse

Real-time tech news aggregator with live crypto markets, AI summaries, and a Cosmitude chat bot

News Pulse pulls live headlines from Hacker News, tracks the top 10 cryptocurrencies in real time, and uses Google Gemini AI to summarize stories, answer questions, and push smart notifications — all through a clean, Google News-style interface.

---

## The Problem

Staying up-to-date across tech news and crypto markets means juggling tabs, missing signals, and reading walls of text. News Pulse combines everything in one dashboard:

- Live HN headlines that arrive over SSE without page reloads
- Live crypto prices updated every ~30 seconds with interactive charts
- One-click AI summaries and plain-English explanations for any story
- A chat assistant grounded in today's actual news and live prices
- A Cosmitude bot that pushes smart alerts when key topics or big crypto moves are detected

---

## What News Pulse Does

| Phase | What happens |
|-------|--------------|
| **News** | Polls Algolia HN every 30s — single request, full front-page details stored in SQLite |
| **Crypto** | Polls CoinGecko every ~30s for top 10 coins with 1h/24h/7d change and sparklines |
| **AI** | Calls Gemini REST API server-side to summarize stories, explain in plain English, and answer free-form questions |
| **Bot** | Cosmitude bridge bot evaluates smart keyword filters (AI, Security, Bitcoin, OpenAI) and crypto alerts (>3% move) and pushes notifications to the Cosmitude cloud |
| **SSE** | Both headline and crypto price updates stream live to the browser without polling |

---

## Quick Start

```bash
# Clone and enter the folder
cd news-system

# Install the only dependency
pip install requests

# (Optional) Set your Gemini AI key for richer summaries and Q&A
cp .env.example .env
# Edit .env and add your key: GEMINI_API_KEY=your_key_here

# Start everything
python run.py

# Start without the Cosmitude bot
python run.py --no-bot
```

Dashboard opens at **http://localhost:8080**

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `GEMINI_API_KEY` | *(empty)* | Google Gemini API key — enables AI summaries, ELI15, and Ask AI |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Which Gemini model to use |
| `GATEWAY_PORT` | `8080` | Port for the gateway and dashboard |
| `BACKEND_URL` | `http://127.0.0.1:8000` | Internal URL of the news service |
| `POLL_SECONDS` | `30` | News polling interval |
| `CRYPTO_POLL_SECONDS` | `30` | Crypto polling interval |
| `DB_PATH` | `news.db` | SQLite database file path |

Copy `.env.example` to `.env` and fill in your values. The key is never sent to the browser.

---

## API Endpoints (all served by gateway.py on port 8080)

### News
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/headlines?limit=N` | Latest N headlines (JSON) |
| `GET` | `/api/headlines/stream` | SSE stream of new headlines |
| `GET/POST` | `/api/summarize?title=…&url=…` | 30-second AI summary + key points |
| `GET/POST` | `/api/explain?title=…` | ELI15 plain-English explanation |
| `POST` | `/api/news/summary` | `{headlines:[]}` → cohesive news brief |

### Crypto
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/crypto` | Current top 10 coins |
| `GET` | `/api/crypto/history?coin=bitcoin&minutes=60` | Price series for charting |
| `GET` | `/api/crypto/stream` | SSE stream of price updates |
| `POST` | `/api/crypto/summary` | `{coins:[]}` → AI market summary |
| `POST` | `/api/crypto/eli15` | `{coins:[]}` → plain-English crypto explainer |

### Ask AI
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/ask` | `{question, history?}` → grounded Q&A answer |

### System
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/health` | Gateway + news + AI + crypto status |
| `GET` | `/api/bot-events` | Last 50 bot flow triggers |
| `POST` | `/api/bot-event` | Bot reports a flow trigger |

---

## Project Structure

```
news-system/
├── news_backend.py     # Polls Algolia HN, stores in SQLite, serves /headlines + SSE
├── crypto_backend.py   # Polls CoinGecko, stores crypto_prices table, SSE subscribers
├── gateway.py          # Single entry point (port 8080): proxies, serves UI, all /api/* routes
├── bot.py              # Cosmitude bridge bot: smart notifications, crypto alerts, flow handlers
├── run.py              # Orchestrator: launches all services, --no-bot flag
├── index.html          # Dashboard: Top Stories / Live Crypto / Ask AI tabs
├── news.db             # SQLite: headlines + crypto_prices tables (auto-created)
├── requirements.txt    # Just: requests
├── .env.example        # Template: GEMINI_API_KEY, GEMINI_MODEL
├── .gitignore          # Ignores: .env, *.db, __pycache__, bot admin txt files
└── ReadMe.md           # This file
```

---

## Dashboard Features

- **Top Stories tab** — Google News 3-column layout with search, Smart Notification filters, per-card Summarize + Explain (ELI15) buttons, Auto-Summarize Top 5, Live Updates sidebar
- **Live Crypto tab** — Coin cards (symbol, price, 24h % in green/red), interactive Chart.js line chart with 1h/6h/24h range selector, live SSE updates, Summarize Market + ELI15 buttons
- **Ask AI tab** — Chat-style Q&A grounded in today's headlines and live crypto prices, suggestion chips, conversation history, thinking indicator, financial advice disclaimer
- **AI offline mode** — Falls back to rule-based summaries silently; shows "AI offline — basic mode" badge when key is missing

---

## Architecture

```
Browser (port 8080)
    └── gateway.py ──┬── /api/headlines  ──► news_backend.py (port 8000)
                     ├── /api/crypto      ──► crypto_backend.py (in-process)
                     ├── /api/ask         ──► Gemini REST API
                     └── /api/bot-event   ◄── bot.py

bot.py ──► ca.cosmitude.com (Cosmitude cloud)
       ◄── gateway.py /api/headlines/stream (SSE listener)
       ──► gateway.py /api/bot-event (reports alerts)
       ──► gateway.py /api/crypto    (monitors for >3% moves)
```

The browser and the bot **never** talk directly to port 8000 — only through the gateway.
