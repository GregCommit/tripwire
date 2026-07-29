# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## ⚠️ This repo contains TWO separate apps

Despite living in one folder and sharing the "Tripwire" name and Yahoo Finance
data, these are **two independent applications** with different codebases,
databases, ports, and purposes. Don't confuse them.

| | **App A — Tripwire Watcher** | **App B — Tripwire Portfolio** |
|---|---|---|
| Purpose | Rule-based **signal/alert engine** — watches a list and flags unusual behavior | **Portfolio tracker** — holdings, transactions, P/L |
| Files | `app.py` + `backtest.py` | `portfolio.py` (server) + `docs/index.html` (phone PWA) |
| Port | **5000** | **5010** (server) / GitHub Pages (phone) |
| DB | `~/.tripwire_v3/state.db` | `~/.tripwire_portfolio/portfolio.db` / browser `localStorage` |
| Password env | `TRIPWIRE_PASSWORD` | `PORTFOLIO_PASSWORD` |
| Docs | `BACKTESTING.md` | `PORTFOLIO.md` |
| User calls it | "the stock watcher" | "the portfolio / transaction tracker" |

The card UI with **"HIGH VOL"** / **"Earnings in 7d"** badges and per-stock
signal triggers is **App A** (`app.py`). The card UI with holdings, sparklines,
and buy/sell markers is **App B** (`docs/index.html`).

---

## App A — Tripwire Watcher (`app.py`, `backtest.py`)

A Flask app (single file, ~3900 lines, port 5000) that watches a user-defined
list of tickers and fires **signals** when a stock does something statistically
unusual *by its own historical standards*. Backtest-calibrated thresholds.

### Core concept: rules, triggers, categories

- Every watched symbol is evaluated against the **same 7 rules** on each scan
  (`evaluate_rules()`, ~line 955):
  1. **Volatility** — today's move vs its own N-day average move
  2. **Support / Resistance** — price breaks beyond the prior N-day high/low band
  3. **Consecutive down days** — *context-only* (see `CONTEXT_ONLY_RULES`); alerts
     only when a losing streak *resolves* upward
  4. **Volume spike** — today's volume vs N-day average
  5. **Gap** — open vs prior close
  6. **RSI** — overbought/oversold
  7. **MA crossover** — golden/death cross (disabled by default)

- A **"trigger"** is not a property a ticker *has* — it's a **live event**: a rule
  is only "triggered" when its condition is met *right now*. A quiet stock shows no
  trigger because nothing is firing, not because it lacks rules. This is the single
  most common point of user confusion.

- Each rule needs a minimum amount of price history; when a ticker is too new the
  rule reports **"Insufficient history"** and sits out (watch for this on
  recently-listed tickers — e.g. the HIGH-VOL support/resistance rule looks back
  120 days).

- **Volatility categories** (`VALID_CATEGORIES = high_vol | mod_vol | low_vol`)
  select *which threshold set* applies to a symbol. Stored per-symbol in the
  `stocks` table; the "HIGH VOL" badge on the card is this category, **not** a
  fired signal. `DEFAULT_RULES` holds the backtest-tuned thresholds per category;
  `INFO_RULES` holds looser thresholds used only for the informational "activity"
  tier. Per-symbol overrides live in `rule_params`; a `sensitivity` preset shifts a
  category's thresholds conservative↔sensitive (`_apply_sensitivity`).

### Backtest calibration (`backtest.py`)

Standalone event-study backtester over ~5 years of daily data. It answers *which
rules predict 1–5 day moves and at what thresholds*, then writes:
- `backtest_results/recommended_params.json` — per-ticker recommended thresholds
- `backtest_results/rule_stats.json` — per-symbol/per-rule evidence, surfaced in the
  UI as confidence hints (`RULE_STATS`, `_rule_stats_for`)

It **re-implements the rule math vectorized** (does NOT import `app.py`, which would
boot Flask + monitor threads) and has a `parity` command to verify its math matches
`evaluate_rules()`. See `BACKTESTING.md`. Key commands:
`python backtest.py fetch|parity|grid|combo|select|report|apply|undo`.

### Scan loop, signals, notifications

- `monitor_loop()` (background daemon thread, started at import) calls `run_check()`
  on an interval that depends on `market_phase()` (open/closed).
- `run_check()` → for each stock: `fetch_quote` → `get_history(days=400)` →
  `get_params` → `evaluate_rules` → log triggered rules as alerts.
- `compute_signal()` combines rules into an ensemble; `STRONG_SIGNALS`
  (`STRONG BUY`, `STRONG BOUNCE WATCH`) gate push notifications when
  `notify_strong_only` is set.
- **Notification channels**: email (SMTP, `_send_email`), WhatsApp via CallMeBot
  (`_send_whatsapp`), and a once-a-day **digest** (`build_daily_digest`) that
  replaces instant pushes when enabled.
- **Claude integration** (`ANTHROPIC_API_KEY`): `synthesize_news()` writes a short
  plain-English "why did it move" note for triggered alerts by summarizing fetched
  news; there's also a chat feature (`chat_messages` table) with tool-calling over
  the watchlist.

### Key API (`@login_required`)

`/api/stocks` is the main read — returns every symbol with its full `rules` array
(each rule's `triggered`, `message`, `actual_value`, `threshold`, `signal`), plus
`alert` (any rule currently firing = the card's trigger), category, price,
`earnings_in_days`, and `info_events`. **To inspect why a symbol is quiet, read its
`rules[]` here** — each `message` says `OK`, `ALERT`, `Insufficient history`, or
`Rule disabled`. Other routes: `/api/check` (force scan), `/api/stocks/add|remove`,
`/api/stock/<sym>/category|params|sensitivity`, `/api/recalibrate/*`, `/api/alerts*`.

### Data model (`~/.tripwire_v3/state.db`)

Tables: `stocks` (symbol, category, active), `prices`, `alerts` (symbol, rule_type,
message, price, timestamp), `rule_params` (per-symbol overrides), `settings`,
`chat_messages`.

### Run it

```bash
pip install -r requirements.txt
TRIPWIRE_PASSWORD=... python app.py        # http://localhost:5000
```
`run.bat` / `run_tunnel.bat` launch it on Windows (+ Cloudflare tunnel); see
`REMOTE_ACCESS.md`. Env: `TRIPWIRE_PASSWORD`, `TRIPWIRE_SECRET_KEY`,
`ANTHROPIC_API_KEY` (news synthesis + chat).

---

## App B — Tripwire Portfolio (`portfolio.py`, `docs/index.html`)

A "My Stocks"-style portfolio tracker with watchlists, holdings, transactions,
charts, and price alerts. Two editions that share a JSON data format:

1. **Server edition** (`portfolio.py`): single-file Flask + SQLite, port 5010,
   password login, optional GitHub Gist cloud backup. Uses `yfinance` directly, so
   it can show richer stats (P/E, EPS, beta) the phone edition can't.
2. **Phone-only edition** (`docs/index.html`): pure client-side PWA on GitHub Pages,
   all data in `localStorage` (`pfapp_v1`), quotes fetched through public CORS
   relays (allorigins → corsproxy.io → codetabs, with failover). No server/login.

### Transaction tracking (v3, both editions)

Average-cost accounting; **transactions are the source of truth**, positions are
computed from them (`recompute()`): buys move avg cost, sells lock in realized P/L
(oversell blocked), dividends accrue as income. Old `shares+cost` data migrates to
an initial buy on first load. Trades render as B/S/D markers on the chart.

### Alerts (phone edition)

Price threshold triggers checked every ~10s refresh; fire once then flip inactive.
Delivered via Browser Notification API (+ `requireInteraction` for iOS), optional
**Discord webhook**, and optional **Formspree email**; history kept in
`pfapp_alert_history` (last 50). All alert config is per-device in `localStorage`.

### Run it

```bash
pip install flask yfinance
PORTFOLIO_DEMO=1 python portfolio.py       # simulated data, no internet
python portfolio.py                        # live; http://localhost:5010, pw "tripwire"
# Phone edition:
python3 -m http.server 5555                # serves docs/ ; open /?demo=1 for mock data
```
Deploy: phone edition auto-deploys to GitHub Pages from `/docs` on push to `main`
(`https://gregcommit.github.io/tripwire/`); server edition via `render.yaml` /
`Procfile`. See `PORTFOLIO.md`. Env: `PORTFOLIO_PASSWORD`, `PORTFOLIO_PORT`,
`PORTFOLIO_DEMO`, `PORTFOLIO_GIST_TOKEN`/`PORTFOLIO_GIST_ID`, `PORTFOLIO_SECRET_KEY`.

---

## Testing without internet

Both apps rely on Yahoo Finance, which is **firewalled in the Claude Code sandbox**
(yfinance / relay calls return a 403 proxy error). To exercise logic here:
- App B phone edition: `mock_relay.py` serves canned Yahoo JSON; or `?demo=1`.
- App B server / App A: `PORTFOLIO_DEMO=1` (App B) simulates quotes. App A has no
  demo mode — its rule math can be checked offline via `backtest.py` against cached
  data.

## Conventions

- The phone edition (`docs/index.html`) is intentionally a **single self-contained
  file** (inlined CSS/JS) for offline capability and GitHub Pages. Keep it that way.
- App B's two editions must keep a **compatible JSON export/import format**.
- App A's `backtest.py` must stay **import-free of `app.py`**; keep the two rule
  implementations in sync and use `python backtest.py parity` to verify.
- Both apps default to a weak password (`tripwire`) — never expose either to the
  internet without setting the real password env var (+ a tunnel, not open ports).
