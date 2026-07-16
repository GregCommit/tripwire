# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Tripwire Portfolio** is a mobile stock portfolio tracker app modeled after the classic "My Stocks" app. It tracks watchlists and holdings with real-time Yahoo Finance quotes, pre/post-market prices, charts, key statistics, news, and price alerts.

The project has **two independent implementations**:

1. **Server edition** (`portfolio.py`): Single-file Flask app with SQLite database. Runs on port 5010 with user authentication. Recommended for persistent cloud deployment (Render, Heroku).

2. **Phone-only edition** (`/docs/index.html`): Pure client-side PWA deployed to GitHub Pages. All data lives in browser localStorage. No server, no login, no API keys. Recommended for users who want zero infrastructure.

Both editions are fully functional and compatible (can export/import each other's data formats).

## Architecture & Technical Decisions

### Why Two Editions?

The phone-only edition (`/docs`) was added after the server edition because it's more practical: users want to run the app on their phone without keeping a laptop on. The phone edition handles 90% of use cases and requires zero backend infrastructure.

### Data Layer

**Server edition**: SQLite database at `~/.tripwire_portfolio/portfolio.db` with tables for portfolios, positions, transactions, alerts, and settings.

**Phone edition**: All data in `localStorage` under key `pfapp_v1`. Serialized as JSON. Supports export/import to JSON files (compatible with server edition).

**Transaction tracking** (v3, both editions): Uses average-cost accounting method. Transactions are the source of truth; positions are computed from them on load:
- Buy: adjusts average cost, adds shares
- Sell: locks in realized P/L, removes shares (prevents oversell)
- Dividend: adds to dividend income without affecting shares

Migration from pre-transaction data: old `shares + cost` fields automatically converted to an initial buy transaction on first load.

### Quote Data (No API Keys)

Both editions use **public Yahoo Finance APIs** without authentication:
- Chart endpoint: `/v8/finance/chart/{symbol}?range=1d|5d|1mo|6mo|ytd|1y|5y|max`
- Search endpoint: `/v1/finance/search?q={query}`

**Phone edition**: Fetches through public CORS relays (allorigins, corsproxy.io, codetabs) with automatic failover. Relay index stored in `localStorage` so the app remembers which one worked last.

**Server edition**: Direct access to `yfinance` library (Python wrapper around Yahoo's public APIs).

Quote caching: 10-second cache in both editions to avoid hammering relays. Stale-on-error fallback preserves last-known quotes if fetch fails.

### Alert System (v4)

Alerts are price threshold triggers ("above $X" or "below $X") checked on every quote refresh (~10 seconds). When triggered, they fire once and flip `active: 0`.

**Multi-channel notifications**:
1. Browser Notification API (persistent on iOS with `requireInteraction: true`)
2. Discord webhook (POST to user's webhook URL; instant push notification to Discord)
3. Formspree email (POST form data to Formspree endpoint for email delivery)

Alerts logged to `pfapp_alert_history` in localStorage (last 50 kept). User can view history in "Alert history" view.

## File Structure

```
tripwire/
├── CLAUDE.md                 # This file
├── PORTFOLIO.md              # User-facing documentation
├── portfolio.py              # Server edition: Flask app (single file, ~1500 lines)
├── portfolio.db              # (Created at runtime) Server SQLite database
├── run_portfolio.bat         # Windows launcher + Cloudflare tunnel
├── Procfile                  # For cloud deployment (gunicorn)
├── render.yaml               # Render.com deployment config
│
└── docs/                     # Phone-only PWA (GitHub Pages)
    ├── index.html            # Single-file app (5000+ lines, includes CSS + JS)
    ├── sw.js                 # Service worker for caching (v3 shell versioning)
    ├── manifest.webmanifest  # PWA metadata
    ├── icon-192.png          # 192x192 icon
    ├── icon-512.png          # 512x512 icon
    └── apple-touch-icon.png  # iOS home screen icon (180x180)
```

## Key Code Sections (Phone Edition)

The phone edition is a single ~5000-line HTML file with inlined CSS and JavaScript. Key logical sections:

- **Lines 1-437**: HTML skeleton (header, tabs, sheets for detail/add/settings/webhook config)
- **Lines 24-400**: CSS (dark theme variables, flexbox layout, animations)
- **Lines 440-500**: Initialization (localStorage load, state object `S`, database `DB`)
- **Lines 452-481**: Transaction accounting (`recompute()` function, average-cost logic)
- **Lines 536-551**: Market state detection (US/Eastern timezone, market hours)
- **Lines 553-584**: Yahoo quote fetching via CORS relays with failover
- **Lines 847-861**: `checkAlerts()` - alert triggering logic
- **Lines 1192-1350**: Alert firing, notification, history, and webhook integration
- **Lines 1374-1500**: Detail view rendering (chart, stats, transactions, alerts)
- **Lines 1600-1800**: Portfolio/position CRUD operations
- **Lines 1900+**: Settings, export/import, search

## Development Workflow

### Testing the Phone Edition

**Demo mode** (no internet required):
```bash
# macOS/Linux
PORTFOLIO_DEMO=1 python3 -m http.server 5555  # Serves docs/ folder
# Open http://localhost:5555/?demo=1

# Or for live testing with mock relay:
python mock_relay.py &  # Serves canned Yahoo responses on port 5099
```

**With real quotes** (requires internet):
```bash
python3 -m http.server 5555  # Serves docs/ folder on port 5555
# Open http://localhost:5555 (no ?demo=1)
```

The app hydrates instantly from localStorage (you see cached data immediately) and fetches fresh quotes in background.

### Testing the Server Edition

```bash
pip install flask yfinance
PORTFOLIO_DEMO=1 python portfolio.py      # Demo mode (simulated quotes)
python portfolio.py                        # Live mode (real Yahoo data)
# Open http://localhost:5010 with password "tripwire"
```

### Common Test Scenarios

1. **Add a position**: Search ticker → Add symbol → Enter shares & cost → Refresh to see live quote
2. **Create an alert**: Detail view → Add alert → Wait for price threshold to trigger
3. **Transaction**: Detail view → New transaction → Record buy/sell/dividend → Chart shows B/S/D markers
4. **Export/import**: Settings → Export → (share file) → another instance → Import → verify data
5. **Alert notifications**: Create alert → enable Discord webhook → wait for trigger → check Discord channel

## Deployment

**Phone edition** (GitHub Pages, automatic):
- Hosted at `https://gregcommit.github.io/tripwire/` 
- Deploy by pushing changes to `main` branch (GitHub Actions auto-deploys `/docs` folder)
- Accessed via PWA: open on phone → Add to Home Screen

**Server edition** (Render free tier, recommended):
- Create account at render.com
- Connect this repo as Blueprint
- Set `PORTFOLIO_PASSWORD` env var
- Optional: set `PORTFOLIO_GIST_TOKEN` + `PORTFOLIO_GIST_ID` for cloud backup to GitHub Gist

## Key Environment Variables

| Var | Default | Used By | Purpose |
|-----|---------|---------|---------|
| `PORTFOLIO_DEMO` | unset | Both | `1` = use simulated data (no internet) |
| `PORTFOLIO_PASSWORD` | `tripwire` | Server only | Login password |
| `PORTFOLIO_PORT` | `5010` | Server only | HTTP port |
| `PORTFOLIO_GIST_TOKEN` | unset | Server only | GitHub token (gist scope) for auto-backup |
| `PORTFOLIO_GIST_ID` | unset | Server only | Gist ID to auto-restore from on boot |
| `PORTFOLIO_SECRET_KEY` | dev value | Server only | Flask session secret (set on cloud hosts) |

## Important Behavioral Notes

1. **Quote caching**: Quotes are cached for ~10 seconds per symbol to avoid hammering public relays. Last-known quotes are preserved on network error.

2. **Alert one-fire behavior**: Once an alert triggers, `active` flips to 0 and it won't fire again until re-enabled by the user. This prevents spam.

3. **Market hours detection**: Uses `Intl.DateTimeFormat` with `America/New_York` timezone to detect pre/open/post/closed market state. No dependency on system timezone.

4. **Transaction ordering**: Transactions are sorted by date before recomputing positions. Out-of-order dates can produce incorrect cost basis.

5. **Service worker caching (v3)**: Phone edition caches shell assets (HTML, manifest, icons) but always fetches Yahoo quotes from network (with fallback to cache). This ensures fresh data while remaining offline-capable.

6. **No persistent server state**: Server edition stores only portfolios/positions/alerts in SQLite. Quotes are fetched fresh on every request (not cached server-side) to ensure liveness.

## Browser Support

- **Phone edition**: iOS Safari 12+, Chrome/Firefox on Android (all modern versions)
- **Server edition**: Any browser that can reach the server

## Notes for Contributors

- The phone edition is intentionally a single file to simplify offline capabilities and GitHub Pages deployment. Keep it that way.
- Both editions must maintain compatible data export/import formats (JSON schema for portfolios/positions/alerts/transactions).
- Always test chart data parsing with real and simulated Yahoo responses (see `mock_relay.py`).
- When adding features, test in both demo mode (no internet) and live mode (real quotes).
- Alerts should work across phone/server editions transparently.
