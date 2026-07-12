# Tripwire Portfolio — mobile stock portfolio tracker

A mobile-first, installable web app (PWA) in the spirit of the classic
**"My Stocks"** app: watchlists + holdings, near-real-time quotes from
**Yahoo Finance**, pre/post-market prices, charts, key statistics, news,
and price alerts. Single self-contained file: `portfolio.py`.

It runs alongside the main Tripwire dashboard (port 5000) on its own port
(**5010**) with its own database, so the two never interfere.

## Quick start

```
pip install flask yfinance
python portfolio.py
```

Open http://localhost:5010 and sign in (default password `tripwire` — see
**Security** below).

On Windows, `run_portfolio.bat` does all of this and also starts a
Cloudflare tunnel (if `cloudflared.exe` is present, same one-time setup as
in `REMOTE_ACCESS.md`) so you can open it from your phone anywhere.

### Try it without internet / market data

```
set PORTFOLIO_DEMO=1        (Windows)     PORTFOLIO_DEMO=1 python portfolio.py   (mac/linux)
```

Demo mode seeds a sample portfolio and simulates ticking quotes, charts,
search and news — useful for exploring the UI.

## Phone-only edition (no server, no accounts) — `docs/`

`docs/index.html` is a second, fully client-side build of the app for when
you don't want to run or host *anything*: it keeps all data in the
phone's browser storage and fetches Yahoo quotes directly from the phone
through free public CORS relays (allorigins → corsproxy.io → codetabs,
with automatic failover). No login, no server, no API key, no new account.

**Enable it once (from any browser, phone included):** repo → *Settings →
Pages → Source: Deploy from a branch → `main` + `/docs` → Save*. A minute
later the app is live at `https://<user>.github.io/tripwire/` — open it on
the phone and *Add to Home Screen*.

The phone edition also has a full **transaction tracker** (v3): record
buys, sells and dividends per symbol — average cost, realized vs
unrealized P/L and dividend income are computed automatically, and your
trades appear as B/S/D markers on the price chart, like My Stocks.

Notes:
- Data (portfolios, positions, transactions, alerts, notes) lives **on
  the device**, private by design. Use *Settings → Backup → Export file*
  now and then — the file is interchangeable with the server edition's
  export format (transactions survive phone-edition roundtrips).
- Quote requests travel via public relays (they see only ticker symbols,
  never credentials). If one relay is down the app rotates to the next;
  *Settings → Test connection* shows which one is active.
- Statistics are limited to what Yahoo's public chart API provides (no
  P/E/EPS — those need the authenticated API the server edition uses).
- Add `?demo=1` to the URL for simulated data.

## Live data — how it connects

There is **no API key to set up**. Any time the app runs *without*
`PORTFOLIO_DEMO=1`, every quote, chart, search and news item comes live
from Yahoo Finance (the same public API the Yahoo Finance app uses; the
`yfinance` library is the fallback path). Quotes refresh every 10 seconds
while the app is open. Demo mode exists only for trying the UI offline —
just don't set the variable and you're on live data.

## Run it in the cloud (phone-only, no laptop needed)

The recommended setup: host the app on Render's free tier, then install
it on your phone from the Render URL. Your laptop can stay off; the URL
is permanent (unlike the trycloudflare one).

1. Push this repo to your GitHub (already done if you're reading this on
   GitHub).
2. Sign up at https://render.com (free) → **New → Blueprint** → connect
   this repo. Render reads `render.yaml` and creates the service.
3. In the service's **Environment** tab set `PORTFOLIO_PASSWORD` to a
   strong password. (Optionally `PORTFOLIO_GIST_TOKEN` /
   `PORTFOLIO_GIST_ID` — see **Cloud backup** below.)
4. Open `https://tripwire-portfolio.onrender.com` (or whatever name you
   picked) on your phone and *Add to Home Screen*:
   - **iPhone (Safari):** Share button → *Add to Home Screen*.
   - **Android (Chrome):** menu ⋮ → *Add to Home screen* / *Install app*.

Free-tier notes:

- The instance **sleeps after ~15 min idle**; the first open after a
  break takes ~30–60 s to wake. Everything is instant after that.
- The free disk is **wiped on every deploy/restart**, so turn on cloud
  backup (below) — with the two Gist env vars set, the app restores your
  data automatically on boot. Without backup you'd re-enter holdings
  after a redeploy.
- Price alerts are evaluated while the app is open on some device (each
  refresh checks them) — same behavior as My Stocks.

Any other Python host works the same way (`Procfile` included):
`gunicorn -w 1 --threads 16 -b 0.0.0.0:$PORT portfolio:app`.

### Alternative: keep running it from the laptop

`run_portfolio.bat` still works as before — starts the app locally plus a
Cloudflare tunnel, and you open the printed `https://….trycloudflare.com`
URL on your phone. Downsides: the laptop must stay on, and the free
tunnel URL changes each run.

## Cloud backup (data + settings survive anything)

The app can snapshot everything you'd hate to lose — portfolios,
positions, cost bases, alerts — to a **private GitHub Gist** after every
change (debounced a few seconds):

1. Create a token at github.com → *Settings → Developer settings →
   Personal access tokens (classic)* → **only the `gist` scope**.
2. In the app: ⚙ Settings → **Cloud backup** → paste the token →
   *Enable auto-backup*. The app creates a private Gist and shows its id.
3. On a cloud host, also set env vars `PORTFOLIO_GIST_TOKEN` (the same
   token) and `PORTFOLIO_GIST_ID` (the id from step 2). Then a fresh
   deploy with an empty database **restores itself automatically** on
   boot.

Manual controls in Settings: *Back up now*, *Restore*, *Disable*, plus
**Export file / Import file** for keeping your own JSON copies or moving
data between a laptop install and a cloud install.

## Features

- **Portfolio & watchlist** — track any Yahoo Finance symbol (stocks, ETFs,
  indices, crypto, FX). A symbol with 0 shares is watch-only; give it
  shares + average cost and it becomes a holding.
- **Live prices** — auto-refresh every 10 s (configurable 5–60 s in
  Settings), only while the app is visible. Rows flash green/red on ticks.
- **Pre-market / after-hours** prices shown under each symbol, like My Stocks.
- **Summary card** — total value, day P/L, total P/L, cost basis.
- **Tap the colored pill** to cycle % change → $ change → market value
  (holdings) / market cap.
- **Market indices strip** — S&P 500, Dow, Nasdaq — and a market-state
  indicator (pre / open / after hours / closed, US Eastern).
- **Detail view** — touch-scrubbable chart (1D 5D 1M 6M YTD 1Y 5Y MAX),
  key stats (day/52-week range, volume, market cap, P/E, EPS, dividend
  yield, beta…), latest news, and your position with gain/loss.
- **Price alerts** — "rises above / falls below" per symbol, checked on
  every refresh; fires an in-app banner and (if you enable them in
  Settings) a browser notification.
- **Multiple portfolios** — tabs at the top, create/rename/delete.
- **Search** — Yahoo's symbol search by ticker or company name.
- **Sort** — added order, symbol, % change, or market value (⇅ button).

## Where data lives

- SQLite at `~/.tripwire_portfolio/portfolio.db` (portfolios, positions,
  alerts). Delete the folder to reset.
- Quotes come straight from Yahoo Finance (chart API first, `yfinance` as
  fallback) with a ~10 s cache; nothing is stored long-term.

## Security

Same model as the main Tripwire app: a single password gate.

- Set a real password before tunneling to the internet:
  `set PORTFOLIO_PASSWORD=your-strong-password`
- Without it the app uses the default password `tripwire` and the launcher
  warns you.
- `PORTFOLIO_NO_TUNNEL=1` keeps `run_portfolio.bat` local-only.

## Env reference

| Variable              | Default    | Meaning                                    |
|-----------------------|------------|--------------------------------------------|
| `PORTFOLIO_PASSWORD`  | `tripwire` | Login password                             |
| `PORTFOLIO_PORT`/`PORT`| `5010`    | HTTP port (cloud hosts set `PORT`)         |
| `PORTFOLIO_DEMO`      | unset      | `1` = simulated market data (live is default) |
| `PORTFOLIO_NO_TUNNEL` | unset      | `1` = launcher skips cloudflared           |
| `PORTFOLIO_SECRET_KEY`| dev value  | Flask session secret (set on cloud hosts)  |
| `PORTFOLIO_DATA_DIR`  | `~/.tripwire_portfolio` | Directory for the SQLite DB   |
| `PORTFOLIO_GIST_TOKEN`| unset      | GitHub token (gist scope) for cloud backup |
| `PORTFOLIO_GIST_ID`   | unset      | Gist to auto-restore from on empty boot    |
