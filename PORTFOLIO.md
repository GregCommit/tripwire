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

## Install it on your phone (the "app" part)

1. Run `run_portfolio.bat` and open the printed `https://….trycloudflare.com`
   URL on your phone (or `http://<laptop-ip>:5010` on the same WiFi).
2. Sign in.
3. **iPhone (Safari):** Share button → *Add to Home Screen*.
   **Android (Chrome):** menu ⋮ → *Add to Home screen* / *Install app*.
4. Launch it from the icon — it opens full-screen like a native app.

Note: the free trycloudflare URL changes each run, so re-add the icon if
you restart the tunnel, or keep the same-WiFi address for a stable icon.

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

| Variable             | Default    | Meaning                          |
|----------------------|------------|----------------------------------|
| `PORTFOLIO_PASSWORD` | `tripwire` | Login password                   |
| `PORTFOLIO_PORT`     | `5010`     | HTTP port                        |
| `PORTFOLIO_DEMO`     | unset      | `1` = simulated market data      |
| `PORTFOLIO_NO_TUNNEL`| unset      | `1` = launcher skips cloudflared |
| `PORTFOLIO_SECRET_KEY`| dev value | Flask session secret             |
