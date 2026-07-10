"""
Tripwire Portfolio - mobile stock portfolio tracker (single file, self-contained)
=================================================================================
A mobile-first, installable web app (PWA) in the spirit of "My Stocks":
watchlists + holdings, near-real-time Yahoo Finance quotes with pre/post
market, charts, key stats, news, and price alerts.

Run:  python portfolio.py
Open: http://localhost:5010   (add to your phone's home screen for app mode)

Env:
  PORTFOLIO_PASSWORD    login password (default "tripwire" - change it!)
  PORTFOLIO_PORT / PORT port (default 5010; cloud hosts set PORT)
  PORTFOLIO_DEMO=1      simulated quotes, no internet needed (try the UI)
  PORTFOLIO_DATA_DIR    where portfolio.db lives (default ~/.tripwire_portfolio)
  PORTFOLIO_GIST_TOKEN  GitHub token (gist scope) for cloud backup
  PORTFOLIO_GIST_ID     Gist id to restore from on a fresh deploy

Cloud deploy (see PORTFOLIO.md): gunicorn -w 1 --threads 16 -b 0.0.0.0:$PORT portfolio:app
"""

import sqlite3, threading, time, json, os, logging, math, random, struct, zlib, hashlib
import urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

try:
    from zoneinfo import ZoneInfo
    EASTERN = ZoneInfo("America/New_York")
except Exception:
    EASTERN = None

from flask import Flask, jsonify, request, Response, session, redirect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("portfolio")

app = Flask(__name__)
app.secret_key = os.environ.get("PORTFOLIO_SECRET_KEY", "tripwire-portfolio-dev-secret")

PORT = int(os.environ.get("PORT") or os.environ.get("PORTFOLIO_PORT") or "5010")
DEMO = os.environ.get("PORTFOLIO_DEMO") == "1"
AUTH_PASSWORD = os.environ.get("PORTFOLIO_PASSWORD")
if not AUTH_PASSWORD:
    AUTH_PASSWORD = "tripwire"
    log.warning("PORTFOLIO_PASSWORD not set — using default password 'tripwire'. "
                "Set PORTFOLIO_PASSWORD env var before exposing this to the internet.")

if not DEMO:
    import yfinance as yf

worker_pool = ThreadPoolExecutor(max_workers=8)

INDICES = [("^GSPC", "S&P 500"), ("^DJI", "Dow"), ("^IXIC", "Nasdaq")]
INDEX_NAMES = dict(INDICES)

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

_data_dir = os.environ.get("PORTFOLIO_DATA_DIR")
DB_PATH = (Path(_data_dir) if _data_dir else Path.home() / ".tripwire_portfolio") / "portfolio.db"

def get_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS portfolios (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY,
                portfolio_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                shares REAL DEFAULT 0,
                cost REAL DEFAULT 0,           -- average cost per share
                sort_order INTEGER DEFAULT 0,
                added_at INTEGER,
                UNIQUE(portfolio_id, symbol)
            );
            CREATE TABLE IF NOT EXISTS price_alerts (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                kind TEXT NOT NULL,            -- 'above' | 'below'
                threshold REAL NOT NULL,
                active INTEGER DEFAULT 1,
                created_at INTEGER,
                triggered_at INTEGER,
                triggered_price REAL
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        n = conn.execute("SELECT COUNT(*) c FROM portfolios").fetchone()["c"]
        if n == 0:
            conn.execute("INSERT INTO portfolios(name, sort_order) VALUES('My Portfolio', 0)")
            if DEMO:
                pid = conn.execute("SELECT id FROM portfolios").fetchone()["id"]
                seed = [("AAPL", "Apple Inc.", 25, 168.40), ("MSFT", "Microsoft Corp.", 10, 310.25),
                        ("NVDA", "NVIDIA Corp.", 12, 96.10), ("TSLA", "Tesla, Inc.", 0, 0),
                        ("AMZN", "Amazon.com, Inc.", 8, 141.72), ("SPY", "SPDR S&P 500 ETF", 15, 505.30)]
                for i, (s, nm, sh, c) in enumerate(seed):
                    conn.execute("INSERT INTO positions(portfolio_id,symbol,name,shares,cost,sort_order,added_at) "
                                 "VALUES(?,?,?,?,?,?,?)", (pid, s, nm, sh, c, i, int(time.time())))
        conn.commit()

def get_setting(key, default=None):
    with get_db() as conn:
        r = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r["value"] if r else default

def set_setting(key, value):
    with get_db() as conn:
        if value is None:
            conn.execute("DELETE FROM settings WHERE key=?", (key,))
        else:
            conn.execute("INSERT INTO settings(key,value) VALUES(?,?) "
                         "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
        conn.commit()

# ─────────────────────────────────────────────────────────────────────────────
# CACHE (TTL + stale-on-error so a flaky Yahoo response never blanks the UI)
# ─────────────────────────────────────────────────────────────────────────────

_cache = {}
_cache_lock = threading.Lock()

def cached(key, ttl, fn):
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now < hit[0]:
            return hit[1]
    try:
        val = fn()
    except Exception as e:
        if hit:  # serve stale rather than failing
            log.warning("cache %s: fetch failed (%s), serving stale", key, e)
            return hit[1]
        raise
    with _cache_lock:
        _cache[key] = (now + ttl, val)
    return val

# ─────────────────────────────────────────────────────────────────────────────
# MARKET CLOCK (US/Eastern)
# ─────────────────────────────────────────────────────────────────────────────

def _now_eastern():
    if EASTERN:
        return datetime.now(EASTERN)
    return datetime.now(timezone.utc) - timedelta(hours=5)

def market_state():
    """'pre' | 'open' | 'post' | 'closed' based on US equity hours."""
    now = _now_eastern()
    if now.weekday() >= 5:
        return "closed"
    mins = now.hour * 60 + now.minute
    if 4 * 60 <= mins < 9 * 60 + 30:   return "pre"
    if 9 * 60 + 30 <= mins < 16 * 60:  return "open"
    if 16 * 60 <= mins < 20 * 60:      return "post"
    return "closed"

# ─────────────────────────────────────────────────────────────────────────────
# YAHOO FINANCE
# ─────────────────────────────────────────────────────────────────────────────

def _yahoo_json(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
        "Accept": "application/json",
        "Accept-Encoding": "identity",
    })
    with urllib.request.urlopen(req, timeout=12) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _quote_via_chart(symbol):
    """One lightweight request to Yahoo's v8 chart endpoint: price, prev close,
    day range, volume and pre/post-market trades — no auth crumb needed."""
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
           + urllib.parse.quote(symbol)
           + "?range=1d&interval=1m&includePrePost=true")
    data = _yahoo_json(url)
    result = (data.get("chart") or {}).get("result")
    if not result:
        raise ValueError(f"no chart data for {symbol}")
    r = result[0]
    meta = r.get("meta") or {}
    price = meta.get("regularMarketPrice")
    prev = meta.get("chartPreviousClose") or meta.get("previousClose")
    ts = r.get("timestamp") or []
    closes, vols = [], []
    try:
        q0 = r["indicators"]["quote"][0]
        closes = q0.get("close") or []
        vols = q0.get("volume") or []
    except Exception:
        pass
    # classify pre/post trades using the session boundaries Yahoo reports
    pre_price = post_price = None
    ctp = meta.get("currentTradingPeriod") or {}
    reg = ctp.get("regular") or {}
    reg_start, reg_end = reg.get("start"), reg.get("end")
    if ts and closes and reg_start and reg_end:
        for t, c in zip(ts, closes):
            if c is None:
                continue
            if t < reg_start:
                pre_price = c
            elif t >= reg_end:
                post_price = c
            elif price is None:
                price = c
    if price is None:
        raise ValueError(f"no price for {symbol}")
    day_high = meta.get("regularMarketDayHigh")
    day_low = meta.get("regularMarketDayLow")
    volume = meta.get("regularMarketVolume")
    if (day_high is None or day_low is None) and ts and closes and reg_start and reg_end:
        rc = [c for t, c in zip(ts, closes) if c is not None and reg_start <= t < reg_end]
        if rc:
            day_high = day_high if day_high is not None else max(rc)
            day_low = day_low if day_low is not None else min(rc)
    if volume is None and vols:
        volume = sum(v for v in vols if v)
    q = {
        "symbol": symbol,
        "name": meta.get("shortName") or meta.get("longName"),
        "currency": meta.get("currency") or "USD",
        "price": round(float(price), 4),
        "prev_close": round(float(prev), 4) if prev else None,
        "day_high": round(float(day_high), 4) if day_high else None,
        "day_low": round(float(day_low), 4) if day_low else None,
        "volume": int(volume) if volume else None,
        "week52_high": meta.get("fiftyTwoWeekHigh"),
        "week52_low": meta.get("fiftyTwoWeekLow"),
        "market_cap": None,
        "pre_price": round(float(pre_price), 4) if pre_price else None,
        "post_price": round(float(post_price), 4) if post_price else None,
        "ts": int(time.time()),
    }
    return q

def _quote_via_yf(symbol):
    """Fallback path through yfinance (handles cookies/crumbs internally)."""
    t = yf.Ticker(symbol)
    fi = t.fast_info
    price = getattr(fi, "last_price", None)
    prev = getattr(fi, "previous_close", None)
    if not price:
        hist = t.history(period="2d")
        if hist.empty:
            raise ValueError(f"no data for {symbol}")
        price = float(hist["Close"].iloc[-1])
        if not prev and len(hist) > 1:
            prev = float(hist["Close"].iloc[-2])
    q = {
        "symbol": symbol, "name": None,
        "currency": getattr(fi, "currency", None) or "USD",
        "price": round(float(price), 4),
        "prev_close": round(float(prev), 4) if prev else None,
        "day_high": getattr(fi, "day_high", None),
        "day_low": getattr(fi, "day_low", None),
        "volume": getattr(fi, "last_volume", None),
        "week52_high": getattr(fi, "year_high", None),
        "week52_low": getattr(fi, "year_low", None),
        "market_cap": getattr(fi, "market_cap", None),
        "pre_price": None, "post_price": None,
        "ts": int(time.time()),
    }
    return q

def _finish_quote(q):
    """Derive change fields shared by all quote sources."""
    prev = q.get("prev_close")
    price = q.get("price")
    if prev:
        q["change"] = round(price - prev, 4)
        q["change_pct"] = round((price - prev) / prev * 100, 2)
    else:
        q["change"] = q["change_pct"] = None
    for kind in ("pre", "post"):
        p = q.get(kind + "_price")
        base = price if kind == "post" else prev
        if p and base:
            q[kind + "_change"] = round(p - base, 4)
            q[kind + "_change_pct"] = round((p - base) / base * 100, 2)
        else:
            q[kind + "_change"] = q[kind + "_change_pct"] = None
    return q

def fetch_quote(symbol):
    def _raw():
        if DEMO:
            return _finish_quote(demo_quote(symbol))
        try:
            return _finish_quote(_quote_via_chart(symbol))
        except Exception as e:
            log.info("chart quote failed for %s (%s); falling back to yfinance", symbol, e)
            return _finish_quote(_quote_via_yf(symbol))
    return cached(f"quote:{symbol}", 10, _raw)

def fetch_quotes(symbols):
    symbols = list(dict.fromkeys(symbols))
    out = {}
    futures = {s: worker_pool.submit(fetch_quote, s) for s in symbols}
    for s, f in futures.items():
        try:
            out[s] = f.result(timeout=20)
        except Exception as e:
            log.warning("quote failed: %s (%s)", s, e)
            out[s] = {"symbol": s, "error": str(e)}
    return out

CHART_RANGES = {
    # range -> (yahoo range, interval, include prepost, cache ttl)
    "1d":  ("1d",  "2m",  True,  60),
    "5d":  ("5d",  "15m", False, 300),
    "1mo": ("1mo", "1h",  False, 900),
    "6mo": ("6mo", "1d",  False, 3600),
    "ytd": ("ytd", "1d",  False, 3600),
    "1y":  ("1y",  "1d",  False, 3600),
    "5y":  ("5y",  "1wk", False, 3600),
    "max": ("max", "1mo", False, 3600),
}

def fetch_chart(symbol, rng):
    yr, interval, prepost, ttl = CHART_RANGES[rng]
    def _raw():
        if DEMO:
            return demo_chart(symbol, rng)
        url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
               + urllib.parse.quote(symbol)
               + "?range=%s&interval=%s&includePrePost=%s" % (yr, interval, "true" if prepost else "false"))
        data = _yahoo_json(url)
        r = data["chart"]["result"][0]
        meta = r.get("meta") or {}
        ts = r.get("timestamp") or []
        closes = r["indicators"]["quote"][0].get("close") or []
        pts = [[int(t), round(float(c), 4)] for t, c in zip(ts, closes) if c is not None]
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        return {"symbol": symbol, "range": rng, "points": pts,
                "prev_close": round(float(prev), 4) if prev else None,
                "currency": meta.get("currency") or "USD"}
    return cached(f"chart:{symbol}:{rng}", ttl, _raw)

def search_symbols(query):
    def _raw():
        if DEMO:
            return demo_search(query)
        url = ("https://query2.finance.yahoo.com/v1/finance/search?q="
               + urllib.parse.quote(query) + "&quotesCount=12&newsCount=0&listsCount=0")
        data = _yahoo_json(url)
        out = []
        for it in data.get("quotes") or []:
            sym = it.get("symbol")
            if not sym:
                continue
            out.append({"symbol": sym,
                        "name": it.get("shortname") or it.get("longname") or sym,
                        "exchange": it.get("exchDisp") or it.get("exchange") or "",
                        "type": it.get("typeDisp") or it.get("quoteType") or ""})
        return out
    return cached(f"search:{query.lower()}", 300, _raw)

def fetch_detail(symbol):
    """Extra stats for the detail sheet (best effort — omit what Yahoo won't give)."""
    def _raw():
        if DEMO:
            return demo_detail(symbol)
        stats = {}
        try:
            fi = yf.Ticker(symbol).fast_info
            for key, attr in [("market_cap", "market_cap"), ("year_high", "year_high"),
                              ("year_low", "year_low"), ("avg_volume_3m", "three_month_average_volume"),
                              ("fifty_day_avg", "fifty_day_average"), ("two_hundred_day_avg", "two_hundred_day_average")]:
                v = getattr(fi, attr, None)
                if v:
                    stats[key] = float(v)
        except Exception as e:
            log.info("fast_info failed for %s: %s", symbol, e)
        try:
            fut = worker_pool.submit(lambda: yf.Ticker(symbol).info)
            info = fut.result(timeout=8) or {}
            for key, ik in [("pe", "trailingPE"), ("eps", "trailingEps"),
                            ("div_yield", "dividendYield"), ("beta", "beta"),
                            ("sector", "sector"), ("long_name", "longName")]:
                v = info.get(ik)
                if v is not None:
                    stats[key] = v
        except Exception as e:
            log.info("info failed for %s: %s", symbol, e)
        return stats
    return cached(f"detail:{symbol}", 900, _raw)

def _norm_news(items):
    out = []
    for it in (items or [])[:10]:
        title = link = pub = None
        ts = None
        c = it.get("content") if isinstance(it, dict) else None
        if isinstance(c, dict):  # yfinance >= 0.2.5x format
            title = c.get("title")
            link = ((c.get("canonicalUrl") or {}).get("url")
                    or (c.get("clickThroughUrl") or {}).get("url"))
            pub = (c.get("provider") or {}).get("displayName")
            pd = c.get("pubDate")
            if pd:
                try:
                    ts = int(datetime.fromisoformat(pd.replace("Z", "+00:00")).timestamp())
                except Exception:
                    pass
        elif isinstance(it, dict):  # legacy format
            title, link, pub = it.get("title"), it.get("link"), it.get("publisher")
            ts = it.get("providerPublishTime")
        if title and link:
            out.append({"title": title, "link": link, "publisher": pub or "", "ts": ts})
    return out

def fetch_news(symbol):
    def _raw():
        if DEMO:
            return demo_news(symbol)
        try:
            return _norm_news(yf.Ticker(symbol).news)
        except Exception as e:
            log.info("news failed for %s: %s", symbol, e)
            return []
    return cached(f"news:{symbol}", 600, _raw)

# ─────────────────────────────────────────────────────────────────────────────
# DEMO MODE (simulated market so the app is fully usable offline)
# ─────────────────────────────────────────────────────────────────────────────

DEMO_UNIVERSE = [
    ("AAPL", "Apple Inc.", "NASDAQ"), ("MSFT", "Microsoft Corp.", "NASDAQ"),
    ("GOOGL", "Alphabet Inc.", "NASDAQ"), ("AMZN", "Amazon.com, Inc.", "NASDAQ"),
    ("NVDA", "NVIDIA Corp.", "NASDAQ"), ("META", "Meta Platforms, Inc.", "NASDAQ"),
    ("TSLA", "Tesla, Inc.", "NASDAQ"), ("BRK-B", "Berkshire Hathaway", "NYSE"),
    ("JPM", "JPMorgan Chase & Co.", "NYSE"), ("V", "Visa Inc.", "NYSE"),
    ("JNJ", "Johnson & Johnson", "NYSE"), ("WMT", "Walmart Inc.", "NYSE"),
    ("XOM", "Exxon Mobil Corp.", "NYSE"), ("DIS", "Walt Disney Co.", "NYSE"),
    ("NFLX", "Netflix, Inc.", "NASDAQ"), ("AMD", "Advanced Micro Devices", "NASDAQ"),
    ("INTC", "Intel Corp.", "NASDAQ"), ("BA", "Boeing Co.", "NYSE"),
    ("KO", "Coca-Cola Co.", "NYSE"), ("PLTR", "Palantir Technologies", "NASDAQ"),
    ("SPY", "SPDR S&P 500 ETF", "NYSEArca"), ("QQQ", "Invesco QQQ Trust", "NASDAQ"),
    ("VTI", "Vanguard Total Stock Market ETF", "NYSEArca"),
]
DEMO_NAMES = {s: n for s, n, _ in DEMO_UNIVERSE}
DEMO_NAMES.update({"^GSPC": "S&P 500", "^DJI": "Dow Jones Industrial", "^IXIC": "Nasdaq Composite"})

_demo_state = {}
_demo_lock = threading.Lock()

def _demo_base(symbol):
    h = int(hashlib.md5(symbol.encode()).hexdigest(), 16)
    if symbol.startswith("^"):
        return {"^GSPC": 6820.0, "^DJI": 46210.0, "^IXIC": 22930.0}.get(symbol, 5000 + h % 2000)
    return 15 + (h % 4800) / 10.0

def demo_quote(symbol):
    with _demo_lock:
        st = _demo_state.setdefault(symbol, None)
        base = _demo_base(symbol)
        if st is None:
            h = int(hashlib.md5(symbol.encode()).hexdigest(), 16)
            drift = ((h >> 8) % 900 - 450) / 10000.0  # -4.5%..+4.5% vs prev close
            st = _demo_state[symbol] = {"price": base * (1 + drift), "prev": base}
        st["price"] = max(0.5, st["price"] * (1 + random.uniform(-0.0012, 0.00125)))
        price, prev = st["price"], st["prev"]
    state = market_state()
    pre = post = None
    if state == "pre":
        pre = price * (1 + random.uniform(-0.002, 0.002))
    elif state in ("post", "closed"):
        post = price * (1 + random.uniform(-0.003, 0.003))
    return {
        "symbol": symbol, "name": DEMO_NAMES.get(symbol, symbol), "currency": "USD",
        "price": round(price, 2), "prev_close": round(prev, 2),
        "day_high": round(max(price, prev) * 1.008, 2), "day_low": round(min(price, prev) * 0.991, 2),
        "volume": random.randint(2_000_000, 90_000_000),
        "week52_high": round(base * 1.34, 2), "week52_low": round(base * 0.63, 2),
        "market_cap": round(price * random.randint(500_000_000, 3_000_000_000)),
        "pre_price": round(pre, 2) if pre else None,
        "post_price": round(post, 2) if post else None,
        "ts": int(time.time()),
    }

def demo_chart(symbol, rng):
    n = {"1d": 78, "5d": 130, "1mo": 150, "6mo": 126, "ytd": 130, "1y": 252, "5y": 260, "max": 240}[rng]
    span = {"1d": 6.5 * 3600, "5d": 5 * 86400, "1mo": 30 * 86400, "6mo": 182 * 86400,
            "ytd": 190 * 86400, "1y": 365 * 86400, "5y": 5 * 365 * 86400, "max": 12 * 365 * 86400}[rng]
    end_price = demo_quote(symbol)["price"]
    rnd = random.Random(hashlib.md5((symbol + rng).encode()).hexdigest())
    walk = [0.0]
    for _ in range(n - 1):
        walk.append(walk[-1] + rnd.uniform(-1, 1.02))
    lo, hi = min(walk), max(walk)
    spread = (hi - lo) or 1
    amp = {"1d": 0.015, "5d": 0.05, "1mo": 0.09, "6mo": 0.22, "ytd": 0.25,
           "1y": 0.35, "5y": 1.4, "max": 4.0}[rng]
    now = int(time.time())
    pts = []
    for i, w in enumerate(walk):
        rel = (w - walk[-1]) / spread * amp
        t = now - int(span * (1 - i / (n - 1)))
        pts.append([t, round(end_price * (1 + rel), 2)])
    prev = _demo_state.get(symbol, {}).get("prev") or pts[0][1]
    return {"symbol": symbol, "range": rng, "points": pts,
            "prev_close": round(prev, 2), "currency": "USD"}

def demo_search(query):
    q = query.lower()
    return [{"symbol": s, "name": n, "exchange": e, "type": "Equity"}
            for s, n, e in DEMO_UNIVERSE if q in s.lower() or q in n.lower()][:12]

def demo_detail(symbol):
    rnd = random.Random(symbol)
    return {"market_cap": round(_demo_base(symbol) * rnd.randint(10**9, 3 * 10**9)),
            "pe": round(rnd.uniform(9, 60), 1), "eps": round(rnd.uniform(0.5, 12), 2),
            "div_yield": round(rnd.uniform(0, 2.8), 2), "beta": round(rnd.uniform(0.6, 2.1), 2),
            "sector": "Technology", "long_name": DEMO_NAMES.get(symbol, symbol)}

def demo_news(symbol):
    name = DEMO_NAMES.get(symbol, symbol)
    now = int(time.time())
    return [
        {"title": f"{name} rises as analysts lift price targets", "link": "https://finance.yahoo.com",
         "publisher": "Demo Wire", "ts": now - 3600},
        {"title": f"What to watch ahead of {name} earnings", "link": "https://finance.yahoo.com",
         "publisher": "Demo Journal", "ts": now - 14400},
        {"title": f"{name} announces expanded buyback program", "link": "https://finance.yahoo.com",
         "publisher": "Demo Times", "ts": now - 86400},
    ]

# ─────────────────────────────────────────────────────────────────────────────
# ALERTS
# ─────────────────────────────────────────────────────────────────────────────

def check_alerts(quotes):
    """Compare active alerts to fresh quotes; mark triggered ones. Returns fired list."""
    fired = []
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM price_alerts WHERE active=1").fetchall()
        for a in rows:
            q = quotes.get(a["symbol"])
            if not q or q.get("error") or q.get("price") is None:
                continue
            price = q["price"]
            hit = (a["kind"] == "above" and price >= a["threshold"]) or \
                  (a["kind"] == "below" and price <= a["threshold"])
            if hit:
                conn.execute("UPDATE price_alerts SET active=0, triggered_at=?, triggered_price=? WHERE id=?",
                             (int(time.time()), price, a["id"]))
                fired.append({"id": a["id"], "symbol": a["symbol"], "kind": a["kind"],
                              "threshold": a["threshold"], "price": price})
        conn.commit()
    if fired:
        schedule_backup()
    return fired

def all_alerts():
    with get_db() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM price_alerts ORDER BY active DESC, created_at DESC").fetchall()]

# ─────────────────────────────────────────────────────────────────────────────
# CLOUD BACKUP (snapshot -> private GitHub Gist; survives ephemeral host disks)
# ─────────────────────────────────────────────────────────────────────────────

GIST_FILENAME = "tripwire-portfolio-backup.json"

def snapshot():
    with get_db() as conn:
        pfs = []
        for pf in conn.execute("SELECT * FROM portfolios ORDER BY sort_order, id").fetchall():
            pfs.append({
                "name": pf["name"], "sort_order": pf["sort_order"],
                "positions": [dict(r) for r in conn.execute(
                    "SELECT symbol,name,shares,cost,sort_order,added_at FROM positions "
                    "WHERE portfolio_id=? ORDER BY sort_order, id", (pf["id"],)).fetchall()],
            })
        alerts = [dict(r) for r in conn.execute(
            "SELECT symbol,kind,threshold,active,created_at,triggered_at,triggered_price "
            "FROM price_alerts").fetchall()]
    return {"app": "tripwire-portfolio", "version": 1, "exported_at": int(time.time()),
            "portfolios": pfs, "alerts": alerts}

def restore_snapshot(data):
    if not isinstance(data, dict) or not isinstance(data.get("portfolios"), list):
        raise ValueError("not a portfolio backup file")
    with get_db() as conn:
        conn.execute("DELETE FROM positions")
        conn.execute("DELETE FROM price_alerts")
        conn.execute("DELETE FROM portfolios")
        for i, pf in enumerate(data["portfolios"]):
            cur = conn.execute("INSERT INTO portfolios(name, sort_order) VALUES(?,?)",
                               (pf.get("name") or f"Portfolio {i+1}", pf.get("sort_order", i)))
            pid = cur.lastrowid
            for j, p in enumerate(pf.get("positions") or []):
                if not p.get("symbol"):
                    continue
                conn.execute("INSERT OR IGNORE INTO positions"
                             "(portfolio_id,symbol,name,shares,cost,sort_order,added_at) VALUES(?,?,?,?,?,?,?)",
                             (pid, str(p["symbol"]).upper(), p.get("name"),
                              float(p.get("shares") or 0), float(p.get("cost") or 0),
                              p.get("sort_order", j), p.get("added_at") or int(time.time())))
        for a in data.get("alerts") or []:
            if not a.get("symbol") or a.get("kind") not in ("above", "below"):
                continue
            conn.execute("INSERT INTO price_alerts"
                         "(symbol,kind,threshold,active,created_at,triggered_at,triggered_price) VALUES(?,?,?,?,?,?,?)",
                         (str(a["symbol"]).upper(), a["kind"], float(a.get("threshold") or 0),
                          1 if a.get("active", 1) else 0, a.get("created_at"),
                          a.get("triggered_at"), a.get("triggered_price")))
        if not conn.execute("SELECT 1 FROM portfolios").fetchone():
            conn.execute("INSERT INTO portfolios(name, sort_order) VALUES('My Portfolio', 0)")
        conn.commit()

def _github_request(method, url, token, payload=None):
    req = urllib.request.Request(url, method=method,
        data=json.dumps(payload).encode("utf-8") if payload is not None else None,
        headers={"Authorization": "Bearer " + token,
                 "Accept": "application/vnd.github+json",
                 "User-Agent": "tripwire-portfolio",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))

def backup_config():
    """Token/gist id come from the DB (set in the UI) or env (survives disk wipes)."""
    token = get_setting("gist_token") or os.environ.get("PORTFOLIO_GIST_TOKEN")
    gist_id = get_setting("gist_id") or os.environ.get("PORTFOLIO_GIST_ID")
    return token, gist_id

def backup_to_gist():
    token, gist_id = backup_config()
    if not token:
        return False
    files = {GIST_FILENAME: {"content": json.dumps(snapshot(), indent=1)}}
    try:
        if gist_id:
            _github_request("PATCH", "https://api.github.com/gists/" + gist_id, token, {"files": files})
        else:
            r = _github_request("POST", "https://api.github.com/gists", token,
                                {"description": "Tripwire Portfolio backup (auto-updated)",
                                 "public": False, "files": files})
            gist_id = r["id"]
            set_setting("gist_id", gist_id)
        set_setting("last_backup_ts", int(time.time()))
        set_setting("last_backup_error", None)
        log.info("portfolio backed up to gist %s", gist_id)
        return True
    except Exception as e:
        msg = "%s %s" % (getattr(e, "code", ""), getattr(e, "reason", e))
        set_setting("last_backup_error", msg.strip())
        log.warning("gist backup failed: %s", e)
        return False

_backup_timer = None
_backup_timer_lock = threading.Lock()

def schedule_backup(delay=5):
    """Debounced auto-backup after any data change."""
    token, _ = backup_config()
    if not token:
        return
    global _backup_timer
    with _backup_timer_lock:
        if _backup_timer:
            _backup_timer.cancel()
        _backup_timer = threading.Timer(delay, backup_to_gist)
        _backup_timer.daemon = True
        _backup_timer.start()

def restore_from_gist():
    token, gist_id = backup_config()
    if not (token and gist_id):
        raise ValueError("cloud backup is not configured")
    g = _github_request("GET", "https://api.github.com/gists/" + gist_id, token)
    f = (g.get("files") or {}).get(GIST_FILENAME)
    if not f:
        raise ValueError("backup file not found in gist")
    content = f.get("content")
    if f.get("truncated") and f.get("raw_url"):
        req = urllib.request.Request(f["raw_url"], headers={"User-Agent": "tripwire-portfolio"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode("utf-8")
    restore_snapshot(json.loads(content))

def maybe_restore_on_boot():
    """Fresh deploy on an ephemeral disk + env-configured backup -> pull the data back."""
    token, gist_id = backup_config()
    if not (token and gist_id):
        return
    with get_db() as conn:
        if conn.execute("SELECT COUNT(*) c FROM positions").fetchone()["c"]:
            return
    try:
        restore_from_gist()
        log.info("restored portfolio data from gist backup %s", gist_id)
    except Exception as e:
        log.warning("auto-restore from gist failed: %s", e)

def backup_status():
    token, gist_id = backup_config()
    ts = get_setting("last_backup_ts")
    return {"configured": bool(token), "gist_id": gist_id,
            "via_env": bool(not get_setting("gist_token") and os.environ.get("PORTFOLIO_GIST_TOKEN")),
            "last_backup_ts": int(ts) if ts else None,
            "last_error": get_setting("last_backup_error")}

# ─────────────────────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────────────────────

def is_authed():
    return session.get("authed") is True

def require_auth_api(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not is_authed():
            return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = (request.form.get("password") or (request.get_json(silent=True) or {}).get("password") or "")
        if pw == AUTH_PASSWORD:
            session["authed"] = True
            session.permanent = True
            return redirect("/")
        return Response(LOGIN_HTML.replace("<!--ERR-->",
                        '<div class="err">Wrong password, try again.</div>'), mimetype="text/html")
    return Response(LOGIN_HTML, mimetype="text/html")

@app.route("/logout", methods=["GET", "POST"])
def logout():
    session.clear()
    return redirect("/login")

# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────

def state_payload():
    with get_db() as conn:
        pfs = [dict(r) for r in conn.execute("SELECT * FROM portfolios ORDER BY sort_order, id").fetchall()]
        for pf in pfs:
            pf["positions"] = [dict(r) for r in conn.execute(
                "SELECT * FROM positions WHERE portfolio_id=? ORDER BY sort_order, id", (pf["id"],)).fetchall()]
    return {"portfolios": pfs, "alerts": all_alerts(), "demo": DEMO}

@app.route("/api/state")
@require_auth_api
def api_state():
    return jsonify(state_payload())

@app.route("/api/portfolios", methods=["POST"])
@require_auth_api
def api_portfolio_create():
    name = ((request.get_json(silent=True) or {}).get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    with get_db() as conn:
        conn.execute("INSERT INTO portfolios(name, sort_order) VALUES(?, "
                     "(SELECT COALESCE(MAX(sort_order),0)+1 FROM portfolios))", (name,))
        conn.commit()
    schedule_backup()
    return jsonify(state_payload())

@app.route("/api/portfolios/<int:pid>", methods=["PATCH", "DELETE"])
@require_auth_api
def api_portfolio_modify(pid):
    with get_db() as conn:
        if request.method == "DELETE":
            n = conn.execute("SELECT COUNT(*) c FROM portfolios").fetchone()["c"]
            if n <= 1:
                return jsonify({"error": "can't delete the last portfolio"}), 400
            conn.execute("DELETE FROM positions WHERE portfolio_id=?", (pid,))
            conn.execute("DELETE FROM portfolios WHERE id=?", (pid,))
        else:
            name = ((request.get_json(silent=True) or {}).get("name") or "").strip()
            if not name:
                return jsonify({"error": "name required"}), 400
            conn.execute("UPDATE portfolios SET name=? WHERE id=?", (name, pid))
        conn.commit()
    schedule_backup()
    return jsonify(state_payload())

@app.route("/api/positions", methods=["POST"])
@require_auth_api
def api_position_add():
    d = request.get_json(silent=True) or {}
    symbol = (d.get("symbol") or "").strip().upper()
    pid = d.get("portfolio_id")
    if not symbol or not pid:
        return jsonify({"error": "symbol and portfolio_id required"}), 400
    shares = float(d.get("shares") or 0)
    cost = float(d.get("cost") or 0)
    name = (d.get("name") or "").strip() or None
    with get_db() as conn:
        conn.execute("""INSERT INTO positions(portfolio_id,symbol,name,shares,cost,sort_order,added_at)
                        VALUES(?,?,?,?,?,(SELECT COALESCE(MAX(sort_order),0)+1 FROM positions WHERE portfolio_id=?),?)
                        ON CONFLICT(portfolio_id,symbol)
                        DO UPDATE SET shares=excluded.shares, cost=excluded.cost,
                                      name=COALESCE(excluded.name, positions.name)""",
                     (pid, symbol, name, shares, cost, pid, int(time.time())))
        conn.commit()
    schedule_backup()
    return jsonify(state_payload())

@app.route("/api/positions/<int:pos_id>", methods=["PATCH", "DELETE"])
@require_auth_api
def api_position_modify(pos_id):
    with get_db() as conn:
        if request.method == "DELETE":
            conn.execute("DELETE FROM positions WHERE id=?", (pos_id,))
        else:
            d = request.get_json(silent=True) or {}
            sets, vals = [], []
            for field in ("shares", "cost"):
                if field in d:
                    sets.append(f"{field}=?")
                    vals.append(float(d[field] or 0))
            if sets:
                vals.append(pos_id)
                conn.execute(f"UPDATE positions SET {', '.join(sets)} WHERE id=?", vals)
        conn.commit()
    schedule_backup()
    return jsonify(state_payload())

@app.route("/api/quotes")
@require_auth_api
def api_quotes():
    pid = request.args.get("portfolio", type=int)
    with get_db() as conn:
        if pid:
            syms = [r["symbol"] for r in conn.execute(
                "SELECT symbol FROM positions WHERE portfolio_id=?", (pid,)).fetchall()]
        else:
            syms = [r["symbol"] for r in conn.execute("SELECT DISTINCT symbol FROM positions").fetchall()]
        alert_syms = [r["symbol"] for r in conn.execute(
            "SELECT DISTINCT symbol FROM price_alerts WHERE active=1").fetchall()]
    extra = request.args.get("symbols", "")
    extra_syms = [s.strip().upper() for s in extra.split(",") if s.strip()]
    wanted = syms + alert_syms + extra_syms + [s for s, _ in INDICES]
    quotes = fetch_quotes(wanted)
    fired = check_alerts(quotes)
    indices = []
    for s, label in INDICES:
        q = dict(quotes.get(s) or {"symbol": s, "error": "n/a"})
        q["label"] = label
        indices.append(q)
    return jsonify({"quotes": quotes, "indices": indices, "market": market_state(),
                    "fired": fired, "ts": int(time.time())})

@app.route("/api/chart/<symbol>")
@require_auth_api
def api_chart(symbol):
    rng = request.args.get("range", "1d")
    if rng not in CHART_RANGES:
        return jsonify({"error": "bad range"}), 400
    try:
        return jsonify(fetch_chart(symbol.upper(), rng))
    except Exception as e:
        log.warning("chart failed %s %s: %s", symbol, rng, e)
        return jsonify({"error": str(e)}), 502

@app.route("/api/detail/<symbol>")
@require_auth_api
def api_detail(symbol):
    symbol = symbol.upper()
    try:
        quote = fetch_quote(symbol)
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"quote": quote, "stats": fetch_detail(symbol), "news": fetch_news(symbol)})

@app.route("/api/search")
@require_auth_api
def api_search():
    q = (request.args.get("q") or "").strip()
    if len(q) < 1:
        return jsonify({"results": []})
    try:
        return jsonify({"results": search_symbols(q)})
    except Exception as e:
        log.warning("search failed %s: %s", q, e)
        return jsonify({"results": [], "error": str(e)})

@app.route("/api/alerts", methods=["POST"])
@require_auth_api
def api_alert_create():
    d = request.get_json(silent=True) or {}
    symbol = (d.get("symbol") or "").strip().upper()
    kind = d.get("kind")
    try:
        threshold = float(d.get("threshold"))
    except (TypeError, ValueError):
        return jsonify({"error": "bad threshold"}), 400
    if not symbol or kind not in ("above", "below") or threshold <= 0:
        return jsonify({"error": "symbol, kind (above|below) and threshold required"}), 400
    with get_db() as conn:
        conn.execute("INSERT INTO price_alerts(symbol,kind,threshold,active,created_at) VALUES(?,?,?,1,?)",
                     (symbol, kind, threshold, int(time.time())))
        conn.commit()
    schedule_backup()
    return jsonify({"alerts": all_alerts()})

@app.route("/api/alerts/<int:aid>", methods=["DELETE"])
@require_auth_api
def api_alert_delete(aid):
    with get_db() as conn:
        conn.execute("DELETE FROM price_alerts WHERE id=?", (aid,))
        conn.commit()
    schedule_backup()
    return jsonify({"alerts": all_alerts()})

# ── backup / export / import ────────────────────────────────────────────────

@app.route("/api/backup")
@require_auth_api
def api_backup_status():
    return jsonify(backup_status())

@app.route("/api/backup/config", methods=["POST"])
@require_auth_api
def api_backup_config():
    token = ((request.get_json(silent=True) or {}).get("token") or "").strip()
    if not token:
        return jsonify({"error": "token required"}), 400
    set_setting("gist_token", token)
    set_setting("last_backup_error", None)
    if not backup_to_gist():
        set_setting("gist_token", None)
        err = get_setting("last_backup_error") or "backup failed"
        return jsonify({"error": "GitHub rejected the token: " + err}), 400
    return jsonify(backup_status())

@app.route("/api/backup/now", methods=["POST"])
@require_auth_api
def api_backup_now():
    backup_to_gist()
    return jsonify(backup_status())

@app.route("/api/backup/restore", methods=["POST"])
@require_auth_api
def api_backup_restore():
    try:
        restore_from_gist()
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(state_payload())

@app.route("/api/backup/disable", methods=["POST"])
@require_auth_api
def api_backup_disable():
    set_setting("gist_token", None)
    set_setting("gist_id", None)
    return jsonify(backup_status())

@app.route("/api/export")
@require_auth_api
def api_export():
    body = json.dumps(snapshot(), indent=1)
    fname = "portfolio-backup-%s.json" % datetime.now().strftime("%Y%m%d")
    return Response(body, mimetype="application/json",
                    headers={"Content-Disposition": "attachment; filename=" + fname})

@app.route("/api/import", methods=["POST"])
@require_auth_api
def api_import():
    try:
        restore_snapshot(request.get_json(force=True))
    except Exception as e:
        return jsonify({"error": "import failed: %s" % e}), 400
    schedule_backup()
    return jsonify(state_payload())

# ─────────────────────────────────────────────────────────────────────────────
# PWA ASSETS (manifest, service worker, generated icons)
# ─────────────────────────────────────────────────────────────────────────────

_icon_cache = {}

def make_icon_png(size):
    """Rounded dark tile with a rising green sparkline — pure stdlib PNG."""
    if size in _icon_cache:
        return _icon_cache[size]
    pts = [(0.14, 0.72), (0.30, 0.52), (0.44, 0.61), (0.60, 0.36), (0.74, 0.46), (0.88, 0.22)]
    lw, radius = 0.05, 0.22

    def seg_dist(px, py, a, b):
        ax, ay = a
        bx, by = b
        dx, dy = bx - ax, by - ay
        l2 = dx * dx + dy * dy
        t = 0.0 if l2 == 0 else max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / l2))
        cx, cy = ax + t * dx, ay + t * dy
        return math.hypot(px - cx, py - cy)

    rows = []
    for j in range(size):
        v = (j + 0.5) / size
        row = bytearray([0])  # filter byte
        for i in range(size):
            u = (i + 0.5) / size
            eu, ev = min(u, 1 - u), min(v, 1 - v)
            if eu < radius and ev < radius:
                alpha = 255 if math.hypot(eu - radius, ev - radius) <= radius else 0
            else:
                alpha = 255
            base = (13 + int(8 * v), 17 + int(11 * v), 24 + int(18 * v))
            d = min(seg_dist(u, v, pts[k], pts[k + 1]) for k in range(len(pts) - 1))
            if d < lw:
                col = (52, 211, 153)
            elif d < lw * 1.7:
                t = (d - lw) / (lw * 0.7)
                col = tuple(int(g * (1 - t) + b * t) for g, b in zip((52, 211, 153), base))
            else:
                col = base
            row += bytes((col[0], col[1], col[2], alpha))
        rows.append(bytes(row))

    def chunk(tag, data):
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

    ihdr = struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)
    png = (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr)
           + chunk(b"IDAT", zlib.compress(b"".join(rows), 9)) + chunk(b"IEND", b""))
    _icon_cache[size] = png
    return png

@app.route("/icon-<int:size>.png")
def icon(size):
    if size not in (180, 192, 512):
        size = 192
    return Response(make_icon_png(size), mimetype="image/png",
                    headers={"Cache-Control": "public, max-age=86400"})

@app.route("/apple-touch-icon.png")
@app.route("/favicon.ico")
def apple_icon():
    return Response(make_icon_png(180), mimetype="image/png",
                    headers={"Cache-Control": "public, max-age=86400"})

@app.route("/manifest.webmanifest")
def manifest():
    return Response(json.dumps({
        "name": "Portfolio — Tripwire", "short_name": "Portfolio",
        "start_url": "/", "scope": "/", "display": "standalone",
        "background_color": "#0b0f14", "theme_color": "#0b0f14",
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png"},
        ],
    }), mimetype="application/manifest+json")

@app.route("/sw.js")
def service_worker():
    return Response(SW_JS, mimetype="application/javascript",
                    headers={"Cache-Control": "no-cache"})

@app.route("/")
def index():
    if not is_authed():
        return redirect("/login")
    return Response(INDEX_HTML, mimetype="text/html")

# ─────────────────────────────────────────────────────────────────────────────
# STATIC CONTENT
# ─────────────────────────────────────────────────────────────────────────────

SW_JS = """
const V = 'portfolio-v1';
const SHELL = ['/manifest.webmanifest', '/icon-192.png', '/icon-512.png'];
self.addEventListener('install', e => {
  e.waitUntil(caches.open(V).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys()
    .then(ks => Promise.all(ks.filter(k => k !== V).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const u = new URL(e.request.url);
  if (u.pathname.startsWith('/api/') || u.pathname === '/login' || u.pathname === '/logout') return;
  if (e.request.mode === 'navigate') {
    e.respondWith(fetch(e.request).then(r => {
      const copy = r.clone();
      if (r.ok) caches.open(V).then(c => c.put('/', copy));
      return r;
    }).catch(() => caches.match('/')));
    return;
  }
  e.respondWith(caches.match(e.request).then(r => r || fetch(e.request).then(resp => {
    const copy = resp.clone();
    if (resp.ok) caches.open(V).then(c => c.put(e.request, copy));
    return resp;
  })));
});
"""

LOGIN_HTML = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0b0f14">
<title>Portfolio — Sign in</title>
<link rel="manifest" href="/manifest.webmanifest">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<style>
:root{color-scheme:dark}
*{box-sizing:border-box;margin:0}
body{background:#0b0f14;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
     min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}
.card{width:100%;max-width:340px;text-align:center}
.logo{width:76px;height:76px;border-radius:18px;margin:0 auto 18px;display:block}
h1{font-size:22px;margin-bottom:4px}
p{color:#8b98a5;font-size:14px;margin-bottom:22px}
input{width:100%;padding:14px 16px;border-radius:12px;border:1px solid #263041;background:#151a22;
      color:#e6edf3;font-size:16px;margin-bottom:12px;outline:none}
input:focus{border-color:#34d399}
button{width:100%;padding:14px;border-radius:12px;border:0;background:#34d399;color:#04110b;
       font-size:16px;font-weight:700;cursor:pointer}
.err{background:#3a1116;border:1px solid #7f1d1d;color:#fca5a5;border-radius:10px;
     padding:10px;font-size:13px;margin-bottom:12px}
</style></head><body>
<form class="card" method="post" action="/login">
  <img class="logo" src="/icon-192.png" alt="">
  <h1>Portfolio</h1>
  <p>Stock tracker &middot; live Yahoo Finance quotes</p>
  <!--ERR-->
  <input type="password" name="password" placeholder="Password" autofocus
         autocomplete="current-password" enterkeyhint="go">
  <button type="submit">Sign in</button>
</form>
</body></html>"""

INDEX_HTML = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover, user-scalable=no">
<meta name="theme-color" content="#0b0f14">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Portfolio">
<title>Portfolio</title>
<link rel="manifest" href="/manifest.webmanifest">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<link rel="icon" href="/icon-192.png">
<style>
:root{
  color-scheme:dark;
  --bg:#0b0f14; --card:#151a22; --card2:#1b2230; --line:#232c3b;
  --text:#e6edf3; --dim:#8b98a5; --dim2:#5c6875;
  --green:#22c55e; --green-soft:#12351f; --red:#ef4444; --red-soft:#3a1518;
  --amber:#f59e0b; --accent:#34d399;
  --sat:env(safe-area-inset-top,0px); --sab:env(safe-area-inset-bottom,0px);
}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{height:100%}
body{background:var(--bg);color:var(--text);
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
  overscroll-behavior-y:contain;-webkit-font-smoothing:antialiased;
  user-select:none;-webkit-user-select:none}
button{font-family:inherit;color:inherit;background:none;border:0;cursor:pointer}
input{font-family:inherit}
#appwrap{max-width:560px;margin:0 auto;padding-bottom:calc(64px + var(--sab))}

/* header */
header{position:sticky;top:0;z-index:20;background:rgba(11,15,20,.86);
  backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
  padding:calc(var(--sat) + 10px) 16px 8px;border-bottom:1px solid var(--line)}
.hrow{display:flex;align-items:center;gap:10px}
.hrow h1{font-size:20px;font-weight:800;letter-spacing:-.3px;flex:1}
.hbtn{width:34px;height:34px;border-radius:50%;background:var(--card);display:flex;
  align-items:center;justify-content:center;font-size:17px;color:var(--dim);flex-shrink:0}
.hbtn:active{background:var(--card2)}
#mktdot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--dim2);margin-right:6px}
#mktdot.open{background:var(--green)} #mktdot.pre,#mktdot.post{background:var(--amber)}
#mktlabel{font-size:12px;color:var(--dim)}

/* portfolio tabs */
#pftabs{display:flex;gap:8px;overflow-x:auto;padding:10px 0 4px;scrollbar-width:none}
#pftabs::-webkit-scrollbar{display:none}
.pftab{flex-shrink:0;padding:7px 14px;border-radius:18px;background:var(--card);
  font-size:13.5px;font-weight:600;color:var(--dim);white-space:nowrap}
.pftab.on{background:var(--accent);color:#04110b}

/* summary card */
#summary{margin:12px 16px;background:linear-gradient(150deg,#161d29,#121722);
  border:1px solid var(--line);border-radius:16px;padding:16px}
#summary .lbl{font-size:12px;color:var(--dim);text-transform:uppercase;letter-spacing:.6px}
#totval{font-size:32px;font-weight:800;letter-spacing:-.5px;margin:2px 0 10px;font-variant-numeric:tabular-nums}
.sumrow{display:flex;gap:12px}
.sumcell{flex:1;background:rgba(255,255,255,.03);border-radius:10px;padding:8px 10px}
.sumcell .k{font-size:11px;color:var(--dim)}
.sumcell .v{font-size:14.5px;font-weight:700;margin-top:2px;font-variant-numeric:tabular-nums}

/* indices strip */
#indices{display:flex;gap:8px;overflow-x:auto;padding:2px 16px 10px;scrollbar-width:none}
#indices::-webkit-scrollbar{display:none}
.idx{flex-shrink:0;background:var(--card);border:1px solid var(--line);border-radius:12px;
  padding:8px 12px;min-width:118px}
.idx .n{font-size:11.5px;color:var(--dim);font-weight:600}
.idx .p{font-size:14px;font-weight:700;margin-top:2px;font-variant-numeric:tabular-nums}
.idx .c{font-size:12px;font-weight:600;font-variant-numeric:tabular-nums}

/* list */
#list{margin:0 16px}
.lhead{display:flex;align-items:center;justify-content:space-between;padding:6px 2px}
.lhead .t{font-size:13px;color:var(--dim);font-weight:600}
.lhead button{font-size:13px;color:var(--accent);font-weight:600;padding:4px 8px}
.row{display:flex;align-items:center;gap:10px;padding:12px 2px;border-bottom:1px solid var(--line)}
.row:active{background:rgba(255,255,255,.02)}
.rdel{width:24px;height:24px;border-radius:50%;background:var(--red);color:#fff;font-size:15px;
  font-weight:800;display:none;align-items:center;justify-content:center;flex-shrink:0}
body.editing .rdel{display:flex}
.rleft{flex:1;min-width:0}
.rsym{font-size:16.5px;font-weight:800;letter-spacing:-.2px}
.rname{font-size:12px;color:var(--dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:180px}
.rpos{font-size:11.5px;color:var(--dim2);margin-top:1px;font-variant-numeric:tabular-nums}
.rright{text-align:right;flex-shrink:0}
.rprice{font-size:16px;font-weight:700;font-variant-numeric:tabular-nums;margin-bottom:3px}
.pill{display:inline-block;min-width:86px;text-align:center;padding:5px 8px;border-radius:8px;
  font-size:14px;font-weight:700;color:#fff;font-variant-numeric:tabular-nums;background:var(--dim2)}
.pill.up{background:var(--green)} .pill.dn{background:var(--red)}
.rext{font-size:11px;color:var(--amber);margin-top:3px;font-variant-numeric:tabular-nums}
@keyframes flashUp{0%{background:var(--green-soft)}100%{background:transparent}}
@keyframes flashDn{0%{background:var(--red-soft)}100%{background:transparent}}
.row.fup{animation:flashUp 1s} .row.fdn{animation:flashDn 1s}
#empty{padding:44px 20px;text-align:center;color:var(--dim)}
#empty .big{font-size:38px;margin-bottom:10px}
#empty button{margin-top:14px;background:var(--accent);color:#04110b;font-weight:700;
  padding:11px 22px;border-radius:12px;font-size:15px}

/* footer status + fab */
#status{position:fixed;bottom:calc(10px + var(--sab));left:0;right:0;text-align:center;
  font-size:11px;color:var(--dim2);pointer-events:none;z-index:5}
#fab{position:fixed;right:18px;bottom:calc(26px + var(--sab));width:54px;height:54px;border-radius:50%;
  background:var(--accent);color:#04110b;font-size:28px;font-weight:700;box-shadow:0 6px 20px rgba(52,211,153,.35);
  display:flex;align-items:center;justify-content:center;z-index:15}
#fab:active{transform:scale(.94)}

/* sheets */
.sheet{position:fixed;inset:0;z-index:40;visibility:hidden}
.sheet.open{visibility:visible}
.sheet .back{position:absolute;inset:0;background:rgba(0,0,0,.55);opacity:0;transition:opacity .25s}
.sheet.open .back{opacity:1}
.sheet .panel{position:absolute;left:0;right:0;bottom:0;max-height:calc(100% - var(--sat) - 10px);
  background:#10151d;border-radius:18px 18px 0 0;transform:translateY(100%);
  transition:transform .28s cubic-bezier(.32,.72,.35,1);display:flex;flex-direction:column;
  max-width:560px;margin:0 auto}
.sheet.open .panel{transform:translateY(0)}
.grab{width:38px;height:4px;border-radius:2px;background:#2c3648;margin:9px auto 2px;flex-shrink:0}
.sheet .body{overflow-y:auto;padding:8px 18px calc(24px + var(--sab));-webkit-overflow-scrolling:touch}
.shead{display:flex;align-items:center;gap:10px;padding:6px 18px 4px}
.shead .t1{font-size:18px;font-weight:800;flex:1}
.xbtn{width:30px;height:30px;border-radius:50%;background:var(--card2);color:var(--dim);
  font-size:15px;display:flex;align-items:center;justify-content:center}

/* detail sheet */
#d_name{font-size:13px;color:var(--dim);margin-top:-2px}
#d_price{font-size:34px;font-weight:800;letter-spacing:-.5px;font-variant-numeric:tabular-nums;margin-top:8px}
#d_chg{font-size:15.5px;font-weight:700;margin-top:2px;font-variant-numeric:tabular-nums}
#d_ext{font-size:12.5px;color:var(--amber);margin-top:4px;font-variant-numeric:tabular-nums}
.up-t{color:var(--green)} .dn-t{color:var(--red)}
#chartbox{position:relative;margin-top:14px}
#chart{width:100%;height:190px;display:block;touch-action:pan-y}
#scrub{position:absolute;top:0;left:0;background:var(--card2);border:1px solid var(--line);
  border-radius:8px;padding:4px 8px;font-size:11.5px;display:none;pointer-events:none;
  font-variant-numeric:tabular-nums;white-space:nowrap}
#ranges{display:flex;gap:4px;margin-top:10px}
#ranges button{flex:1;padding:7px 0;border-radius:8px;font-size:12.5px;font-weight:700;color:var(--dim)}
#ranges button.on{background:var(--card2);color:var(--accent)}
.card-s{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px;margin-top:14px}
.card-s h3{font-size:13px;color:var(--dim);font-weight:700;text-transform:uppercase;
  letter-spacing:.5px;margin-bottom:10px}
#statgrid{display:grid;grid-template-columns:1fr 1fr;gap:9px 18px}
.stat{display:flex;justify-content:space-between;font-size:13.5px;border-bottom:1px solid var(--line);padding-bottom:6px}
.stat .k{color:var(--dim)} .stat .v{font-weight:600;font-variant-numeric:tabular-nums}
.posgrid{display:flex;gap:10px;margin-bottom:12px}
.posgrid label{flex:1;font-size:11.5px;color:var(--dim)}
.posgrid input{width:100%;margin-top:4px;padding:10px;border-radius:10px;border:1px solid var(--line);
  background:var(--card2);color:var(--text);font-size:15px;outline:none;font-variant-numeric:tabular-nums}
.posgrid input:focus{border-color:var(--accent)}
.btnrow{display:flex;gap:10px}
.btn{flex:1;padding:11px;border-radius:11px;font-size:14px;font-weight:700;text-align:center}
.btn.primary{background:var(--accent);color:#04110b}
.btn.ghost{background:var(--card2);color:var(--dim)}
.btn.danger{background:var(--red-soft);color:#fca5a5}
#posmeta{display:flex;gap:10px;margin-top:12px}
.alertrow{display:flex;align-items:center;gap:10px;padding:9px 0;border-bottom:1px solid var(--line);font-size:14px}
.alertrow .k{flex:1}
.alertrow .del{color:var(--red);font-size:13px;font-weight:700;padding:4px 6px}
.newsrow{display:block;padding:11px 0;border-bottom:1px solid var(--line);color:inherit;text-decoration:none}
.newsrow .nt{font-size:14px;font-weight:600;line-height:1.35}
.newsrow .nm{font-size:11.5px;color:var(--dim2);margin-top:4px}

/* add sheet */
#q{width:100%;padding:12px 14px;border-radius:12px;border:1px solid var(--line);background:var(--card);
  color:var(--text);font-size:16px;outline:none;margin-bottom:8px}
#q:focus{border-color:var(--accent)}
.srow{display:flex;align-items:center;padding:12px 2px;border-bottom:1px solid var(--line)}
.srow .sl{flex:1;min-width:0}
.srow .ss{font-size:15.5px;font-weight:800}
.srow .sn{font-size:12px;color:var(--dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.srow .sx{font-size:11px;color:var(--dim2);flex-shrink:0;margin-left:8px}
.addform{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px;margin:10px 0}
.addform .ttl{font-size:14px;font-weight:700;margin-bottom:10px}

/* settings */
.setrow{display:flex;align-items:center;gap:10px;padding:13px 2px;border-bottom:1px solid var(--line);font-size:15px}
.setrow .k{flex:1}
.setrow select{background:var(--card2);color:var(--text);border:1px solid var(--line);
  border-radius:8px;padding:6px 10px;font-size:14px}
.tokinput{width:100%;padding:11px;border-radius:10px;border:1px solid var(--line);
  background:var(--card2);color:var(--text);font-size:14px;outline:none;margin-bottom:10px}
.tokinput:focus{border-color:var(--accent)}
.bknote{font-size:12.5px;color:var(--dim);line-height:1.45;margin-bottom:10px}

/* toast */
#toast{position:fixed;top:calc(var(--sat) + 14px);left:50%;transform:translate(-50%,-140%);
  background:var(--card2);border:1px solid var(--line);color:var(--text);padding:12px 18px;
  border-radius:14px;font-size:14px;font-weight:600;z-index:60;transition:transform .3s;
  max-width:88%;box-shadow:0 8px 30px rgba(0,0,0,.5)}
#toast.show{transform:translate(-50%,0)}
.spin{display:inline-block;width:14px;height:14px;border:2px solid var(--dim2);border-top-color:var(--accent);
  border-radius:50%;animation:sp 1s linear infinite;vertical-align:-2px}
@keyframes sp{to{transform:rotate(360deg)}}
.muted{color:var(--dim);font-size:13px;text-align:center;padding:14px 0}
</style></head>
<body>
<div id="appwrap">
  <header>
    <div class="hrow">
      <h1>Portfolio</h1>
      <span><span id="mktdot"></span><span id="mktlabel"></span></span>
      <button class="hbtn" id="sortbtn" title="Sort">&#8645;</button>
      <button class="hbtn" id="setbtn" title="Settings">&#9881;</button>
    </div>
    <div id="pftabs"></div>
  </header>

  <div id="summary" style="display:none">
    <div class="lbl">Total value</div>
    <div id="totval">—</div>
    <div class="sumrow">
      <div class="sumcell"><div class="k">Day P/L</div><div class="v" id="daypl">—</div></div>
      <div class="sumcell"><div class="k">Total P/L</div><div class="v" id="totpl">—</div></div>
      <div class="sumcell"><div class="k">Cost basis</div><div class="v" id="costb">—</div></div>
    </div>
  </div>

  <div id="indices"></div>

  <div id="list">
    <div class="lhead">
      <span class="t" id="lcount"></span>
      <button id="editbtn">Edit</button>
    </div>
    <div id="rows"></div>
    <div id="empty" style="display:none">
      <div class="big">&#128200;</div>
      <div>No symbols yet.<br>Add stocks, ETFs, crypto or indices to track.</div>
      <button onclick="openAdd()">Add symbol</button>
    </div>
  </div>
</div>

<div id="status"></div>
<button id="fab" onclick="openAdd()">+</button>
<div id="toast"></div>

<!-- detail sheet -->
<div class="sheet" id="detail">
  <div class="back" onclick="closeSheet('detail')"></div>
  <div class="panel">
    <div class="grab"></div>
    <div class="shead">
      <div style="flex:1">
        <div class="t1" id="d_sym"></div>
        <div id="d_name"></div>
      </div>
      <button class="xbtn" onclick="closeSheet('detail')">&#10005;</button>
    </div>
    <div class="body">
      <div id="d_price">—</div>
      <div id="d_chg"></div>
      <div id="d_ext"></div>
      <div id="chartbox">
        <canvas id="chart"></canvas>
        <div id="scrub"></div>
      </div>
      <div id="ranges"></div>
      <div class="card-s" id="poscard">
        <h3>Your position</h3>
        <div class="posgrid">
          <label>Shares<input id="p_shares" type="text" inputmode="decimal" placeholder="0"></label>
          <label>Avg cost / share<input id="p_cost" type="text" inputmode="decimal" placeholder="0.00"></label>
        </div>
        <div class="btnrow"><button class="btn primary" id="p_save">Save position</button></div>
        <div id="posmeta"></div>
      </div>
      <div class="card-s">
        <h3>Price alerts</h3>
        <div id="alertlist"></div>
        <div class="posgrid" style="margin-top:10px">
          <label>Alert when price
            <select id="a_kind" style="width:100%;margin-top:4px;padding:10px;border-radius:10px;
              border:1px solid var(--line);background:var(--card2);color:var(--text);font-size:15px">
              <option value="above">rises above</option>
              <option value="below">falls below</option>
            </select>
          </label>
          <label>Price<input id="a_thr" type="text" inputmode="decimal" placeholder="0.00"></label>
        </div>
        <div class="btnrow"><button class="btn ghost" id="a_add">Add alert</button></div>
      </div>
      <div class="card-s">
        <h3>Statistics</h3>
        <div id="statgrid"></div>
      </div>
      <div class="card-s">
        <h3>News</h3>
        <div id="newslist"><div class="muted"><span class="spin"></span></div></div>
      </div>
    </div>
  </div>
</div>

<!-- add sheet -->
<div class="sheet" id="add">
  <div class="back" onclick="closeSheet('add')"></div>
  <div class="panel">
    <div class="grab"></div>
    <div class="shead"><div class="t1">Add symbol</div>
      <button class="xbtn" onclick="closeSheet('add')">&#10005;</button></div>
    <div class="body" style="min-height:340px">
      <input id="q" type="search" placeholder="Search ticker or company&hellip;"
             autocomplete="off" autocapitalize="characters" enterkeyhint="search">
      <div id="results"></div>
    </div>
  </div>
</div>

<!-- settings sheet -->
<div class="sheet" id="settings">
  <div class="back" onclick="closeSheet('settings')"></div>
  <div class="panel">
    <div class="grab"></div>
    <div class="shead"><div class="t1">Settings</div>
      <button class="xbtn" onclick="closeSheet('settings')">&#10005;</button></div>
    <div class="body">
      <div class="setrow"><span class="k">Refresh interval</span>
        <select id="s_interval">
          <option value="5000">5 s</option><option value="10000">10 s</option>
          <option value="30000">30 s</option><option value="60000">60 s</option>
        </select></div>
      <div class="setrow"><span class="k">Rename this portfolio</span>
        <button class="btn ghost" style="flex:none;padding:8px 14px" onclick="renamePf()">Rename</button></div>
      <div class="setrow"><span class="k">Delete this portfolio</span>
        <button class="btn danger" style="flex:none;padding:8px 14px" onclick="deletePf()">Delete</button></div>
      <div class="setrow"><span class="k">Notifications for alerts</span>
        <button class="btn ghost" style="flex:none;padding:8px 14px" onclick="askNotify()">Enable</button></div>
      <div class="setrow"><span class="k">Sign out</span>
        <button class="btn ghost" style="flex:none;padding:8px 14px" onclick="location.href='/logout'">Log out</button></div>

      <div class="card-s">
        <h3>Cloud backup</h3>
        <div id="bk_status" class="bknote"></div>
        <div id="bk_setup">
          <input id="bk_token" class="tokinput" type="password" autocomplete="off"
                 placeholder="GitHub token with &quot;gist&quot; scope">
          <div class="btnrow"><button class="btn primary" id="bk_save">Enable auto-backup</button></div>
        </div>
        <div id="bk_actions" style="display:none">
          <div class="btnrow">
            <button class="btn ghost" id="bk_now">Back up now</button>
            <button class="btn ghost" id="bk_restore">Restore</button>
            <button class="btn danger" id="bk_off">Disable</button>
          </div>
        </div>
        <div class="btnrow" style="margin-top:10px">
          <button class="btn ghost" onclick="location.href='/api/export'">Export file</button>
          <button class="btn ghost" id="bk_import">Import file</button>
        </div>
        <input type="file" id="bk_file" accept=".json,application/json" style="display:none">
      </div>
      <div class="muted" id="aboutline"></div>
    </div>
  </div>
</div>

<script>
'use strict';
const $ = id => document.getElementById(id);
const S = {
  state: null, quotes: {}, indices: [], market: 'closed',
  pf: +(localStorage.pf || 0), mode: +(localStorage.mode || 0),
  sort: localStorage.sort || 'manual', editing: false,
  detailSym: null, detailPos: null, range: localStorage.range || '1d',
  chart: null, interval: +(localStorage.interval || 10000), timer: null, lastTs: 0,
};
const MODES = ['pct', 'chg', 'val'];  // pill display cycles like My Stocks
const RANGES = ['1d','5d','1mo','6mo','ytd','1y','5y','max'];
const RANGE_LABELS = {'1d':'1D','5d':'5D','1mo':'1M','6mo':'6M','ytd':'YTD','1y':'1Y','5y':'5Y','max':'MAX'};

function fmtMoney(n, cur, digits) {
  if (n == null || isNaN(n)) return '—';
  try {
    return new Intl.NumberFormat(undefined, {style:'currency', currency:cur||'USD',
      minimumFractionDigits:digits??2, maximumFractionDigits:digits??2}).format(n);
  } catch(e) { return (cur||'$') + n.toFixed(2); }
}
function fmtNum(n) {
  if (n == null || isNaN(n)) return '—';
  const a = Math.abs(n);
  if (a >= 1e12) return (n/1e12).toFixed(2)+'T';
  if (a >= 1e9)  return (n/1e9).toFixed(2)+'B';
  if (a >= 1e6)  return (n/1e6).toFixed(2)+'M';
  if (a >= 1e3)  return (n/1e3).toFixed(1)+'K';
  return String(n);
}
function fmtPrice(n) {
  if (n == null || isNaN(n)) return '—';
  return n.toLocaleString(undefined, {minimumFractionDigits: n < 5 ? 3 : 2,
                                      maximumFractionDigits: n < 5 ? 4 : 2});
}
function sign(n){ return n > 0 ? '+' : ''; }
function ago(ts){
  if (!ts) return '';
  const s = Math.max(0, Date.now()/1000 - ts);
  if (s < 90) return 'just now';
  if (s < 3600) return Math.round(s/60) + 'm ago';
  if (s < 86400) return Math.round(s/3600) + 'h ago';
  return Math.round(s/86400) + 'd ago';
}
function esc(s){ return (s||'').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

async function api(path, opts) {
  const r = await fetch(path, Object.assign({headers:{'Content-Type':'application/json'}}, opts));
  if (r.status === 401) { location.href = '/login'; throw new Error('auth'); }
  const d = await r.json();
  if (!r.ok) throw new Error(d.error || r.statusText);
  return d;
}

function toast(msg, ms) {
  const t = $('toast');
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(t._h);
  t._h = setTimeout(() => t.classList.remove('show'), ms || 2600);
}

/* ── state / rendering ─────────────────────────────────────────── */

function curPf() {
  if (!S.state) return null;
  return S.state.portfolios.find(p => p.id === S.pf) || S.state.portfolios[0];
}

function renderTabs() {
  const pf = curPf();
  $('pftabs').innerHTML = S.state.portfolios.map(p =>
    `<button class="pftab ${pf && p.id===pf.id ? 'on':''}" data-id="${p.id}">${esc(p.name)}</button>`
  ).join('') + `<button class="pftab" data-id="new">+ New</button>`;
  $('pftabs').querySelectorAll('.pftab').forEach(b => b.onclick = async () => {
    if (b.dataset.id === 'new') {
      const name = prompt('New portfolio name:');
      if (!name) return;
      S.state = await api('/api/portfolios', {method:'POST', body:JSON.stringify({name})});
      S.pf = S.state.portfolios[S.state.portfolios.length-1].id;
      localStorage.pf = S.pf;
      renderAll(); refresh();
    } else {
      S.pf = +b.dataset.id; localStorage.pf = S.pf;
      renderAll(); refresh();
    }
  });
}

function positionsSorted() {
  const pf = curPf();
  if (!pf) return [];
  const arr = [...pf.positions];
  const q = s => S.quotes[s] || {};
  if (S.sort === 'symbol') arr.sort((a,b) => a.symbol.localeCompare(b.symbol));
  else if (S.sort === 'pct') arr.sort((a,b) => (q(b.symbol).change_pct??-1e9) - (q(a.symbol).change_pct??-1e9));
  else if (S.sort === 'value') arr.sort((a,b) =>
    (b.shares*(q(b.symbol).price||0)) - (a.shares*(q(a.symbol).price||0)));
  return arr;
}

function pillHTML(p, q) {
  if (q.error) return `<span class="pill">…</span>`;
  const up = (q.change ?? 0) >= 0;
  const cls = q.change == null ? '' : (up ? 'up' : 'dn');
  let txt;
  const mode = MODES[S.mode];
  if (mode === 'chg') txt = q.change == null ? '—' : sign(q.change) + q.change.toFixed(2);
  else if (mode === 'val' && p.shares > 0) txt = fmtNum(p.shares * q.price);
  else if (mode === 'val') txt = q.market_cap ? fmtNum(q.market_cap) : (q.change_pct==null?'—':sign(q.change_pct)+q.change_pct.toFixed(2)+'%');
  else txt = q.change_pct == null ? '—' : sign(q.change_pct) + q.change_pct.toFixed(2) + '%';
  return `<span class="pill ${cls}">${txt}</span>`;
}

function extHTML(q, cls) {
  const st = S.market;
  if (st !== 'pre' && q.post_price && q.post_change_pct != null)
    return `<div class="${cls}">After hours ${fmtPrice(q.post_price)} (${sign(q.post_change_pct)}${q.post_change_pct.toFixed(2)}%)</div>`;
  if (st === 'pre' && q.pre_price && q.pre_change_pct != null)
    return `<div class="${cls}">Pre-market ${fmtPrice(q.pre_price)} (${sign(q.pre_change_pct)}${q.pre_change_pct.toFixed(2)}%)</div>`;
  return '';
}

function renderList() {
  const pf = curPf();
  if (!pf) return;
  const arr = positionsSorted();
  $('lcount').textContent = arr.length ? `${arr.length} symbol${arr.length>1?'s':''}` : '';
  $('empty').style.display = arr.length ? 'none' : 'block';
  $('editbtn').style.display = arr.length ? '' : 'none';
  const old = {};
  $('rows').querySelectorAll('.row').forEach(r => old[r.dataset.sym] = +r.dataset.price || 0);
  $('rows').innerHTML = arr.map(p => {
    const q = S.quotes[p.symbol] || {};
    const holding = p.shares > 0;
    const posline = holding
      ? `<div class="rpos">${p.shares.toLocaleString()} sh · avg ${fmtPrice(p.cost)} · ${fmtMoney(p.shares*(q.price||0), q.currency)}</div>` : '';
    return `<div class="row" data-sym="${p.symbol}" data-id="${p.id}" data-price="${q.price||''}">
      <button class="rdel" data-del="${p.id}">−</button>
      <div class="rleft">
        <div class="rsym">${esc(p.symbol)}</div>
        <div class="rname">${esc(p.name || q.name || '')}</div>
        ${posline}
      </div>
      <div class="rright">
        <div class="rprice">${q.error ? '<span class="spin"></span>' : fmtPrice(q.price)}</div>
        ${pillHTML(p, q)}
        ${extHTML(q, 'rext')}
      </div>
    </div>`;
  }).join('');
  $('rows').querySelectorAll('.row').forEach(r => {
    const sym = r.dataset.sym, id = +r.dataset.id;
    const prev = old[sym];
    const now = +r.dataset.price || 0;
    if (prev && now && prev !== now) r.classList.add(now > prev ? 'fup' : 'fdn');
    r.onclick = e => {
      if (e.target.closest('.rdel')) { delPosition(id, sym); return; }
      if (e.target.closest('.pill')) { S.mode = (S.mode+1) % MODES.length; localStorage.mode = S.mode; renderList(); return; }
      openDetail(sym, id);
    };
  });
}

function renderSummary() {
  const pf = curPf();
  if (!pf) return;
  const holdings = pf.positions.filter(p => p.shares > 0);
  if (!holdings.length) { $('summary').style.display = 'none'; return; }
  let val = 0, cost = 0, day = 0, priced = 0;
  let cur = 'USD';
  for (const p of holdings) {
    const q = S.quotes[p.symbol];
    cost += p.shares * p.cost;
    if (q && q.price != null) {
      val += p.shares * q.price;
      if (q.prev_close) day += p.shares * (q.price - q.prev_close);
      cur = q.currency || cur;
      priced++;
    } else {
      val += p.shares * p.cost;  // placeholder until quote arrives
    }
  }
  $('summary').style.display = '';
  $('totval').textContent = priced ? fmtMoney(val, cur) : '…';
  const tot = val - cost;
  const set = (id, v, pctBase) => {
    const el = $(id);
    const pct = pctBase ? ` (${sign(v)}${(v/pctBase*100).toFixed(2)}%)` : '';
    el.textContent = `${sign(v)}${fmtMoney(v, cur)}${pct}`;
    el.style.color = v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--dim)';
  };
  set('daypl', day, val - day || 0);
  set('totpl', tot, cost || 0);
  $('costb').textContent = fmtMoney(cost, cur);
}

function renderIndices() {
  $('indices').innerHTML = S.indices.map(q => {
    if (q.error) return '';
    const up = (q.change_pct ?? 0) >= 0;
    return `<div class="idx">
      <div class="n">${esc(q.label)}</div>
      <div class="p">${fmtPrice(q.price)}</div>
      <div class="c" style="color:${up?'var(--green)':'var(--red)'}">${sign(q.change)}${(q.change??0).toFixed(2)} (${sign(q.change_pct)}${(q.change_pct??0).toFixed(2)}%)</div>
    </div>`;
  }).join('');
}

function renderMarket() {
  const labels = {open:'Market open', pre:'Pre-market', post:'After hours', closed:'Market closed'};
  $('mktdot').className = '';
  $('mktdot').classList.add(S.market);
  $('mktlabel').textContent = labels[S.market] || '';
}

function renderAll() { renderTabs(); renderSummary(); renderIndices(); renderList(); renderMarket(); }

/* ── data flow ─────────────────────────────────────────────────── */

async function loadState() {
  S.state = await api('/api/state');
  if (!curPf()) S.pf = S.state.portfolios[0].id;
  if (S.state.demo) $('aboutline').textContent = 'DEMO MODE — simulated quotes';
  renderAll();
}

let refreshing = false;
async function refresh() {
  const pf = curPf();
  if (!pf || refreshing) return;
  refreshing = true;
  try {
    const extra = S.detailSym ? '&symbols=' + encodeURIComponent(S.detailSym) : '';
    const d = await api(`/api/quotes?portfolio=${pf.id}${extra}`);
    Object.assign(S.quotes, d.quotes);
    S.indices = d.indices;
    S.market = d.market;
    S.lastTs = d.ts;
    renderSummary(); renderIndices(); renderList(); renderMarket();
    if (S.detailSym) updateDetailPrice();
    for (const f of d.fired || []) fireAlert(f);
    $('status').textContent = 'Updated ' + new Date(d.ts*1000).toLocaleTimeString();
  } catch(e) {
    if (e.message !== 'auth') $('status').textContent = 'Update failed — retrying…';
  } finally { refreshing = false; }
}

function schedule() {
  clearInterval(S.timer);
  S.timer = setInterval(() => { if (!document.hidden) refresh(); }, S.interval);
}

document.addEventListener('visibilitychange', () => { if (!document.hidden) { refresh(); } });

/* ── alerts ────────────────────────────────────────────────────── */

function fireAlert(f) {
  const dir = f.kind === 'above' ? 'rose above' : 'fell below';
  const msg = `${f.symbol} ${dir} ${fmtPrice(f.threshold)} — now ${fmtPrice(f.price)}`;
  toast('🔔 ' + msg, 6000);
  if ('Notification' in window && Notification.permission === 'granted') {
    try { new Notification('Price alert: ' + f.symbol, {body: msg, icon: '/icon-192.png'}); } catch(e){}
  }
  loadState().catch(()=>{});
}

function askNotify() {
  if (!('Notification' in window)) { toast('Notifications not supported here'); return; }
  Notification.requestPermission().then(p => toast(p === 'granted' ? 'Notifications enabled' : 'Notifications ' + p));
}

/* ── list actions ──────────────────────────────────────────────── */

async function delPosition(id, sym) {
  if (!confirm(`Remove ${sym} from this portfolio?`)) return;
  S.state = await api('/api/positions/' + id, {method:'DELETE'});
  renderAll();
}

$('editbtn').onclick = () => {
  S.editing = !S.editing;
  document.body.classList.toggle('editing', S.editing);
  $('editbtn').textContent = S.editing ? 'Done' : 'Edit';
};

$('sortbtn').onclick = () => {
  const order = ['manual','symbol','pct','value'];
  const labels = {manual:'Added order', symbol:'Symbol A→Z', pct:'% change', value:'Market value'};
  S.sort = order[(order.indexOf(S.sort)+1) % order.length];
  localStorage.sort = S.sort;
  toast('Sorted by: ' + labels[S.sort]);
  renderList();
};

$('setbtn').onclick = () => { $('s_interval').value = S.interval; openSheet('settings'); loadBackup(); };
$('s_interval').onchange = e => { S.interval = +e.target.value; localStorage.interval = S.interval; schedule(); };

async function renamePf() {
  const pf = curPf();
  const name = prompt('Portfolio name:', pf.name);
  if (!name) return;
  S.state = await api('/api/portfolios/' + pf.id, {method:'PATCH', body:JSON.stringify({name})});
  renderAll();
}
async function deletePf() {
  const pf = curPf();
  if (!confirm(`Delete portfolio "${pf.name}" and all its symbols?`)) return;
  try {
    S.state = await api('/api/portfolios/' + pf.id, {method:'DELETE'});
    S.pf = S.state.portfolios[0].id; localStorage.pf = S.pf;
    closeSheet('settings'); renderAll(); refresh();
  } catch(e) { toast(e.message); }
}

/* ── cloud backup ──────────────────────────────────────────────── */

function renderBackup(d) {
  $('bk_setup').style.display = d.configured ? 'none' : '';
  $('bk_actions').style.display = d.configured ? '' : 'none';
  if (d.configured) {
    let s = 'Auto-backup is ON — every change is saved to a private GitHub Gist';
    if (d.gist_id) s += ` (id ${d.gist_id.slice(0,10)}…)`;
    if (d.via_env) s += ', configured via env vars';
    s += '.';
    if (d.last_backup_ts) s += ` Last backup ${ago(d.last_backup_ts)}.`;
    if (d.last_error) s += ` ⚠️ Last error: ${d.last_error}`;
    $('bk_status').textContent = s;
  } else {
    $('bk_status').textContent = 'Back up portfolios, positions and alerts to a private GitHub Gist ' +
      'after every change. Create a token at github.com → Settings → Developer settings → ' +
      'Personal access tokens (classic) with only the "gist" scope, and paste it here.';
  }
}

async function loadBackup() {
  try { renderBackup(await api('/api/backup')); } catch(e) {}
}

$('bk_save').onclick = async () => {
  const token = $('bk_token').value.trim();
  if (!token) { toast('Paste a GitHub token first'); return; }
  $('bk_save').textContent = 'Checking…';
  try {
    renderBackup(await api('/api/backup/config', {method:'POST', body:JSON.stringify({token})}));
    $('bk_token').value = '';
    toast('Cloud backup enabled ✓');
  } catch(e) { toast(e.message, 5000); }
  $('bk_save').textContent = 'Enable auto-backup';
};

$('bk_now').onclick = async () => {
  const d = await api('/api/backup/now', {method:'POST'});
  renderBackup(d);
  toast(d.last_error ? 'Backup failed: ' + d.last_error : 'Backed up ✓');
};

$('bk_restore').onclick = async () => {
  if (!confirm('Replace everything on this device with the cloud backup?')) return;
  try {
    S.state = await api('/api/backup/restore', {method:'POST'});
    renderAll(); refresh();
    toast('Restored from cloud backup ✓');
  } catch(e) { toast(e.message, 5000); }
};

$('bk_off').onclick = async () => {
  if (!confirm('Disable cloud backup? (The Gist itself is not deleted.)')) return;
  renderBackup(await api('/api/backup/disable', {method:'POST'}));
};

$('bk_import').onclick = () => $('bk_file').click();
$('bk_file').onchange = async e => {
  const file = e.target.files[0];
  e.target.value = '';
  if (!file) return;
  if (!confirm(`Replace everything with the contents of "${file.name}"?`)) return;
  try {
    const text = await file.text();
    S.state = await api('/api/import', {method:'POST', body: text});
    renderAll(); refresh();
    toast('Imported ✓');
  } catch(err) { toast(err.message, 5000); }
};

/* ── sheets ────────────────────────────────────────────────────── */

function openSheet(id) { $(id).classList.add('open'); }
function closeSheet(id) {
  $(id).classList.remove('open');
  if (id === 'detail') S.detailSym = null;
}

/* ── add symbol ────────────────────────────────────────────────── */

function openAdd() {
  openSheet('add');
  $('results').innerHTML = '<div class="muted">Type a ticker (AAPL) or a company name.</div>';
  $('q').value = '';
  setTimeout(() => $('q').focus(), 320);
}

let qTimer = null;
$('q').addEventListener('input', () => {
  clearTimeout(qTimer);
  const v = $('q').value.trim();
  if (!v) { $('results').innerHTML = ''; return; }
  qTimer = setTimeout(() => doSearch(v), 300);
});

async function doSearch(v) {
  $('results').innerHTML = '<div class="muted"><span class="spin"></span></div>';
  try {
    const d = await api('/api/search?q=' + encodeURIComponent(v));
    if (!d.results.length) { $('results').innerHTML = '<div class="muted">No matches.</div>'; return; }
    $('results').innerHTML = d.results.map((r,i) => `
      <div class="srow" data-i="${i}">
        <div class="sl"><div class="ss">${esc(r.symbol)}</div><div class="sn">${esc(r.name)}</div></div>
        <div class="sx">${esc(r.exchange)}${r.type ? ' · ' + esc(r.type) : ''}</div>
      </div>`).join('');
    $('results').querySelectorAll('.srow').forEach(row => {
      row.onclick = () => showAddForm(d.results[+row.dataset.i], row);
    });
  } catch(e) { $('results').innerHTML = '<div class="muted">Search failed: ' + esc(e.message) + '</div>'; }
}

function showAddForm(r, row) {
  document.querySelectorAll('.addform').forEach(x => x.remove());
  const f = document.createElement('div');
  f.className = 'addform';
  f.innerHTML = `
    <div class="ttl">Add ${esc(r.symbol)} to "${esc(curPf().name)}"</div>
    <div class="posgrid">
      <label>Shares (optional)<input type="text" inputmode="decimal" placeholder="0" class="af-sh"></label>
      <label>Avg cost / share<input type="text" inputmode="decimal" placeholder="0.00" class="af-c"></label>
    </div>
    <div class="btnrow">
      <button class="btn ghost af-watch">Watch only</button>
      <button class="btn primary af-add">Add</button>
    </div>`;
  row.after(f);
  const add = async (watchOnly) => {
    const shares = watchOnly ? 0 : parseFloat(f.querySelector('.af-sh').value) || 0;
    const cost = watchOnly ? 0 : parseFloat(f.querySelector('.af-c').value) || 0;
    try {
      S.state = await api('/api/positions', {method:'POST', body:JSON.stringify(
        {portfolio_id: curPf().id, symbol: r.symbol, name: r.name, shares, cost})});
      closeSheet('add'); renderAll(); refresh();
      toast(`${r.symbol} added`);
    } catch(e) { toast(e.message); }
  };
  f.querySelector('.af-add').onclick = () => add(false);
  f.querySelector('.af-watch').onclick = () => add(true);
}

/* ── detail sheet ──────────────────────────────────────────────── */

function findPosition(sym) {
  const pf = curPf();
  return pf ? pf.positions.find(p => p.symbol === sym) : null;
}

async function openDetail(sym, posId) {
  S.detailSym = sym;
  S.detailPos = posId || (findPosition(sym) || {}).id || null;
  $('d_sym').textContent = sym;
  const p = findPosition(sym);
  $('d_name').textContent = (p && p.name) || (S.quotes[sym]||{}).name || '';
  $('p_shares').value = p && p.shares ? p.shares : '';
  $('p_cost').value = p && p.cost ? p.cost : '';
  $('statgrid').innerHTML = '<div class="muted"><span class="spin"></span></div>';
  $('newslist').innerHTML = '<div class="muted"><span class="spin"></span></div>';
  $('alertlist').innerHTML = '';
  renderRangeTabs();
  updateDetailPrice();
  renderAlerts();
  openSheet('detail');
  loadChart();
  try {
    const d = await api('/api/detail/' + encodeURIComponent(sym));
    if (S.detailSym !== sym) return;
    S.quotes[sym] = d.quote;
    updateDetailPrice();
    renderStats(d);
    renderNews(d.news);
  } catch(e) {
    $('statgrid').innerHTML = '<div class="muted">' + esc(e.message) + '</div>';
    $('newslist').innerHTML = '';
  }
}

function updateDetailPrice() {
  const q = S.quotes[S.detailSym];
  if (!q || q.error) return;
  $('d_price').textContent = fmtPrice(q.price);
  const up = (q.change ?? 0) >= 0;
  $('d_chg').textContent = q.change == null ? '' :
    `${sign(q.change)}${q.change.toFixed(2)} (${sign(q.change_pct)}${q.change_pct.toFixed(2)}%) today`;
  $('d_chg').className = up ? 'up-t' : 'dn-t';
  $('d_ext').innerHTML = extHTML(q, '');
  const p = findPosition(S.detailSym);
  if (p && p.shares > 0 && q.price != null) {
    const val = p.shares * q.price, cost = p.shares * p.cost;
    const gain = val - cost;
    $('posmeta').innerHTML = `
      <div class="sumcell" style="flex:1"><div class="k">Market value</div><div class="v">${fmtMoney(val, q.currency)}</div></div>
      <div class="sumcell" style="flex:1"><div class="k">Gain/Loss</div>
        <div class="v" style="color:${gain>=0?'var(--green)':'var(--red)'}">${sign(gain)}${fmtMoney(gain, q.currency)}${cost ? ` (${sign(gain)}${(gain/cost*100).toFixed(1)}%)` : ''}</div></div>`;
  } else $('posmeta').innerHTML = '';
}

function renderStats(d) {
  const q = d.quote, st = d.stats || {};
  const rows = [
    ['Open', null], ['Prev close', fmtPrice(q.prev_close)],
    ['Day high', fmtPrice(q.day_high)], ['Day low', fmtPrice(q.day_low)],
    ['52-wk high', fmtPrice(q.week52_high ?? st.year_high)], ['52-wk low', fmtPrice(q.week52_low ?? st.year_low)],
    ['Volume', fmtNum(q.volume)], ['Avg vol (3M)', fmtNum(st.avg_volume_3m)],
    ['Market cap', fmtNum(q.market_cap ?? st.market_cap)], ['P/E', st.pe != null ? (+st.pe).toFixed(1) : '—'],
    ['EPS', st.eps != null ? (+st.eps).toFixed(2) : '—'],
    ['Div yield', st.div_yield != null ? ((+st.div_yield) > 0.5 ? (+st.div_yield).toFixed(2) : ((+st.div_yield)*100).toFixed(2)) + '%' : '—'],
    ['Beta', st.beta != null ? (+st.beta).toFixed(2) : '—'],
    ['50-day avg', fmtPrice(st.fifty_day_avg)], ['200-day avg', fmtPrice(st.two_hundred_day_avg)],
    ['Sector', st.sector || '—'],
  ].filter(r => r[1] !== null);
  $('statgrid').innerHTML = rows.map(r =>
    `<div class="stat"><span class="k">${r[0]}</span><span class="v">${r[1]}</span></div>`).join('');
}

function renderNews(news) {
  if (!news || !news.length) { $('newslist').innerHTML = '<div class="muted">No recent news.</div>'; return; }
  $('newslist').innerHTML = news.map(n => `
    <a class="newsrow" href="${esc(n.link)}" target="_blank" rel="noopener">
      <div class="nt">${esc(n.title)}</div>
      <div class="nm">${esc(n.publisher)}${n.ts ? ' · ' + ago(n.ts) : ''}</div>
    </a>`).join('');
}

function renderAlerts() {
  const sym = S.detailSym;
  const list = (S.state.alerts || []).filter(a => a.symbol === sym);
  $('alertlist').innerHTML = list.length ? list.map(a => `
    <div class="alertrow">
      <span class="k">${a.active ? '🔔' : '✅'} ${a.kind === 'above' ? 'Above' : 'Below'} ${fmtPrice(a.threshold)}
        ${a.active ? '' : `<span style="color:var(--dim2)"> · fired at ${fmtPrice(a.triggered_price)}</span>`}</span>
      <button class="del" data-a="${a.id}">Remove</button>
    </div>`).join('') : '<div class="muted" style="padding:4px 0 0">No alerts for this symbol.</div>';
  $('alertlist').querySelectorAll('.del').forEach(b => b.onclick = async () => {
    const d = await api('/api/alerts/' + b.dataset.a, {method:'DELETE'});
    S.state.alerts = d.alerts; renderAlerts();
  });
}

$('a_add').onclick = async () => {
  const thr = parseFloat($('a_thr').value);
  if (!thr || thr <= 0) { toast('Enter an alert price'); return; }
  try {
    const d = await api('/api/alerts', {method:'POST', body:JSON.stringify(
      {symbol: S.detailSym, kind: $('a_kind').value, threshold: thr})});
    S.state.alerts = d.alerts;
    $('a_thr').value = '';
    renderAlerts();
    if ('Notification' in window && Notification.permission === 'default') askNotify();
    toast('Alert set');
  } catch(e) { toast(e.message); }
};

$('p_save').onclick = async () => {
  const shares = parseFloat($('p_shares').value) || 0;
  const cost = parseFloat($('p_cost').value) || 0;
  try {
    if (S.detailPos) {
      S.state = await api('/api/positions/' + S.detailPos, {method:'PATCH',
        body:JSON.stringify({shares, cost})});
    } else {
      S.state = await api('/api/positions', {method:'POST', body:JSON.stringify(
        {portfolio_id: curPf().id, symbol: S.detailSym, shares, cost})});
      S.detailPos = (findPosition(S.detailSym) || {}).id;
    }
    renderAll(); updateDetailPrice();
    toast('Position saved');
  } catch(e) { toast(e.message); }
};

/* ── chart ─────────────────────────────────────────────────────── */

function renderRangeTabs() {
  $('ranges').innerHTML = RANGES.map(r =>
    `<button class="${r===S.range?'on':''}" data-r="${r}">${RANGE_LABELS[r]}</button>`).join('');
  $('ranges').querySelectorAll('button').forEach(b => b.onclick = () => {
    S.range = b.dataset.r; localStorage.range = S.range;
    renderRangeTabs(); loadChart();
  });
}

async function loadChart() {
  const sym = S.detailSym, rng = S.range;
  S.chart = null;
  drawChart();  // clears
  try {
    const d = await api(`/api/chart/${encodeURIComponent(sym)}?range=${rng}`);
    if (S.detailSym !== sym || S.range !== rng) return;
    S.chart = d;
    drawChart();
  } catch(e) { /* leave blank */ }
}

function drawChart(scrubX) {
  const cv = $('chart');
  const dpr = window.devicePixelRatio || 1;
  const W = cv.clientWidth || cv.parentElement.clientWidth, H = 190;
  cv.width = W * dpr; cv.height = H * dpr;
  const ctx = cv.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);
  const d = S.chart;
  if (!d || !d.points || d.points.length < 2) {
    ctx.fillStyle = '#5c6875'; ctx.font = '12px -apple-system, sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(d === null ? 'Loading…' : 'No chart data', W/2, H/2);
    $('scrub').style.display = 'none';
    return;
  }
  const pts = d.points;
  const baseline = S.range === '1d' && d.prev_close ? d.prev_close : pts[0][1];
  let lo = Math.min(...pts.map(p => p[1]), baseline);
  let hi = Math.max(...pts.map(p => p[1]), baseline);
  if (hi === lo) { hi += 1; lo -= 1; }
  const pad = (hi - lo) * 0.08;
  lo -= pad; hi += pad;
  const t0 = pts[0][0], t1 = pts[pts.length-1][0];
  const X = t => (t - t0) / (t1 - t0 || 1) * (W - 8) + 4;
  const Y = v => H - 18 - (v - lo) / (hi - lo) * (H - 34);
  const up = pts[pts.length-1][1] >= baseline;
  const col = up ? '#22c55e' : '#ef4444';

  // baseline (prev close) dashed
  ctx.strokeStyle = '#2c3648'; ctx.setLineDash([4,4]); ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(4, Y(baseline)); ctx.lineTo(W-4, Y(baseline)); ctx.stroke();
  ctx.setLineDash([]);

  // area fill
  const grad = ctx.createLinearGradient(0, 0, 0, H);
  grad.addColorStop(0, up ? 'rgba(34,197,94,.28)' : 'rgba(239,68,68,.28)');
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  ctx.beginPath();
  ctx.moveTo(X(pts[0][0]), Y(pts[0][1]));
  for (const p of pts) ctx.lineTo(X(p[0]), Y(p[1]));
  ctx.lineTo(X(t1), H-2); ctx.lineTo(X(t0), H-2); ctx.closePath();
  ctx.fillStyle = grad; ctx.fill();

  // line
  ctx.beginPath();
  ctx.moveTo(X(pts[0][0]), Y(pts[0][1]));
  for (const p of pts) ctx.lineTo(X(p[0]), Y(p[1]));
  ctx.strokeStyle = col; ctx.lineWidth = 2; ctx.lineJoin = 'round'; ctx.stroke();

  // hi/lo labels
  ctx.fillStyle = '#5c6875'; ctx.font = '10.5px -apple-system, sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText(fmtPrice(hi - pad), 6, 11);
  ctx.fillText(fmtPrice(lo + pad), 6, H - 5);

  // scrub crosshair
  const sc = $('scrub');
  if (scrubX != null) {
    let best = pts[0], bd = Infinity;
    for (const p of pts) { const dd = Math.abs(X(p[0]) - scrubX); if (dd < bd) { bd = dd; best = p; } }
    const x = X(best[0]), y = Y(best[1]);
    ctx.strokeStyle = '#8b98a5'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(x, 4); ctx.lineTo(x, H-16); ctx.stroke();
    ctx.beginPath(); ctx.arc(x, y, 3.5, 0, Math.PI*2); ctx.fillStyle = col; ctx.fill();
    const dt = new Date(best[0]*1000);
    const dstr = S.range === '1d'
      ? dt.toLocaleTimeString(undefined, {hour:'numeric', minute:'2-digit'})
      : dt.toLocaleDateString(undefined, {month:'short', day:'numeric', year:'2-digit'});
    const chg = baseline ? ((best[1]-baseline)/baseline*100) : null;
    sc.textContent = `${fmtPrice(best[1])}  ${chg!=null ? `(${sign(chg)}${chg.toFixed(2)}%)` : ''}  ${dstr}`;
    sc.style.display = 'block';
    const sw = sc.offsetWidth;
    sc.style.left = Math.max(0, Math.min(W - sw, x - sw/2)) + 'px';
  } else sc.style.display = 'none';
}

(function bindScrub() {
  const cv = $('chart');
  const pos = e => {
    const r = cv.getBoundingClientRect();
    const t = e.touches ? e.touches[0] : e;
    return t.clientX - r.left;
  };
  const mv = e => { drawChart(pos(e)); if (e.cancelable && e.touches) e.preventDefault(); };
  const end = () => drawChart();
  cv.addEventListener('touchstart', mv, {passive:false});
  cv.addEventListener('touchmove', mv, {passive:false});
  cv.addEventListener('touchend', end);
  cv.addEventListener('mousemove', mv);
  cv.addEventListener('mouseleave', end);
})();

/* ── boot ──────────────────────────────────────────────────────── */

if ('serviceWorker' in navigator) navigator.serviceWorker.register('/sw.js').catch(()=>{});

(async function boot() {
  try { await loadState(); } catch(e) { return; }
  await refresh();
  schedule();
})();
</script>
</body></html>"""

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

# Init at import time so `gunicorn portfolio:app` works on cloud hosts
init_db()
maybe_restore_on_boot()

if __name__ == "__main__":
    log.info("Portfolio tracker starting on http://localhost:%d %s", PORT, "(DEMO MODE)" if DEMO else "")
    app.run(host="0.0.0.0", port=PORT, threaded=True)
