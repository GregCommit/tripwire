"""Where every number comes from, and the maths on top of it.

Sources (all free, no keys):
  Yahoo Finance (yfinance)  stock indexes, VIX, commodities futures, dollar index, USD/JPY
  FRED (St. Louis Fed)      US Treasury yields, yield curve, mortgage rate, junk-bond spread,
                            Fed funds rate, US CPI, Japan 10-year yield and call rate (monthly)
  BIS                       Japan consumer prices, year-on-year (official figures, monthly)
  Bank of Japan             Policy Board inflation forecasts from the quarterly Outlook Report (PDF)

CNN's Fear & Greed page blocks automated reads from servers, so the mood gauge is rebuilt here
from four of CNN's seven published ingredients (see mood_series), and CNN's "market volatility"
ingredient (VIX against its 50-day average) is computed the same way CNN describes it.
"""
import csv
import io
import logging
import math
import re
import urllib.request
from datetime import date, datetime, timedelta

log = logging.getLogger("macro")
UA = "MacroWatch/1.0 (personal market dashboard)"

# ── The indicators ──────────────────────────────────────────────────────────
# kind:  index (stock index: reading = distance from 1-year high), level (reading from bands),
#        range (reading = place in 1-year range)
# chg:   pct (show changes in %) or pt (changes in percentage points / points)
# money: colour changes green/red (only for things whose rise is a gain for a stock owner)
GROUPS = [
    ("mood", "Mood", "How scared or greedy investors are right now."),
    ("stocks", "US stocks", "The big US stock indexes and the market's own fear gauge."),
    ("bonds", "US bonds & rates", "What it costs to borrow, and what bond investors expect."),
    ("commod", "Commodities", "Energy, metals and the raw materials of the economy."),
    ("macro", "Dollar & US economy", "The dollar, the Fed's rate and US inflation."),
    ("japan", "Japan & the yen", "The yen, Japan's inflation and the Bank of Japan."),
]

CALM, WATCH, ALERT = "calm", "watch", "alert"

INDICATORS = [
    # Mood (calculated here)
    dict(key="MOOD", name="Fear & Greed (estimate)", group="mood", source="calc", unit="", dec=0,
         kind="level", chg="pt", freq="daily",
         bands=[(25, "Extreme fear", ALERT), (45, "Fear", WATCH), (55, "Neutral", CALM),
                (75, "Greed", WATCH), (101, "Extreme greed", ALERT)],
         explain="A 0–100 mood score built the way CNN builds its Fear & Greed Index, from four of its "
                 "seven ingredients: stock momentum, volatility, safe-haven demand and junk-bond demand. "
                 "Each is compared with its own last two years, then averaged. Below 25 = extreme fear "
                 "(often near bottoms), above 75 = extreme greed (often when risk is highest)."),
    dict(key="VOLRATIO", name="Volatility vs normal (CNN method)", group="mood", source="calc", unit="%",
         dec=0, kind="level", chg="pt", freq="daily", signed=True,
         bands=[(-10, "Calmer than usual", CALM), (10, "Normal", CALM), (30, "Worry rising", WATCH),
                (1e9, "Fear spike", ALERT)],
         explain="CNN's 'market volatility' ingredient: how far the VIX is above or below its own "
                 "50-day average. Positive = investors are more nervous than they have been lately; "
                 "a jump above +30% usually comes with sharp sell-offs."),
    # US stocks
    dict(key="SPX", name="S&P 500", group="stocks", source="yahoo", sym="^GSPC", unit="", dec=0,
         kind="index", chg="pct", money=True, freq="daily",
         explain="The 500 largest US companies — the standard measure of 'the market'."),
    dict(key="IXIC", name="Nasdaq Composite", group="stocks", source="yahoo", sym="^IXIC", unit="", dec=0,
         kind="index", chg="pct", money=True, freq="daily",
         explain="Mostly technology and growth companies; usually swings more than the S&P 500."),
    dict(key="DJI", name="Dow Jones", group="stocks", source="yahoo", sym="^DJI", unit="", dec=0,
         kind="index", chg="pct", money=True, freq="daily",
         explain="30 large, established US companies — the oldest index, weighted by share price."),
    dict(key="RUT", name="Russell 2000", group="stocks", source="yahoo", sym="^RUT", unit="", dec=0,
         kind="index", chg="pct", money=True, freq="daily",
         explain="2,000 small US companies. They depend more on borrowing and the home economy, so "
                 "they often lead when investors grow more (or less) confident about growth."),
    dict(key="VIX", name="VIX (fear gauge)", group="stocks", source="yahoo", sym="^VIX", unit="", dec=1,
         kind="level", chg="pt", freq="daily",
         bands=[(15, "Very calm", CALM), (20, "Normal", CALM), (30, "Nervous", WATCH), (1e9, "Fear", ALERT)],
         explain="How big a move options traders expect in the S&P 500 over the next month. "
                 "Under 20 is normal; above 30 means fear and large daily swings."),
    # Bonds & rates
    dict(key="US2Y", name="2-year Treasury yield", group="bonds", source="fred", sym="DGS2", unit="%", dec=2,
         kind="range", chg="pt", freq="daily",
         explain="Interest on 2-year US government bonds. Moves mostly with where investors think "
                 "the Fed will set rates over the next two years."),
    dict(key="US10Y", name="10-year Treasury yield", group="bonds", source="fred", sym="DGS10", unit="%", dec=2,
         kind="range", chg="pt", freq="daily",
         explain="The benchmark long-term interest rate. It sets mortgage rates and is the 'safe' "
                 "return stocks must beat; a fast rise usually pressures stock prices."),
    dict(key="CURVE", name="Yield curve (10y − 2y)", group="bonds", source="fred", sym="T10Y2Y", unit=" pt",
         dec=2, kind="level", chg="pt", freq="daily", signed=True,
         bands=[(0, "Inverted", ALERT), (0.5, "Flat", WATCH), (1e9, "Normal slope", CALM)],
         explain="10-year yield minus 2-year yield. Normally positive. When negative ('inverted') "
                 "it has come before most US recessions; the recession often starts after the curve "
                 "turns positive again."),
    dict(key="MORT30", name="30-year mortgage rate", group="bonds", source="fred", sym="MORTGAGE30US",
         unit="%", dec=2, kind="range", chg="pt", freq="weekly",
         explain="Average US 30-year fixed mortgage rate (Freddie Mac, weekly). Drives housing activity."),
    dict(key="HYSPREAD", name="Junk-bond spread", group="bonds", source="fred", sym="BAMLH0A0HYM2", unit=" pt",
         dec=2, kind="level", chg="pt", freq="daily",
         bands=[(3, "Relaxed", CALM), (5, "Normal", CALM), (7, "Stress building", WATCH),
                (1e9, "Credit stress", ALERT)],
         explain="Extra interest that risky companies pay over the US government. Rising spreads mean "
                 "lenders are getting scared — often an early warning before stocks fall."),
    # Commodities
    dict(key="WTI", name="Oil (WTI)", group="commod", source="yahoo", sym="CL=F", unit="$", dec=2,
         kind="range", chg="pct", freq="daily",
         explain="US crude oil, dollars per barrel. Feeds straight into fuel prices and inflation."),
    dict(key="BRENT", name="Oil (Brent)", group="commod", source="yahoo", sym="BZ=F", unit="$", dec=2,
         kind="range", chg="pct", freq="daily",
         explain="The world benchmark crude oil price, dollars per barrel."),
    dict(key="GOLD", name="Gold", group="commod", source="yahoo", sym="GC=F", unit="$", dec=0,
         kind="range", chg="pct", freq="daily",
         explain="Dollars per ounce. A safe haven: tends to rise on fear, inflation or a weaker dollar."),
    dict(key="SILVER", name="Silver", group="commod", source="yahoo", sym="SI=F", unit="$", dec=2,
         kind="range", chg="pct", freq="daily",
         explain="Dollars per ounce. Part precious metal, part industrial metal, so it swings more than gold."),
    dict(key="COPPER", name="Copper", group="commod", source="yahoo", sym="HG=F", unit="$", dec=2,
         kind="range", chg="pct", freq="daily",
         explain="Dollars per pound. Used in almost everything that is built, so it is read as a gauge "
                 "of world economic health ('Dr. Copper')."),
    dict(key="NATGAS", name="Natural gas", group="commod", source="yahoo", sym="NG=F", unit="$", dec=2,
         kind="range", chg="pct", freq="daily",
         explain="US natural gas, dollars per million BTU. Very weather-driven and very jumpy."),
    # Dollar & macro
    dict(key="DXY", name="US dollar index", group="macro", source="yahoo", sym="DX-Y.NYB", unit="", dec=2,
         kind="range", chg="pct", freq="daily",
         explain="The dollar against six major currencies (mostly the euro). A strong dollar hurts US "
                 "exporters and makes commodities more expensive for the rest of the world."),
    dict(key="FEDFUNDS", name="Fed funds rate", group="macro", source="fred", sym="DFF", unit="%", dec=2,
         kind="range", chg="pt", freq="daily",
         explain="The overnight rate the Federal Reserve steers. Every other US interest rate starts here."),
    dict(key="USCPI", name="US inflation (CPI, yearly)", group="macro", source="fred", sym="CPIAUCSL", yoy=True,
         unit="%", dec=1, kind="level", chg="pt", freq="monthly",
         bands=[(2.5, "Near the Fed's 2% target", CALM), (4, "Above target", WATCH), (1e9, "High", ALERT)],
         explain="How much consumer prices rose over the past 12 months. The Fed aims for 2%."),
    # Japan
    dict(key="USDJPY", name="USD/JPY", group="japan", source="yahoo", sym="JPY=X", unit="¥", dec=2,
         kind="level", chg="pct", freq="daily",
         bands=[(140, "Strong yen", CALM), (150, "Middle range", CALM), (158, "Weak yen", WATCH),
                (1e9, "Very weak yen", ALERT)],
         explain="Yen per US dollar. Higher = weaker yen. Japan's Ministry of Finance bought yen to "
                 "stop the slide near 160 in 2024, so that level is watched closely."),
    dict(key="JPCPI", name="Japan inflation (CPI, yearly)", group="japan", source="bis", unit="%", dec=1,
         kind="level", chg="pt", freq="monthly",
         bands=[(1, "Below the BoJ's 2% target", WATCH), (3, "Around the BoJ's 2% target", CALM),
                (1e9, "Well above target", WATCH)],
         explain="How much Japanese consumer prices rose over the past 12 months (all items, official "
                 "figure). After decades near zero, sustained 2% inflation lets the Bank of Japan keep "
                 "raising rates — which tends to strengthen the yen."),
    dict(key="JP10Y", name="Japan 10-year yield", group="japan", source="fred", sym="IRLTLT01JPM156N",
         unit="%", dec=2, kind="range", chg="pt", freq="monthly",
         explain="Interest on 10-year Japanese government bonds (monthly average). The gap to the US "
                 "10-year yield is a big driver of USD/JPY: a narrower gap tends to strengthen the yen."),
    dict(key="BOJRATE", name="Japan overnight rate", group="japan", source="fred", sym="IRSTCI01JPM156N",
         unit="%", dec=2, kind="range", chg="pt", freq="monthly",
         explain="Japan's overnight call rate (monthly average) — the rate the Bank of Japan targets."),
    # Helpers used only in calculations
    dict(key="IEF", name="7–10y Treasury bond fund", group=None, source="yahoo", sym="IEF", unit="$", dec=2,
         kind="range", chg="pct", freq="daily", explain=""),
]
BY_KEY = {i["key"]: i for i in INDICATORS}


# ── Fetching ────────────────────────────────────────────────────────────────
def http_get(url, timeout=40, ua=UA):
    req = urllib.request.Request(url, headers={"User-Agent": ua})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _clean(pts):
    """Sorted [[YYYY-MM-DD, float]] with missing / non-finite values dropped, one per date."""
    seen = {}
    for d, v in pts:
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(v):
            seen[d] = v
    return [[d, seen[d]] for d in sorted(seen)]


def fetch_yahoo(symbols, period="5y"):
    """{symbol: points} — daily closes (five years unless told otherwise); today's bar is the latest price."""
    import yfinance as yf
    df = yf.download(symbols, period=period, interval="1d", auto_adjust=False, progress=False,
                     group_by="ticker", threads=False)
    out = {}
    for s in symbols:
        try:
            col = df[s]["Close"]
            pts = _clean([[i.strftime("%Y-%m-%d"), v] for i, v in col.items()])
            if pts:
                out[s] = pts
        except Exception as e:
            log.warning("Yahoo %s: %s", s, e)
    return out


def fetch_fred(series_id, start="2014-01-01"):
    raw = http_get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={start}").decode()
    rows = list(csv.reader(io.StringIO(raw)))
    return _clean([[r[0], r[1]] for r in rows[1:] if len(r) >= 2 and r[0] >= start])


def yoy(pts):
    """Monthly index → % change over 12 months."""
    by = {d[:7]: v for d, v in pts}
    out = []
    for d, v in pts:
        y, m = int(d[:4]), d[5:7]
        prev = by.get(f"{y - 1}-{m}")
        if prev:
            out.append([d, (v / prev - 1) * 100])
    return out


def fetch_japan_cpi(start="2010-01"):
    raw = http_get("https://stats.bis.org/api/v2/data/dataflow/BIS/WS_LONG_CPI/1.0/M.JP.771"
                   f"?startPeriod={start}&format=csv").decode()
    rows = list(csv.reader(io.StringIO(raw)))
    head = rows[0]
    ti, vi = head.index("TIME_PERIOD"), head.index("OBS_VALUE")
    return _clean([[r[ti] + "-01", r[vi]] for r in rows[1:] if len(r) > vi])


_NUM = r"([+\-−]?\d+\.\d)"
_CELL = re.compile(_NUM + r"(?:\s*to\s*" + _NUM + r")?\s*\[" + _NUM + r"\]")


def _cell(m):
    f = lambda s: float(s.replace("−", "-")) if s else None
    lo, hi, med = f(m[0]), f(m[1]), f(m[2])
    return {"low": lo, "high": hi if hi is not None else lo, "median": med}


def parse_boj_outlook(pdf_bytes):
    """The appendix table 'Forecasts of the Majority of the Policy Board Members'.
    Returns [{fy, gdp, cpi, core, prev_cpi, prev_label}] — cpi = CPI less fresh food (the BoJ's
    main measure), core = less fresh food and energy."""
    import pypdf
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    text = next((p.extract_text() for p in reader.pages
                 if "Policy Board Members" in (p.extract_text() or "") and "Fiscal" in (p.extract_text() or "")), "")
    rows, last = [], None
    for line in text.splitlines():
        cells = _CELL.findall(line)
        if len(cells) < 2:
            continue
        m = re.search(r"Fiscal (\d{4})", line)
        if m:
            last = {"fy": int(m.group(1)), "gdp": _cell(cells[0]), "cpi": _cell(cells[1]),
                    "core": _cell(cells[2]) if len(cells) > 2 else None}
            rows.append(last)
        elif last is not None and "Forecasts made in" in line:
            pm = re.search(r"Forecasts made in (\w+ \d{4})", line)
            last["prev_cpi"] = _cell(cells[1])
            last["prev_label"] = pm.group(1) if pm else ""
    return rows


MONTHS = {1: "January", 4: "April", 7: "July", 10: "October"}


def fetch_boj_outlook(today=None):
    """Newest Outlook Report that exists (reports come out in Jan, Apr, Jul and Oct)."""
    today = today or date.today()
    for y in (today.year, today.year - 1):
        for mth in (10, 7, 4, 1):
            if (y, mth) > (today.year, today.month):
                continue
            url = f"https://www.boj.or.jp/en/mopo/outlook/gor{y % 100:02d}{mth:02d}a.pdf"
            try:
                rows = parse_boj_outlook(http_get(url))
            except Exception:
                continue
            if rows:
                return {"report": f"{MONTHS[mth]} {y}", "id": f"{y}-{mth:02d}", "url": url, "rows": rows}
    raise RuntimeError("No Bank of Japan Outlook Report found")


# ── Maths ───────────────────────────────────────────────────────────────────
def _d(s):
    return datetime.strptime(s, "%Y-%m-%d").date()


def _at_or_before(pts, day):
    """Last value on or before `day` (pts sorted)."""
    lo, hi = 0, len(pts) - 1
    if not pts or _d(pts[0][0]) > day:
        return None
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _d(pts[mid][0]) <= day:
            lo = mid
        else:
            hi = mid - 1
    return pts[lo]


def _change(ind, new, old):
    if new is None or old is None:
        return None
    if ind["chg"] == "pct":
        return (new / old - 1) * 100 if old else None
    return new - old


def _band(ind, v):
    for upper, label, tone in ind.get("bands", []):
        if v < upper:
            return label, tone
    return None, CALM


def summarize(ind, pts):
    """Everything a card shows: value, changes, 1-year range, reading, unusual move, sparkline."""
    if not pts:
        return None
    last_d, last_v = pts[-1]
    day = _d(last_d)
    prev = pts[-2] if len(pts) > 1 else None
    m1 = _at_or_before(pts, day - timedelta(days=30))
    y1 = _at_or_before(pts, day - timedelta(days=365))
    year = [v for d, v in pts if _d(d) > day - timedelta(days=365)]
    hi, lo = max(year), min(year)
    pos = (last_v - lo) / (hi - lo) if hi > lo else 0.5
    s = {
        "key": ind["key"], "name": ind["name"], "group": ind["group"], "unit": ind["unit"], "dec": ind["dec"],
        "chgKind": ind["chg"], "money": bool(ind.get("money")), "signed": bool(ind.get("signed")),
        "freq": ind["freq"], "explain": ind["explain"], "source": ind["source"],
        "date": last_d, "value": last_v,
        "prevDate": prev[0] if prev else None,
        "d1": _change(ind, last_v, prev[1]) if prev else None,
        "m1": _change(ind, last_v, m1[1]) if m1 else None,
        "y1": _change(ind, last_v, y1[1]) if y1 else None,
        "hi": hi, "lo": lo, "pos": pos,
    }
    # Plain-English reading
    tone = CALM
    if ind["kind"] == "index":
        off = (last_v / hi - 1) * 100
        if off > -2:
            reading = "At or near its 1-year high"
        elif off > -10:
            reading = f"{-off:.0f}% below its 1-year high — a normal pullback"
        elif off > -20:
            reading, tone = f"{-off:.0f}% below its 1-year high — a correction", WATCH
        else:
            reading, tone = f"{-off:.0f}% below its 1-year high — bear-market territory", ALERT
    elif ind["kind"] == "level":
        reading, tone = _band(ind, last_v)
    else:
        reading = ("Near its 1-year high" if pos >= 0.9 else "Near its 1-year low" if pos <= 0.1
                   else "Middle of its 1-year range")
    s["reading"], s["tone"] = reading, tone
    # Unusual move: today's change against the typical change size of the last year
    s["z"] = None
    if ind["freq"] in ("daily", "weekly") and len(pts) > 60 and s["d1"] is not None:
        recent = pts[-253:]
        diffs = [_change(ind, b[1], a[1]) for a, b in zip(recent[:-2], recent[1:-1])]
        diffs = [x for x in diffs if x is not None]
        if len(diffs) > 40:
            sd = (sum(x * x for x in diffs) / len(diffs)) ** 0.5   # around zero, not the mean
            if sd > 0:
                s["z"] = s["d1"] / sd
    s["unusual"] = s["z"] is not None and abs(s["z"]) >= 2.5
    # Sparkline: the last year, at most ~90 points
    yr = [p for p in pts if _d(p[0]) > day - timedelta(days=365)]
    step = max(1, len(yr) // 90)
    spark = yr[::step]
    if spark[-1] != yr[-1]:
        spark.append(yr[-1])
    s["spark"] = [round(v, 4) for _, v in spark]
    return s


def _mean(xs):
    return sum(xs) / len(xs)


def _pct_rank(window, x):
    return sum(1 for w in window if w <= x) / len(window)


def mood_series(spx, vix, ief, hy, days=520, lookback=504):
    """Fear & Greed estimate and CNN's volatility ingredient, one value per S&P 500 trading day.

    Ingredients (CNN's definitions; each scored 0–100 by where today sits in its last two years):
      momentum      S&P 500 vs its 125-day average
      volatility    VIX vs its 50-day average (higher = more fear, so inverted)
      safe haven    20-day return of stocks minus 20-day return of Treasury bonds
      junk bonds    junk-bond spread (higher = more fear, so inverted)
    Returns (mood_points, volratio_points, latest_ingredient_scores)."""
    if len(spx) < 200 or len(vix) < 100:
        return [], [], {}
    vmap, imap = dict(vix), dict(ief)
    dates = [d for d, _ in spx]
    px = [v for _, v in spx]
    raw = {"momentum": [], "volatility": [], "safe_haven": [], "junk": []}
    vr = []
    vix_seq = []
    for i, d in enumerate(dates):
        vx = vmap.get(d, vix_seq[-1] if vix_seq else None)
        vix_seq.append(vx)
        mom = px[i] / _mean(px[i - 124:i + 1]) - 1 if i >= 124 else None
        vol = None
        if i >= 49 and all(v is not None for v in vix_seq[-50:]):
            vol = vx / _mean(vix_seq[-50:]) - 1
        safe = None
        if i >= 20 and d in imap and dates[i - 20] in imap:
            safe = (px[i] / px[i - 20] - 1) - (imap[d] / imap[dates[i - 20]] - 1)
        h = _at_or_before(hy, _d(d)) if hy else None
        junk = -h[1] if h else None
        raw["momentum"].append(mom)
        raw["volatility"].append(-vol if vol is not None else None)
        raw["safe_haven"].append(safe)
        raw["junk"].append(junk)
        vr.append([d, vol * 100] if vol is not None else None)
    mood, latest = [], {}
    n = len(dates)
    for i in range(max(0, n - days), n):
        scores = {}
        for name, seq in raw.items():
            x = seq[i]
            if x is None:
                continue
            window = [w for w in seq[max(0, i - lookback + 1):i + 1] if w is not None]
            if len(window) >= 120:
                scores[name] = _pct_rank(window, x) * 100
        if len(scores) >= 3:
            mood.append([dates[i], _mean(list(scores.values()))])
            latest = scores
    return mood, [p for p in vr if p][-days:], latest


# ══ The Extremes hub ════════════════════════════════════════════════════════
# Indicators that only matter when they are far out on either side of their own history.
# Each one is placed on a 0–100 scale (its percentile against everything we know about it) and
# flagged "extreme" at the outer 5% on either side, "stretched" at the outer 10%.
HUB_CATS = [
    ("yen", "Yen, Japan & dollar", "The yen carry trade and what drives it: a very weak or very strong yen, the interest gap and Japan's own yields."),
    ("value", "Valuation", "How expensive the US stock market is against the economy, its own earnings and its own trend."),
    ("credit", "Credit & economy", "Bond-market and economic warning lights that rarely matter until they are far from normal."),
    ("mood", "Sentiment & risk", "How scared or relaxed investors and consumers are."),
]
EXTREME_PCT, STRETCHED_PCT = 5, 10      # outer share of history, per side

HUB = [
    # ── Yen, Japan & dollar
    dict(key="USDJPY_L", name="USD/JPY (yen per dollar)", cat="yen", kind="fred", sym="DEXJPUS", extend="USDJPY",
         unit="¥", dec=1, freq="daily",
         why="Yen per US dollar. Higher = weaker yen. In the middle of its range it says little; at the ends it moves global markets.",
         high="The yen is extremely weak. Near these levels Japan's Ministry of Finance bought yen (160 in 2024), and the yen carry trade — borrowing cheap yen to buy higher-yielding assets — is at its most profitable and most crowded. A sharp yen rebound can then hit stocks everywhere.",
         low="The yen is extremely strong. A sudden yen rise is what forced carry trades to unwind in August 2024 and shook stock markets worldwide."),
    dict(key="GAP", name="US–Japan 10-year yield gap", cat="yen", kind="gap", unit=" pt", dec=2, freq="monthly",
         why="US 10-year yield minus Japan's. It is what the yen carry trade 'pays': the wider the gap, the more profitable it is to sell yen for dollars.",
         high="US yields are far above Japan's. The carry trade pays a lot, which keeps the yen weak — and leaves it exposed if the gap narrows quickly.",
         low="The gap is unusually small. The carry trade pays little, which tends to strengthen the yen and can trigger unwinds."),
    dict(key="JP10Y_L", name="Japan 10-year yield", cat="yen", kind="fred", sym="IRLTLT01JPM156N", start="1985-01-01",
         unit="%", dec=2, freq="monthly",
         why="Interest on 10-year Japanese government bonds (monthly average).",
         high="Japanese yields are at multi-decade highs. Japan's investors, the world's biggest foreign lenders, get a reason to bring money home — which can lift yields elsewhere and strengthen the yen.",
         low="Japanese yields are extremely low, the old era of near-zero rates. Japan's money keeps flowing abroad."),
    dict(key="NIKKEI200", name="Nikkei vs its 200-day average", cat="yen", kind="dev200", sym="^N225", unit="%", dec=1,
         freq="daily", signed=True,
         why="How far Japan's stock market is above or below its 200-day average — a measure of how stretched it is.",
         high="The Nikkei is far above its long-term average — a stretched rally. Pullbacks have often followed.",
         low="The Nikkei is far below its long-term average — panic selling, like the early-August 2024 crash."),
    dict(key="DXY_L", name="US dollar index", cat="yen", kind="yahoo", sym="DX-Y.NYB", unit="", dec=1, freq="daily",
         why="The dollar against six major currencies.",
         high="The dollar is extremely strong. That squeezes emerging markets and US exporters, pushes commodity prices down and usually comes with stress abroad.",
         low="The dollar is extremely weak. That tends to lift commodities and foreign stocks, and can reflect doubts about US assets."),
    # ── Valuation
    dict(key="BUFFETT", name="Buffett indicator (stock market ÷ GDP)", cat="value", kind="buffett", unit="%", dec=0,
         freq="quarterly",
         why="Total value of US companies' shares divided by the size of the economy — Warren Buffett's favourite yardstick. Based on Federal Reserve data (non-financial companies), moved forward with the S&P 500 between quarterly releases, so the level differs from the Wilshire-based version you may see elsewhere, but the history and the extremes line up.",
         high="Stocks are extremely expensive compared with the economy. Historically that has gone with weak returns over the following 10 years — but it can stay this way for years, so it is a long-run warning, not a timing tool.",
         low="Stocks are cheap compared with the economy. From levels like this, long-run returns have historically been good."),
    dict(key="CAPE", name="Shiller P/E (CAPE)", cat="value", kind="cape", unit="", dec=1, freq="monthly",
         why="S&P 500 price divided by its average inflation-adjusted earnings of the past 10 years.",
         high="The market is extremely expensive against its long-run earnings. Only 1929 and 1999–2000 were similar. Good for caution about the next decade, useless for timing next month.",
         low="The market is cheap against its long-run earnings — the kind of level seen in deep bear markets."),
    dict(key="ERP", name="Stock reward over bonds", cat="value", kind="erp", unit=" pt", dec=2, freq="monthly",
         why="Earnings yield (100 ÷ Shiller P/E) minus the inflation-protected 10-year Treasury yield — what stocks promise over safe government bonds.",
         high="Stocks pay unusually well compared with safe bonds — a wide cushion.",
         low="Stocks promise almost nothing extra over safe bonds (as in 2000 and 2021) — little cushion if earnings disappoint."),
    dict(key="SPX200", name="S&P 500 vs its 200-day average", cat="value", kind="dev200", sym="^GSPC", unit="%", dec=1,
         freq="daily", signed=True,
         why="How far the S&P 500 is above or below its 200-day average.",
         high="The S&P 500 is stretched far above its long-term trend. Rallies this extended have often cooled off — not a certainty.",
         low="The S&P 500 is far below its long-term trend: panic selling. Such washouts have often been near market lows, but selling can continue."),
    # ── Credit & economy
    dict(key="CURVE_L", name="Yield curve (10y − 2y)", cat="credit", kind="fred", sym="T10Y2Y", unit=" pt", dec=2,
         freq="daily", signed=True,
         why="10-year minus 2-year Treasury yield.",
         high="The curve is unusually steep. That typically happens when the Fed has cut rates hard in or after a downturn.",
         low="The curve is deeply inverted — short rates far above long rates. That has come before most US recessions, with a lag of roughly 6–24 months."),
    dict(key="HY_L", name="Junk-bond spread", cat="credit", kind="fred", sym="BAMLH0A0HYM2", unit=" pt", dec=2,
         freq="daily",
         why="Extra interest risky companies pay over the US government.",
         high="Lenders demand a lot extra from risky companies: credit stress. It usually coincides with stock sell-offs and recession fears.",
         low="Spreads are extremely tight: lenders are relaxed, even complacent. There is little cushion if things turn."),
    dict(key="NFCI", name="Financial conditions (Chicago Fed)", cat="credit", kind="fred", sym="NFCI", start="1971-01-01",
         unit="", dec=2, freq="weekly", signed=True,
         why="Chicago Fed index of how easy or hard it is to get money. Below 0 = looser than average, above 0 = tighter.",
         high="Financial conditions are very tight: credit is hard to get — typical of crises.",
         low="Financial conditions are very loose: money is easy and cheap, which often goes with complacency."),
    dict(key="SAHM", name="Sahm recession rule", cat="credit", kind="fred", sym="SAHMREALTIME", start="1959-01-01",
         unit=" pt", dec=2, freq="monthly", sides="high",
         why="Rises when unemployment's 3-month average is above its 12-month low. A reading of 0.5 or more has signalled the start of every US recession since 1970.",
         high="Unemployment is rising fast enough that, historically, the economy was already in a recession. Check the news: is it real or a distortion?",
         low=""),
    dict(key="US10Y_L", name="US 10-year yield", cat="credit", kind="fred", sym="DGS10", start="1962-01-01", unit="%",
         dec=2, freq="daily",
         why="The benchmark long-term borrowing cost.",
         high="Long-term borrowing costs are very high. That pressures stocks, housing and government budgets.",
         low="Yields are extremely low — fear or deflation worries, and investors reaching for any return."),
    # ── Sentiment & risk
    dict(key="VIX_L", name="VIX (stock fear gauge)", cat="mood", kind="fred", sym="VIXCLS", start="1990-01-01",
         unit="", dec=1, freq="daily",
         why="Expected size of S&P 500 moves over the next month, from option prices.",
         high="Panic. The VIX spikes with crashes. Buying right at the peak is hard, but extreme readings have usually been close to turning points.",
         low="Extreme calm. Complacency is highest when the VIX is lowest, and a jump from here can be violent."),
    dict(key="MOVE", name="MOVE (bond-market fear gauge)", cat="mood", kind="yahoo", sym="^MOVE", unit="", dec=0,
         freq="daily",
         why="The VIX's cousin for US Treasury bonds.",
         high="Bond-market turmoil. It often spills into stocks, currencies and funding markets.",
         low="Unusually calm bond market."),
    dict(key="UMCSENT", name="Consumer sentiment (Michigan)", cat="mood", kind="fred", sym="UMCSENT", start="1952-01-01",
         unit="", dec=1, freq="monthly",
         why="University of Michigan survey of how confident US households feel.",
         high="Households are extremely upbeat — euphoria is rarely a good sign for returns.",
         low="Households are extremely gloomy. As a contrarian signal, sentiment lows have often come near stock-market bottoms."),
    dict(key="CUAU", name="Copper ÷ gold", cat="mood", kind="ratio", sym="HG=F", sym2="GC=F", mult=1000, unit="", dec=2,
         freq="daily",
         why="Copper (needed for growth) against gold (safety). Rising = markets expect growth, falling = they want safety.",
         high="Markets are betting hard on growth — optimism is high.",
         low="Markets want safety far more than growth — a recession scare."),
]
HUB_BY_KEY = {h["key"]: h for h in HUB}
HUB_YAHOO = sorted({s for h in HUB for s in (h.get("sym"), h.get("sym2")) if s and h["kind"] in ("yahoo", "dev200", "ratio")})


def fetch_cape():
    """Shiller P/E, monthly, from multpl.com's table (a plain HTML page)."""
    html = http_get("https://www.multpl.com/shiller-pe/table/by-month",
                    ua="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36").decode("utf-8", "replace")
    out = []
    for m in re.finditer(r"<td[^>]*>\s*([A-Z][a-z]{2} \d{1,2}, \d{4})\s*</td>\s*<td[^>]*>(.*?)</td>", html, re.S):
        val = re.sub(r"<[^>]+>", " ", m.group(2))
        num = re.search(r"\d+(?:\.\d+)?", val.replace(",", ""))
        if num:
            out.append([datetime.strptime(m.group(1), "%b %d, %Y").strftime("%Y-%m-01"), num.group(0)])
    pts = _clean(out)
    if len(pts) < 100:
        raise RuntimeError(f"CAPE table looked wrong ({len(pts)} rows)")
    return pts


def monthly_avg(pts):
    by = {}
    for d, v in pts:
        by.setdefault(d[:7], []).append(v)
    return [[k + "-01", sum(v) / len(v)] for k, v in sorted(by.items())]


def sma_dev(pts, n=200):
    """Percent distance from the n-day moving average."""
    out, run, vals = [], 0.0, []
    for d, v in pts:
        vals.append(v)
        run += v
        if len(vals) > n:
            run -= vals[-n - 1]
        if len(vals) >= n:
            out.append([d, (v / (run / n) - 1) * 100])
    return out


def _join(a, b, f):
    """Combine two series on the dates they share."""
    bm = dict(b)
    return [[d, f(v, bm[d])] for d, v in a if d in bm]


class HubData:
    """Fetches each raw series once per cycle; items are built from these."""

    def __init__(self, own):
        self.own, self._fred, self._cape, self._yahoo = own, {}, None, {}

    def load_yahoo(self):
        if HUB_YAHOO:
            self._yahoo = fetch_yahoo(HUB_YAHOO, period="max")

    def fred(self, sym, start="1950-01-01"):
        if (sym, start) not in self._fred:
            self._fred[(sym, start)] = fetch_fred(sym, start)
        return self._fred[(sym, start)]

    def cape(self):
        if self._cape is None:
            self._cape = fetch_cape()
        return self._cape

    def yahoo(self, sym):
        pts = self._yahoo.get(sym)
        if not pts:
            raise RuntimeError(f"Yahoo returned nothing for {sym}")
        return pts


def build_hub(item, hd):
    """Raw history for one hub item → [[date, value]]."""
    k = item["kind"]
    if k == "fred":
        pts = hd.fred(item["sym"], item.get("start", "1950-01-01"))
        if item.get("extend"):      # newest days from the regular (Yahoo) series
            last = pts[-1][0] if pts else ""
            pts = pts + [p for p in hd.own(item["extend"]) if p[0] > last]
        return pts
    if k == "yahoo":
        return hd.yahoo(item["sym"])
    if k == "dev200":
        return sma_dev(hd.yahoo(item["sym"]))
    if k == "ratio":
        return _join(hd.yahoo(item["sym"]), hd.yahoo(item["sym2"]), lambda a, b: a / b * item["mult"] if b else float("nan"))
    if k == "cape":
        return hd.cape()
    if k == "gap":
        return _join(hd.fred("GS10", "1985-01-01"), hd.fred("IRLTLT01JPM156N", "1985-01-01"), lambda a, b: a - b)
    if k == "erp":
        real = monthly_avg(hd.fred("DFII10", "2003-01-01"))
        return _join(hd.cape(), real, lambda cape, r: 100 / cape - r)
    if k == "buffett":
        eq, gdp = hd.fred("NCBEILQ027S", "1952-01-01"), hd.fred("GDP", "1952-01-01")
        out = []
        for d, v in eq:
            g = _at_or_before(gdp, _d(d))
            if g and g[1] > 0:
                out.append([d, v / 1000 / g[1] * 100])      # equities in $ millions, GDP in $ billions
        # The Fed publishes the latest quarter ~10 weeks late: move it forward with the S&P 500.
        spx = hd.own("SPX")
        if out and spx:
            y, m = int(out[-1][0][:4]), int(out[-1][0][5:7])
            end = date(y + (m + 2) // 12, (m + 2) % 12 + 1, 1) - timedelta(days=1)       # last day of that quarter
            base = _at_or_before(spx, end)
            if base and spx[-1][0] > end.isoformat():
                out.append([spx[-1][0], out[-1][1] * spx[-1][1] / base[1]])
        return out
    raise ValueError(f"unknown hub kind {k}")


def extreme_stats(item, pts):
    """Where the latest value sits in the item's own history, and what that means right now."""
    pts = _clean(pts)
    if len(pts) < 36:
        return None
    vals = [v for _, v in pts]
    last_d, last = pts[-1]
    ordered = sorted(vals)
    n = len(ordered)
    from bisect import bisect_left, bisect_right
    pct = (bisect_left(ordered, last) + bisect_right(ordered, last)) / 2 / n * 100
    q = lambda p: ordered[min(n - 1, max(0, round(p / 100 * (n - 1))))]
    sides = item.get("sides", "both")
    zone = "quiet"
    if sides in ("both", "high"):
        zone = "extreme_high" if pct >= 100 - EXTREME_PCT else "stretched_high" if pct >= 100 - STRETCHED_PCT else zone
    if sides in ("both", "low") and zone == "quiet":
        zone = "extreme_low" if pct <= EXTREME_PCT else "stretched_low" if pct <= STRETCHED_PCT else zone
    side = "high" if zone.endswith("high") else "low" if zone.endswith("low") else None
    # When was it last this far out? (None = never before in the data we have)
    since = None
    if side:
        for d, v in reversed(pts[:-1]):
            if (v >= last) if side == "high" else (v <= last):
                since = d
                break
    # Highest / lowest in five years (a local extreme, even if not extreme in the long run)
    day = _d(last_d)
    five = [v for d, v in pts if _d(d) > day - timedelta(days=5 * 365)]
    local = None
    if len(five) > 12 and not side:
        local = "high" if last >= max(five) else "low" if last <= min(five) else None
    y1 = _at_or_before(pts, day - timedelta(days=365))
    step = max(1, n // 120)
    spark = [round(v, 4) for _, v in pts[::step]]
    if (n - 1) % step:
        spark.append(round(last, 4))
    return {
        "key": item["key"], "name": item["name"], "cat": item["cat"], "unit": item["unit"], "dec": item["dec"],
        "signed": bool(item.get("signed")), "freq": item["freq"], "why": item["why"], "sides": sides,
        "meaning": item.get(side or "", "") if side else "",
        "date": last_d, "value": last, "pct": round(pct, 1), "zone": zone, "side": side,
        "since": since, "start": pts[0][0], "n": n, "local": local,
        "p5": q(EXTREME_PCT), "p95": q(100 - EXTREME_PCT), "median": q(50),
        "y1": y1[1] if y1 else None, "spark": spark,
    }
