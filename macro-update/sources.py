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
    dict(key="US1Y", name="1-year Treasury yield", group="bonds", source="fred", sym="DGS1", unit="%", dec=2,
         kind="range", chg="pt", freq="daily",
         explain="Interest on 1-year US government bonds: the market's view of where the Fed's rate will be within a year."),
    dict(key="REAL10", name="10-year real yield (after inflation)", group="bonds", source="fred", sym="DFII10",
         unit="%", dec=2, kind="range", chg="pt", freq="daily",
         explain="The 10-year Treasury yield after subtracting expected inflation (inflation-protected bonds): the true price of money. Gold and growth stocks struggle when it rises."),
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
    # Special today / this week / this month, and how much it matters for stock investing
    w1 = _at_or_before(pts, day - timedelta(days=7))
    s["w1"] = _change(ind, last_v, w1[1]) if w1 else None
    mv = moves(pts, ind["freq"], ind["chg"])
    s.update(z1=mv["z1"], zw=mv["zw"], zm=mv["zm"])
    s["horizon"], s["spec"] = special(ind["freq"], mv, tone=tone, edge=(pos >= 0.98 or pos <= 0.02) and hi > lo)
    s["rank"] = round(s["spec"] * WEIGHT.get(ind["key"], 2), 2)
    s["says"] = SAYS.get(ind["key"]) or ind["explain"]
    s["good"] = GOOD.get(ind["key"], 1 if ind.get("money") else 0)
    s["hub"] = False
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
    ("metals", "Gold, silver & Dr. Copper", "Metals as a mirror of fear, growth and interest rates."),
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
    dict(key="CCC", name="Weakest junk bonds: extra interest (CCC)", cat="credit", kind="fred", sym="BAMLH0A3HYC", unit=" pt", dec=2,
         freq="daily",
         why="Extra interest the riskiest companies (rated CCC and below) pay over the government, updated daily. These weakest borrowers default first, so their spread widens months before defaults show up in bank statistics — it is an early warning, not a report on the past.",
         high="Lenders are afraid the weakest companies will not pay back. Defaults usually rise within months, and stocks tend to fall with it.",
         low="Lenders are relaxed even about the weakest borrowers. Money is easy, which also means little cushion if sentiment turns."),
    dict(key="ICSA", name="New jobless claims (weekly)", cat="credit", kind="fred", sym="ICSA", start="1967-01-01", unit="", dec=0,
         freq="weekly",
         why="How many Americans filed for unemployment benefits for the first time last week, published every Thursday. People who lose their job fall behind on their loans a few months later, so claims lead household loan defaults.",
         high="Layoffs are rising fast. Household incomes — and soon their ability to pay loans — are under pressure, which usually precedes more defaults and weaker spending.",
         low="Very few layoffs: households' incomes are secure, so loan defaults should stay low."),
    dict(key="SLOOS_CARD", name="Banks tightening credit-card lending", cat="credit", kind="fred", sym="DRTSCLCC", start="1990-01-01",
         unit="%", dec=0, freq="quarterly", signed=True,
         why="Share of US banks tightening their standards for credit cards, minus the share loosening (Federal Reserve survey of loan officers, quarterly). Banks see their customers' problems first and react by lending less — which slows spending and often comes before more defaults.",
         high="Many banks are making it harder to borrow on cards: they expect more households to struggle. Spending usually slows afterwards.",
         low="Banks are loosening: they feel confident about households' ability to pay."),
    dict(key="SLOOS_BUS", name="Banks tightening business lending", cat="credit", kind="fred", sym="DRTSCILM", start="1990-01-01",
         unit="%", dec=0, freq="quarterly", signed=True,
         why="Share of US banks tightening their standards for loans to medium and large companies, minus the share loosening (Federal Reserve survey, quarterly). Tighter lending starves companies of money, so it leads slower hiring and investment, and often recessions.",
         high="Banks are clamping down on business loans — historically a reliable warning that a slowdown or recession is coming.",
         low="Banks are loosening: credit is flowing to companies, which supports growth."),
    dict(key="STLFSI", name="Financial stress (St. Louis Fed)", cat="credit", kind="fred", sym="STLFSI4", start="1993-01-01", unit="", dec=2,
         freq="weekly", signed=True,
         why="Index of 18 weekly market measures (interest rates, spreads, volatility). Zero is average stress; above zero is more stress than usual.",
         high="Financial markets are under unusual stress — typical of crises, when borrowers struggle and lenders pull back.",
         low="Very little stress in the financial system: money is easy and conditions are calm."),
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
    dict(key="CUAU", name="Copper ÷ gold (growth vs fear)", cat="metals", kind="ratio", sym="HG=F", sym2="GC=F", mult=1000,
         unit="", dec=2, freq="daily",
         why="Copper goes into almost everything that is built or manufactured — wiring, pipes, cars, electronics — so its price is read as a health check on the world economy ('Dr. Copper'). Gold is what people buy when they are afraid. Copper divided by gold therefore rises when investors expect growth and falls when they want safety.",
         high="Copper is very expensive against gold: markets are betting hard on growth (factories, construction, China). Optimism is high, which is good for the economy but leaves little room for disappointment.",
         low="Copper is very cheap against gold: markets want safety far more than growth. Past lows came with recession scares, and often shortly before or during stock-market declines."),
    dict(key="GSR", name="Gold ÷ silver ratio", cat="metals", kind="ratio", sym="GC=F", sym2="SI=F", mult=1, unit="", dec=1,
         freq="daily",
         why="How many ounces of silver one ounce of gold buys. Gold is the pure safe haven; silver is half precious, half industrial metal. The ratio rises when investors run to safety and falls when they expect growth and inflation.",
         high="Gold is very expensive against silver: fear or recession worries dominate and industrial demand is weak. Ratios this high have come with crises (2008, 2020).",
         low="Silver is very expensive against gold: investors expect growth and inflation, and speculative money is flowing into metals. Often a late-stage 'everything is rising' signal."),
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
    chg = "pt" if item["unit"] in ("%", " pt") or item.get("signed") else "pct"
    mv = moves(pts, item["freq"], chg)
    horizon, spec = special(item["freq"], mv, hub_zone=zone, pct=pct)
    return {
        "hub": True, "good": GOOD.get(item["key"], 0), "chgKind": chg, "d1": mv["d1"], "w1": mv["w1"], "m1": mv["m1"], "z1": mv["z1"], "zw": mv["zw"],
        "zm": mv["zm"], "horizon": horizon, "spec": spec, "rank": round(spec * WEIGHT.get(item["key"], 2.5), 2),
        "key": item["key"], "name": item["name"], "cat": item["cat"], "unit": item["unit"], "dec": item["dec"],
        "signed": bool(item.get("signed")), "freq": item["freq"], "why": item["why"], "sides": sides,
        "meaning": item.get(side or "", "") if side else "",
        "date": last_d, "value": last, "pct": round(pct, 1), "zone": zone, "side": side,
        "since": since, "start": pts[0][0], "n": n, "local": local,
        "p5": q(EXTREME_PCT), "p95": q(100 - EXTREME_PCT), "median": q(50),
        "yago": y1[1] if y1 else None, "spark": spark,
    }


# ══ What is special now, and how much it matters ════════════════════════════
# "Special" = a move far bigger than usual over the last day, week or month (measured against that
# indicator's own typical moves over the past ~3 years), or a place at the edge of its history.
# Rank = how special × how much it matters for stock investing (WEIGHT, 1–5).
WEIGHT = {
    "SPX": 5, "VIX": 5, "US10Y": 5, "MOOD": 4, "IXIC": 4, "DJI": 3, "RUT": 3, "VOLRATIO": 3,
    "US2Y": 4, "US1Y": 3, "CURVE": 4, "MORT30": 2, "FEDFUNDS": 4, "DXY": 4, "USCPI": 4, "REAL10": 4, "HYSPREAD": 4.5,
    "GOLD": 3, "SILVER": 2, "COPPER": 3.5, "CUAU": 3.5, "GSR": 2, "WTI": 3.5, "BRENT": 3, "NATGAS": 2,
    "USDJPY": 4.5, "GAP": 4, "JP10Y": 3.5, "BOJRATE": 3, "JPCPI": 3, "NIKKEI200": 3,
    "NFCI": 3.5, "SAHM": 4, "UMCSENT": 2.5, "CCC": 4, "ICSA": 3.5, "SLOOS_CARD": 3, "SLOOS_BUS": 3, "STLFSI": 3,
    "BUFFETT": 3.5, "CAPE": 3.5, "ERP": 3.5, "SPX200": 3,
}
SPECIAL_Z = 2.0
# Is a rise good (+1) or bad (-1) for stock investors? 0 = no clear good or bad (shown in white, never green/red).
GOOD = {"SPX": 1, "IXIC": 1, "DJI": 1, "RUT": 1, "VIX": -1, "VOLRATIO": -1, "HYSPREAD": -1, "MORT30": -1, "REAL10": -1, "USCPI": -1,
        "NFCI": -1, "SAHM": -1, "UMCSENT": 1, "CCC": -1, "ICSA": -1, "SLOOS_CARD": -1, "SLOOS_BUS": -1, "STLFSI": -1}
HORIZON = {"daily": ("today", "this week", "this month"), "weekly": ("this week", "this week", "this month"),
           "monthly": ("this month", None, None), "quarterly": ("this quarter", None, None)}
STEPS = {"daily": (1, 5, 21), "weekly": (1, 1, 4), "monthly": (1, None, None), "quarterly": (1, None, None)}

# One plain-language line per indicator: what it says.
SAYS = {
    "MOOD": "Are investors scared or greedy? Very low = panic selling, often near market bottoms. Very high = euphoria, which is when risk is highest.",
    "VOLRATIO": "How much more nervous options traders are than over the last 50 days. A jump above +30% usually comes with sharp sell-offs.",
    "SPX": "The market's overall direction. A fall of more than 10% from the high is a correction, more than 20% a bear market.",
    "IXIC": "Technology and growth stocks. Very sensitive to interest rates: when yields rise, the Nasdaq usually suffers more than the Dow.",
    "DJI": "30 big, established companies — the 'old economy'. Against the Nasdaq it shows whether investors prefer growth or safety.",
    "RUT": "Small US companies. They borrow more and depend on the home economy, so they lead when investors expect growth and lower rates, and lag when credit worries grow.",
    "VIX": "The market's expected swing over the next month. Under 15 = complacent, above 30 = fear.",
    "US1Y": "The shortest-horizon bond view of the Fed: where the market expects the Fed's rate to be within about a year.",
    "US2Y": "What bond investors expect the Fed to do over the next two years. Moves first when expectations about the Fed change.",
    "US10Y": "The benchmark long-term interest rate. It sets mortgage rates and the 'safe return' stocks must beat. Fast rises often hurt stocks.",
    "CURVE": "Long-term minus short-term yield. When it is negative ('inverted'), a recession has usually followed within 6–24 months.",
    "MORT30": "What a home buyer pays. It follows the 10-year yield with a delay of days to weeks; high rates cool the housing market.",
    "HYSPREAD": "The extra interest risky companies pay over the government. Widening = lenders are getting scared, usually before or during stock falls.",
    "REAL10": "The 10-year yield after inflation — the true price of money. Gold and growth stocks struggle when it rises, and do well when it falls.",
    "FEDFUNDS": "The overnight rate the Federal Reserve steers. Every other US interest rate starts here.",
    "USCPI": "US inflation over 12 months. The higher it stays above 2%, the longer the Fed keeps rates high.",
    "DXY": "The dollar against six major currencies. A strong dollar squeezes exporters and emerging markets and pushes commodity prices down; a weak dollar does the reverse.",
    "WTI": "US oil. Feeds into fuel costs, inflation and energy-company profits. Spikes squeeze consumers; collapses signal weak demand.",
    "BRENT": "The world oil benchmark. Compared with WTI it shows whether a move is American or global.",
    "NATGAS": "US natural gas: very weather-driven. Matters for utility bills, power prices and chemical makers.",
    "GOLD": "The safe-haven metal. It pays no interest, so it does best when interest rates after inflation fall, the dollar weakens, or investors are afraid. That is why it is shown next to rates and the dollar.",
    "SILVER": "Half precious metal, half industrial metal. Moves with gold in fear and with copper in growth, and usually swings more than both.",
    "COPPER": "'Dr. Copper': it goes into almost everything that is built or manufactured (wiring, pipes, cars, electronics), so its price is read as a health check on the world economy. Rising = growth expected, falling = slowdown.",
    "USDJPY": "Yen per dollar. Near 160 Japan has stepped in to stop the yen's fall; a sudden jump in the yen is what blows up 'carry trades' and sells off stocks worldwide.",
    "JP10Y": "Japan's long-term interest rate. When it rises, Japanese investors have a reason to bring money home, which strengthens the yen.",
    "BOJRATE": "The rate the Bank of Japan targets. Each increase narrows the gap to the US and supports the yen.",
    "JPCPI": "Japan's inflation. Sustained inflation near 2% lets the Bank of Japan keep raising rates, which tends to strengthen the yen.",
}
# Groups that belong together: the main members, plus "context" members that are always shown
# alongside because they explain them (gold is read against real yields and the dollar, etc.).
CLUSTERS = [
    dict(id="rates", title="US bonds, rates & the dollar",
         intro="Interest rates and the dollar are the weather for every other market: they move together and explain each other.",
         main=["US10Y", "US2Y", "US1Y", "CURVE", "MORT30", "FEDFUNDS", "DXY", "REAL10", "USCPI"], context=[]),
    dict(id="metals", title="Gold, silver & Dr. Copper",
         intro="Gold, silver and copper move with interest rates and the dollar. Tap the ? on any card to see exactly how.",
         main=["GOLD", "SILVER", "COPPER", "CUAU", "GSR"], context=["REAL10", "US10Y", "DXY"]),
    dict(id="stocks", title="Stocks & investor mood",
         intro="The big US stock indexes and how scared investors are. Tap the ? on a card to see what usually drives it.",
         main=["SPX", "IXIC", "DJI", "RUT", "VIX", "MOOD", "VOLRATIO"], context=["US10Y", "HYSPREAD"]),
    dict(id="japan", title="Yen, Japan & the carry trade",
         intro="Traders borrow cheap yen to buy higher-yielding assets. When the yen jumps, they must buy yen back, selling stocks everywhere.",
         main=["USDJPY", "GAP", "JP10Y", "BOJRATE", "JPCPI", "NIKKEI200"], context=["US10Y", "US2Y"]),
    dict(id="credit", title="Credit, jobs & defaults ahead",
         intro="Will borrowers keep paying their loans? These react before defaults appear in bank statistics: the weakest bonds, layoffs, and banks tightening their lending.",
         main=["HYSPREAD", "CCC", "ICSA", "SLOOS_CARD", "SLOOS_BUS", "STLFSI", "NFCI", "SAHM", "UMCSENT"], context=["US10Y", "CURVE"]),
    dict(id="energy", title="Oil & energy",
         intro="Oil drives inflation and, through inflation, interest rates; the dollar and oil usually move in opposite directions.",
         main=["WTI", "BRENT", "NATGAS"], context=["DXY", "USCPI"]),
    dict(id="value", title="Valuation: how expensive is the market?",
         intro="These change slowly. At extremes they say little about next week and a lot about the next ten years.",
         main=["BUFFETT", "CAPE", "ERP", "SPX200"], context=["US10Y", "REAL10"]),
]


def _kchange(chg, new, old):
    if old is None or new is None:
        return None
    return (new / old - 1) * 100 if chg == "pct" else new - old


def moves(pts, freq, chg):
    """Change over the last day/week/month (observations, so 1/5/21 for daily series) and how many
    times larger than a typical such change it is (typical = root-mean-square of the past ~3 years)."""
    out = {"d1": None, "w1": None, "m1": None, "z1": None, "zw": None, "zm": None}
    if not pts or len(pts) < 30:
        return out
    vals = [v for _, v in pts]
    n = len(vals)
    for name, zname, k in zip(("d1", "w1", "m1"), ("z1", "zw", "zm"), STEPS.get(freq, (1, 5, 21))):
        if k is None or n <= k + 30:
            continue
        cur = _kchange(chg, vals[-1], vals[-1 - k])
        out[name] = cur
        lo = max(k, n - 756)
        hist = [_kchange(chg, vals[i], vals[i - k]) for i in range(lo, n - 1)]
        hist = [h for h in hist if h is not None]
        if len(hist) > 40 and cur is not None:
            rms = (sum(h * h for h in hist) / len(hist)) ** 0.5
            if rms > 0:
                out[zname] = cur / rms
    return out


def special(freq, mv, tone=None, edge=False, hub_zone=None, pct=None):
    """(horizon label or None, how special 0..3)."""
    names = HORIZON.get(freq, HORIZON["daily"])
    cands = [(abs(mv[z]), nm) for z, nm in zip(("z1", "zw", "zm"), names) if mv.get(z) is not None and nm]
    best_z, label = max(cands, default=(0.0, None))
    spec = min(3.0, best_z / SPECIAL_Z)
    if best_z < SPECIAL_Z:
        label = None
    if tone == ALERT:
        spec += 0.4
    elif tone == WATCH:
        spec += 0.2
    if edge:
        spec += 0.3
    if hub_zone:
        ext = abs(pct - 50)
        if hub_zone.startswith("extreme"):
            spec = max(spec, 1.2 + min(1.0, (ext - 45) / 5) * 0.8)
            label = label or "at an extreme"
        elif hub_zone.startswith("stretched"):
            spec = max(spec, 0.8)
        else:
            spec = max(spec, ext / 50 * 0.5)
    return label, round(spec, 2)


def fchg(kind, c):
    if c is None:
        return "n/a"
    return f"{c:+.1f}%" if kind == "pct" else f"{c:+.2f} pt"


# ── "What is moving this group?" — rules built from the numbers (not from news) ───────────────
def drivers(cid, items, main=None):
    """items: {key: summary}. Finds the horizon where this group is most unusual, then explains the
    unusual moves with the standard economic links between the members."""
    names = ("today", "this week", "this month")
    zk = ("z1", "zw", "zm")
    score = [sum(max(0.0, abs(it.get(z) or 0) - 1.0) for it in items.values() if it.get("freq") == "daily") for z in zk]
    hi = max(range(3), key=lambda i: score[i]) if any(score) else 0
    hz, zname, cname = names[hi], zk[hi], ("d1", "w1", "m1")[hi]

    def z(k):
        it = items.get(k)
        return (it.get(zname) or 0) if it and it.get("freq") == "daily" else 0

    def sg(k, th=1.0):
        return 0 if abs(z(k)) < th else (1 if z(k) > 0 else -1)

    def mv(k):
        it = items.get(k)
        return f"{it['name']} {fchg(it['chgKind'], it.get(cname))}" if it else ""

    def val(k):
        it = items.get(k)
        return it["value"] if it else None

    out = []
    big = sorted((k for k in items if abs(z(k)) >= SPECIAL_Z and (main is None or k in main)), key=lambda k: -abs(z(k)))
    if cid == "rates":
        if sg("US10Y"):
            up = sg("US10Y") > 0
            out.append(f"Long-term yields {'rose' if up else 'fell'} ({mv('US10Y')} {hz}). " + (
                "Rising yields usually mean investors expect stronger growth or higher inflation, or fewer Fed rate cuts. They push mortgage rates up and make stocks' future profits worth less today."
                if up else "Falling yields usually mean investors expect slower growth, run to safety, or expect more Fed cuts. Mortgage rates follow with a delay; lower yields support stock prices unless fear is the reason."))
        if sg("US2Y") and abs(z("US2Y")) > abs(z("US10Y")):
            out.append("Short-term yields moved more than long-term ones: the market is changing its mind about the Fed's next steps, not about long-run growth.")
        elif sg("US10Y") and not sg("US2Y"):
            out.append("Only long-term yields moved: the cause is inflation, government borrowing or growth expectations, not the Fed's near-term path.")
        if sg("REAL10"):
            out.append(f"The real yield (after inflation) {'rose' if sg('REAL10') > 0 else 'fell'} ({mv('REAL10')}): "
                       + ("money got more expensive in real terms — a headwind for gold and growth stocks." if sg("REAL10") > 0 else "money got cheaper in real terms — a tailwind for gold and growth stocks."))
        if sg("DXY"):
            if sg("US10Y") and sg("DXY") == sg("US10Y"):
                out.append(f"The dollar and yields moved together ({mv('DXY')}): money flows to the US because it pays more.")
            elif sg("US10Y"):
                out.append(f"The dollar moved against yields ({mv('DXY')}): that usually points to fear and safe-haven buying, or news from outside the US, not US interest rates.")
            else:
                out.append(f"The dollar moved on its own ({mv('DXY')}) without a matching move in US yields: look at other countries' rates or at risk appetite.")
        if sg("MORT30"):
            out.append("Mortgage rates moved; they follow the 10-year yield with a lag of days to weeks.")
        cv = val("CURVE")
        if cv is not None and cv < 0:
            out.append(f"The yield curve is still inverted ({cv:+.2f} pt): short rates above long rates, historically a recession warning.")
    elif cid == "metals":
        g, ry, dx = sg("GOLD"), sg("REAL10"), sg("DXY")
        if g:
            word = "rose" if g > 0 else "fell"
            if g > 0 and (ry < 0 or dx < 0):
                out.append(f"Gold {word} ({mv('GOLD')} {hz}) while real yields and/or the dollar fell. Gold pays no interest, so it gains when interest after inflation falls and when the dollar weakens.")
            elif g < 0 and (ry > 0 or dx > 0):
                out.append(f"Gold {word} ({mv('GOLD')} {hz}) while real yields and/or the dollar rose. Gold pays no interest, so higher rates and a stronger dollar are its usual headwinds.")
            elif g > 0 and (ry > 0 or dx > 0):
                out.append(f"Gold {word} ({mv('GOLD')} {hz}) even though real yields and/or the dollar rose. That is unusual: fear, safe-haven or central-bank buying is outweighing interest rates.")
            else:
                out.append(f"Gold {word} ({mv('GOLD')} {hz}) without a clear move in real yields or the dollar: look at fear levels and at what investors are doing, not at interest rates.")
        c, ratio = sg("COPPER"), sg("CUAU")
        if c:
            out.append(f"Copper {'rose' if c > 0 else 'fell'} ({mv('COPPER')}). " + (
                ("Copper outpaced gold: markets are betting on growth (factories, construction, China)." if ratio > 0 else "Copper rose but gold rose as well, so the move is not purely about growth.") if c > 0 else
                ("Copper fell against gold: growth worries. This often comes before weaker stock markets." if ratio < 0 else "Copper fell, but so did gold, so it looks like a broad metals sell-off, often a stronger dollar or rising real yields.")))
        if sg("SILVER") and sg("GOLD") and sg("SILVER") != sg("GOLD"):
            out.append("Silver and gold moved in opposite directions: silver follows industrial demand and speculation, gold follows fear and interest rates.")
        elif sg("SILVER") and abs(z("SILVER")) > abs(z("GOLD")) + 0.5:
            out.append("Silver moved much more than gold: silver adds industrial demand and speculative money on top of gold's drivers, so it exaggerates the move.")
    elif cid == "stocks":
        sp, vx = sg("SPX"), sg("VIX")
        if sp and vx:
            if sp < 0 < vx:
                out.append(f"Stocks fell and the fear gauge rose ({mv('SPX')}, {mv('VIX')} {hz}): a normal risk-off move.")
            elif sp > 0 > vx:
                out.append(f"Stocks rose and the fear gauge fell ({mv('SPX')}, {mv('VIX')} {hz}): a calm rally.")
            elif sp > 0 and vx > 0:
                out.append("Stocks rose while the fear gauge also rose: investors are buying protection into the rally — a nervous rally, which is less stable.")
        elif sp:
            out.append(f"The S&P 500 moved ({mv('SPX')} {hz}) without a matching move in fear: a quiet drift, not a panic.")
        elif vx:
            out.append(f"Fear moved ({mv('VIX')} {hz}) while the S&P 500 barely did: traders are buying or selling protection ahead of events.")
        if sg("IXIC") and sg("DJI") and sg("IXIC") != sg("DJI"):
            out.append("The Nasdaq and the Dow moved in opposite directions: investors are rotating between growth and safety, usually driven by interest-rate expectations.")
        elif sg("IXIC") and abs(z("IXIC")) > abs(z("DJI")) + 0.7:
            out.append("The Nasdaq moved much more than the Dow: growth and technology stocks are reacting to rates or to news about big tech.")
        if sg("RUT") and abs(z("RUT")) > abs(z("SPX")) + 0.7:
            out.append(f"Small companies moved more than the big ones ({mv('RUT')}): they react most to borrowing costs and domestic growth, so it is a read on credit and the economy.")
        if sg("US10Y") and sp and sg("US10Y") != sp and sg("US10Y") > 0:
            out.append("Rising yields and falling stocks together: higher interest rates are the most common cause of broad sell-offs.")
        if sg("HYSPREAD") > 0 and sp < 0:
            out.append("Credit spreads are widening while stocks fall: lenders are nervous too, which makes a sell-off more serious.")
        mp = val("MOOD")
        if mp is not None and (mp < 25 or mp > 75):
            out.append(f"Our mood score is {mp:.0f}: {'extreme fear, which has often come near bottoms' if mp < 25 else 'extreme greed, which is when risk is highest'}.")
    elif cid == "japan":
        yv, nk = sg("USDJPY"), sg("NIKKEI200")
        if yv < 0 and nk < 0:
            out.append("The yen jumped while Japanese stocks fell: this is the pattern of carry-trade unwinding (as in August 2024). Traders who borrowed yen buy it back and sell stocks to do it.")
        elif yv < 0:
            out.append(f"The yen strengthened ({mv('USDJPY')} {hz}). A sudden yen rise is the trigger for carry-trade unwinding; watch Japanese and US stocks.")
        elif yv > 0:
            out.append(f"The yen weakened ({mv('USDJPY')} {hz}). A weaker yen makes the carry trade more profitable, and near 160 it raises the risk that Japan steps in to buy yen.")
        if sg("JP10Y") > 0:
            out.append("Japanese yields rose: the gap to US yields narrows, which supports the yen, and Japanese investors have a reason to bring money home.")
        fx = val("USDJPY")
        if fx is not None and fx >= 155:
            out.append(f"USD/JPY is at {fx:.1f}, close to the 160 level where Japan stepped in during 2024.")
    elif cid == "credit":
        if sg("HYSPREAD"):
            out.append(f"Junk-bond spreads {'widened' if sg('HYSPREAD') > 0 else 'narrowed'} ({mv('HYSPREAD')} {hz}): " + ("lenders are asking more from risky companies — a stress signal that usually leads stock falls." if sg("HYSPREAD") > 0 else "lenders are relaxing — supportive for stocks."))
        if sg("CCC") > 0:
            out.append(f"The weakest borrowers' spread widened ({mv('CCC')}): they default first, so defaults tend to follow within months.")
        elif sg("CCC") < 0:
            out.append(f"The weakest borrowers' spread narrowed ({mv('CCC')}): fear of defaults is easing.")
        if sg("ICSA") > 0:
            out.append(f"Jobless claims rose ({mv('ICSA')}). People who lose their job fall behind on loans a few months later, so rising claims point to more household defaults and weaker spending.")
        elif sg("ICSA") < 0:
            out.append(f"Jobless claims fell ({mv('ICSA')}): fewer layoffs, so household loan defaults should stay low.")
        if sg("NFCI") > 0 or sg("STLFSI") > 0:
            out.append("Financial conditions tightened: credit is getting harder and more expensive to get.")
        sc, sb = val("SLOOS_CARD"), val("SLOOS_BUS")
        if sc is not None and sc > 20:
            out.append(f"{sc:.0f}% net of banks are tightening credit-card lending (quarterly survey): they see their customers struggle, and spending usually slows afterwards.")
        if sb is not None and sb > 20:
            out.append(f"{sb:.0f}% net of banks are tightening business lending (quarterly survey): historically a reliable warning of a slowdown.")
    elif cid == "energy":
        w = sg("WTI") or sg("BRENT")
        if w:
            out.append(f"Oil {'rose' if w > 0 else 'fell'} ({mv('WTI')} {hz}). " + ("Higher oil feeds inflation, which keeps interest rates high — bad for stocks overall, good for energy companies." if w > 0 else "Falling oil eases inflation, but a big drop usually means demand is weakening."))
        if w and sg("DXY") and sg("DXY") != w:
            out.append("The dollar moved the opposite way, as usual: oil is priced in dollars, so a stronger dollar pushes it down.")
        if sg("NATGAS"):
            out.append(f"Natural gas moved ({mv('NATGAS')}): mostly weather and storage, rarely an economy-wide signal.")
    elif cid == "value":
        out.append("Valuation barely changes from day to day. An extreme level describes long-run risk and expected returns over years, not a reason to act this week.")
        if sg("SPX200"):
            out.append("The S&P 500 moved a lot against its trend: that is the quickest-moving valuation signal here.")
    if not out:
        out.append("Nothing unusual in this group: moves are within their normal range.")
    if big:
        head = "Most unusual " + hz + ": " + ", ".join(items[k]["name"] for k in big[:3]) + "."
    else:
        head = "Quiet — no unusual move " + hz + "."
    return {"headline": head, "horizon": hz, "bullets": out[:6]}


# ══ How the indicators connect ("?" on a card) ═════════════════════════════════════════════════
# (driver, affected, usual direction, why) — "same" = they usually move together, "inverse" = opposite.
RELATIONS = [
    ("US10Y", "MORT30", "same", "Mortgage rates are priced off the 10-year Treasury yield, so they follow it with a delay of days to weeks."),
    ("US2Y", "US10Y", "same", "Both react to what investors expect from the Fed. The 2-year moves first and most when those expectations change."),
    ("US1Y", "US2Y", "same", "Same story over a shorter horizon: both price the Fed's path."),
    ("FEDFUNDS", "US2Y", "same", "The 2-year yield is mostly the market's guess of the Fed's rate, so it moves with the Fed's actual and expected decisions."),
    ("USCPI", "FEDFUNDS", "same", "Higher inflation makes the Fed hold rates high or raise them; lower inflation lets it cut."),
    ("USCPI", "US10Y", "same", "Higher inflation makes investors demand higher long-term yields to protect their purchasing power."),
    ("US10Y", "REAL10", "same", "The real yield is the 10-year yield minus expected inflation, so it moves mostly with the 10-year yield."),
    ("US10Y", "CURVE", "same", "The curve is the 10-year yield minus the 2-year yield, so the 10-year raises it."),
    ("US2Y", "CURVE", "inverse", "The curve is the 10-year yield minus the 2-year yield, so a higher 2-year yield lowers it."),
    ("US10Y", "DXY", "same", "Higher US yields attract foreign money into dollars, which lifts the dollar."),
    ("REAL10", "GOLD", "inverse", "Gold pays no interest. When yields after inflation rise, holding gold costs investors more in lost interest, so gold usually falls; when they fall, gold usually rises."),
    ("DXY", "GOLD", "inverse", "Gold is priced in dollars: a stronger dollar makes it dearer for buyers elsewhere, which usually pushes the gold price down."),
    ("DXY", "SILVER", "inverse", "Silver is priced in dollars: a stronger dollar makes it dearer for buyers elsewhere and usually pushes the price down."),
    ("DXY", "COPPER", "inverse", "Copper is priced in dollars: a stronger dollar makes it dearer for buyers elsewhere and usually pushes the price down."),
    ("DXY", "WTI", "inverse", "Oil is priced in dollars: a stronger dollar makes it dearer for buyers elsewhere and usually pushes the price down."),
    ("VIX", "GOLD", "same", "In panics investors buy gold as a safe haven, so a jump in fear often lifts gold — unless rising yields or a stronger dollar outweigh it."),
    ("GOLD", "SILVER", "same", "Silver follows gold in fear and inflation scares, but swings more."),
    ("GOLD", "CUAU", "inverse", "Gold is the bottom of the copper ÷ gold ratio: when gold rises faster than copper, the ratio falls."),
    ("COPPER", "CUAU", "same", "Copper is the top of the copper ÷ gold ratio: when copper rises faster than gold, the ratio rises."),
    ("GOLD", "GSR", "same", "Gold is the top of the gold ÷ silver ratio: when gold rises faster than silver, the ratio rises."),
    ("SILVER", "GSR", "inverse", "Silver is the bottom of the gold ÷ silver ratio: when silver rises faster than gold, the ratio falls."),
    ("COPPER", "SPX", "same", "Copper tracks manufacturing and construction, so it tends to rise and fall with the economy and with stocks."),
    ("WTI", "USCPI", "same", "Oil feeds into fuel and transport costs, so higher oil lifts inflation a few weeks later."),
    ("BRENT", "WTI", "same", "World and US oil prices move together; a gap points to a local supply problem."),
    ("WTI", "SPX", "inverse", "Higher oil raises costs for companies and households but lifts energy stocks. In a sharp rise the effect on the whole market is usually negative."),
    ("US10Y", "SPX", "inverse", "Higher yields make bonds a better alternative and cut the value of future profits, so rising yields are the most common cause of broad stock sell-offs."),
    ("US10Y", "IXIC", "inverse", "Growth and tech companies earn most of their profit far in the future, so they are hit hardest by higher yields."),
    ("VIX", "SPX", "inverse", "The VIX measures fear of falling stocks, so it rises when the S&P 500 falls and falls when it rises."),
    ("SPX", "VIX", "inverse", "Falling stocks make investors buy protection, which raises the VIX; rising stocks lower it."),
    ("HYSPREAD", "SPX", "inverse", "When lenders demand more from risky companies, it signals stress that usually comes with falling stocks."),
    ("HYSPREAD", "RUT", "inverse", "Small companies borrow more, so credit stress hits them hardest."),
    ("CCC", "HYSPREAD", "same", "The weakest borrowers' spread is the earliest part of the junk-bond spread to move."),
    ("ICSA", "SAHM", "same", "More new jobless claims lead to a higher unemployment rate, which is what the Sahm rule measures."),
    ("SLOOS_BUS", "SAHM", "same", "When banks tighten business lending, companies hire and invest less, so unemployment rises months later."),
    ("UMCSENT", "SPX", "same", "Households' mood drives spending (about 70% of the US economy), and stocks tend to follow confidence."),
    ("NFCI", "SPX", "inverse", "Tighter financial conditions mean harder, costlier credit, which weighs on stocks and growth."),
    ("STLFSI", "VIX", "same", "Both measure market stress; the stress index adds bond and credit stress."),
    ("USDJPY", "NIKKEI200", "same", "A weaker yen lifts Japanese exporters' profits and the Nikkei; a sudden yen jump hits it."),
    ("US10Y", "GAP", "same", "The US 10-year yield is the top of the US–Japan gap: a higher US yield widens it."),
    ("JP10Y", "GAP", "inverse", "The Japanese 10-year yield is the bottom of the gap: a higher one narrows it."),
    ("GAP", "USDJPY", "same", "A wider gap pays more for holding dollars than yen, which pushes USD/JPY up."),
    ("BOJRATE", "USDJPY", "inverse", "When the Bank of Japan raises rates, holding yen pays more, which tends to strengthen the yen (USD/JPY falls)."),
    ("JPCPI", "BOJRATE", "same", "Sustained inflation lets the Bank of Japan keep raising rates."),
    ("USDJPY", "SPX", "inverse", "A sudden yen jump (USD/JPY falling) can force carry-trade investors to sell stocks to repay yen loans, as in August 2024."),
    ("VIX", "MOOD", "inverse", "Our fear & greed score includes the VIX: more fear lowers the score."),
    ("SPX", "MOOD", "same", "Stock momentum is one of the score's four ingredients."),
    ("HYSPREAD", "MOOD", "inverse", "Junk-bond demand is one of the score's four ingredients: wider spreads lower the score."),
    ("SPX", "SPX200", "same", "The S&P 500's distance from its 200-day average moves with the index."),
    ("SPX", "CAPE", "same", "The Shiller P/E is the S&P 500's price over its 10-year average earnings: rising prices lift it."),
    ("SPX", "BUFFETT", "same", "Between quarterly releases the Buffett indicator is moved forward with the S&P 500."),
    ("CAPE", "ERP", "inverse", "The stock reward over bonds is 100 ÷ the Shiller P/E minus the real yield: a more expensive market lowers it."),
    ("REAL10", "ERP", "inverse", "The stock reward over bonds is the earnings yield minus the real yield: a higher real yield lowers it."),
]
_MODE_WORDS = {"same": "usually move together", "inverse": "usually move in opposite directions"}


def explain(key, items):
    """What explains this indicator and what does it move? Each link says how the two usually behave
    and whether they are behaving that way right now. items: {key: summary}."""
    me = items.get(key)
    if not me:
        return None
    names = ("today", "this week", "this month")
    zk, ck = ("z1", "zw", "zm"), ("d1", "w1", "m1")

    def link(driver, affected, mode, why):
        d, a = items.get(driver), items.get(affected)
        if not d or not a:
            return None
        score = [max(abs(d.get(z) or 0), abs(a.get(z) or 0)) for z in zk]
        hi = max(range(3), key=lambda i: score[i]) if any(score) else 0
        zd, za = (d.get(zk[hi]) or 0), (a.get(zk[hi]) or 0)
        hz = names[hi]
        cd, ca = fchg(d["chgKind"], d.get(ck[hi])), fchg(a["chgKind"], a.get(ck[hi]))
        if abs(zd) < 1 and abs(za) < 1:
            status, note = "quiet", f"Neither moved much {hz}."
        elif abs(zd) >= 1 and abs(za) < 1:
            status, note = "noreact", f"{d['name']} moved ({cd} {hz}) but {a['name']} has not reacted (yet)."
        elif abs(zd) < 1:
            status, note = "other", f"{a['name']} moved ({ca} {hz}) without a matching move in {d['name']}: look for another cause."
        else:
            expected = (1 if zd > 0 else -1) * (1 if mode == "same" else -1)
            if (1 if za > 0 else -1) == expected:
                status, note = "asusual", f"{d['name']} {cd}, {a['name']} {ca} {hz}: the usual link is working."
            else:
                status, note = "against", f"{d['name']} {cd}, {a['name']} {ca} {hz}: moving against the usual link — something else is driving it."
        return {"driver": driver, "affected": affected, "mode": mode, "modeText": _MODE_WORDS[mode], "why": why,
                "status": status, "note": note, "horizon": hz}

    drivers = [x for x in (link(d, a, m, w) for d, a, m, w in RELATIONS if a == key) if x]
    effects = [x for x in (link(d, a, m, w) for d, a, m, w in RELATIONS if d == key) if x]
    order = {"against": 0, "other": 1, "noreact": 2, "asusual": 3, "quiet": 4}
    drivers.sort(key=lambda x: order[x["status"]])
    effects.sort(key=lambda x: order[x["status"]])
    return {"key": key, "drivers": drivers, "effects": effects}


# ══ News ═══════════════════════════════════════════════════════════════════════════════════════
# Free RSS feeds, no keys. Each headline is matched to the indicators it is about.
NEWS_FEEDS = [
    ("CNBC", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("CNBC Finance", "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ("CNBC Investing", "https://www.cnbc.com/id/15839069/device/rss/rss.html"),
    ("CNBC Energy", "https://www.cnbc.com/id/19836768/device/rss/rss.html"),
    ("CNBC Asia", "https://www.cnbc.com/id/19832390/device/rss/rss.html"),
    ("MarketWatch", "http://feeds.marketwatch.com/marketwatch/topstories/"),
    ("MarketWatch Pulse", "http://feeds.marketwatch.com/marketwatch/marketpulse/"),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
    ("Federal Reserve", "https://www.federalreserve.gov/feeds/press_all.xml"),
]
NEWS_TOPIC_QUERIES = ["Treasury yields", "Federal Reserve interest rates", "gold price", "silver price", "copper price",
                      "oil prices OPEC", "US dollar index", "Japanese yen Bank of Japan", "Wall Street stocks S&P 500",
                      "jobless claims unemployment", "inflation CPI", "junk bond credit spreads", "mortgage rates",
                      "Nikkei Japan stocks", "VIX volatility", "natural gas prices", "consumer sentiment",
                      "bank lending standards", "Nasdaq tech stocks", "Russell 2000 small caps"]


def news_feeds():
    out = list(NEWS_FEEDS)
    for q in NEWS_TOPIC_QUERIES:
        out.append(("Google News", "https://news.google.com/rss/search?q=" + q.replace(" ", "+") + "+when:2d&hl=en-US&gl=US&ceid=US:en"))
    return out


NEWS_TOPICS = {
    "US10Y": ["10-year", "10 year", "treasury yield", "treasury yields", "treasuries", "bond yield", "bond yields", "bond market"],
    "US2Y": ["2-year yield", "two-year yield", "2-year treasury", "two-year treasury"],
    "US1Y": ["1-year yield", "one-year yield"],
    "FEDFUNDS": ["federal reserve", "the fed", "fomc", "rate cut", "rate cuts", "rate hike", "rate hikes", "powell", "interest rate", "interest rates", "fed funds"],
    "MORT30": ["mortgage rate", "mortgage rates", "mortgage"],
    "CURVE": ["yield curve", "inverted"],
    "DXY": ["dollar index", "us dollar", "the dollar", "strong dollar", "weak dollar", "greenback", "dollar rally", "dollar slump", "dollar falls", "dollar rises"],
    "REAL10": ["real yield", "real yields", "tips yield"],
    "USCPI": ["inflation", "consumer price", "cpi", "pce"],
    "GOLD": ["gold", "bullion"],
    "SILVER": ["silver price", "silver prices", "silver futures", "silver hits", "silver rises", "silver falls"],
    "COPPER": ["copper"],
    "WTI": ["oil price", "oil prices", "crude oil", "crude", "wti", "opec", "oil futures", "oil rises", "oil falls", "oil jumps",
            "oil slides", "oil drops", "oil climbs", "oil settles", "oil gains", "oil tumbles", "us oil"],
    "BRENT": ["brent"],
    "NATGAS": ["natural gas", "nat gas", "lng"],
    "SPX": ["s&p 500", "stock market", "wall street", "stocks"],
    "IXIC": ["nasdaq", "tech stocks", "big tech", "tech sell"],
    "DJI": ["dow jones", "the dow"],
    "RUT": ["russell 2000", "small caps", "small-cap", "small cap"],
    "VIX": ["vix", "volatility index", "fear gauge"],
    "HYSPREAD": ["junk bond", "junk bonds", "high-yield", "high yield bond", "credit spread", "credit spreads", "credit stress"],
    "CCC": ["distressed debt", "ccc-rated", "defaults rise", "default rate", "private credit"],
    "ICSA": ["jobless claims", "unemployment claims", "initial claims", "layoffs"],
    "SAHM": ["unemployment rate", "jobs report", "nonfarm payrolls", "payrolls", "recession"],
    "UMCSENT": ["consumer sentiment", "consumer confidence"],
    "NFCI": ["financial conditions"], "STLFSI": ["financial stress"],
    "SLOOS_CARD": ["lending standards", "senior loan officer", "credit card delinquencies", "credit card debt"],
    "SLOOS_BUS": ["lending standards", "senior loan officer", "bank lending"],
    "USDJPY": ["yen", "usd/jpy", "carry trade"],
    "JP10Y": ["jgb", "japanese government bond", "japan bond yield", "japanese bond"],
    "BOJRATE": ["bank of japan", "boj", "ueda"],
    "JPCPI": ["japan inflation", "japan's inflation", "japanese inflation"],
    "NIKKEI200": ["nikkei", "tokyo stocks", "japanese stocks"],
    "MOOD": ["fear and greed", "fear & greed"],
    "CAPE": ["valuation", "valuations", "overvalued", "stock bubble", "ai bubble", "shiller"],
    "BUFFETT": ["buffett indicator", "warren buffett"],
}
_NEWS_NEG = {"GOLD": re.compile(r"gold(?:en)?\s+(?:medal|cup|coast|state|rush|globe)|olympic|goldman|golden", re.I)}
_NEWS_RX = {k: re.compile(r"(?<![a-z0-9])(?:" + "|".join(re.escape(p) for p in v) + r")(?![a-z0-9])", re.I) for k, v in NEWS_TOPICS.items()}


def tag_news(title, summary=""):
    """Indicator keys a headline is about (a match in the title counts, one only in the summary counts less)."""
    hits = []
    for k, rx in _NEWS_RX.items():
        if k in _NEWS_NEG and _NEWS_NEG[k].search(title):
            continue
        if rx.search(title):
            hits.append((k, 2))
        elif summary and rx.search(summary):
            hits.append((k, 1))
    return hits


def _clean_text(s):
    import html as _h
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = _h.unescape(s).replace("\xa0", " ")
    return re.sub(r"\s+", " ", s).strip()


def first_sentence(text, title="", limit=240):
    t = _clean_text(text)
    if not t or t.lower().startswith(title.lower()[:40]) and len(t) < len(title) + 40:
        return ""
    m = re.match(r"(.{30,}?[.!?])(\s|$)", t)
    s = m.group(1) if m else t
    return s if len(s) <= limit else s[:limit].rsplit(" ", 1)[0] + "…"


def parse_feed(xml_bytes, default_source=""):
    """RSS 2.0 (and simple Atom) → [{title, link, summary, ts, source}]."""
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    root = ET.fromstring(xml_bytes)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    items = root.findall(".//item") or root.findall(".//a:entry", ns)
    out = []
    for it in items:
        def g(tag, atom=None):
            e = it.find(tag)
            if e is None and atom:
                e = it.find(atom, ns)
            return (e.text or "").strip() if e is not None and e.text else ""
        title = _clean_text(g("title", "a:title"))
        link = g("link")
        if not link:
            le = it.find("a:link", ns)
            link = le.get("href", "") if le is not None else ""
        if not title or not link:
            continue
        raw = g("pubDate") or g("published", "a:published") or g("updated", "a:updated")
        ts = None
        try:
            ts = int(parsedate_to_datetime(raw).timestamp())
        except Exception:
            try:
                ts = int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp())
            except Exception:
                ts = None
        src = it.find("source")
        source = (src.text or "").strip() if src is not None and src.text else default_source
        if default_source == "Google News":
            m = re.search(r"\s[-–]\s([^-–]+)$", title)          # "Headline - Publisher"
            if m and not source:
                source = m.group(1).strip()
            if m:
                title = title[:m.start()].strip()
        summary = first_sentence(g("description", "a:summary") or g("{http://purl.org/rss/1.0/modules/content/}encoded"), title)
        out.append({"title": title, "link": link, "summary": summary, "ts": ts, "source": source or default_source})
    return out
