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
import json
import time
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
    ("internals", "Market internals", "What is under the index: how many sectors move, the AI builders, banks and the volatility curve."),
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
# ── Market internals: what is under the index (sector ETFs and big AI names are helpers; the rest is calculated from them)
_SECTORS = [("XLK", "Technology"), ("XLC", "Communication services"), ("XLY", "Consumer discretionary"), ("XLF", "Financials"), ("XLI", "Industrials"),
            ("XLV", "Health care"), ("XLP", "Consumer staples"), ("XLU", "Utilities"), ("XLRE", "Real estate"), ("XLE", "Energy"), ("XLB", "Materials")]
AI_BUILDER_SECTORS = {"XLK", "XLC", "XLY"}
AI_BUILDERS = ["NVDA", "MSFT", "GOOGL", "META", "AMZN", "AVGO"]
for _k, _n in _SECTORS:
    INDICATORS.append(dict(key=_k, name=_n + " sector", group=("internals" if _k == "XLF" else None), source="yahoo", sym=_k, unit="$", dec=2, kind="range", chg="pct",
                           freq="daily", money=True, explain=f"The {_n.lower()} companies of the S&P 500 as one fund."))
INDICATORS[-11 + [k for k, _ in _SECTORS].index("XLF")]["name"] = "Banks & financials (XLF)"
INDICATORS += [dict(key=k, name=n, group=g, source="yahoo", sym=sym, unit=u, dec=2, kind="range", chg="pct", freq="daily", money=m, explain=ex)
               for k, n, g, sym, u, m, ex in [
    ("KRE", "Regional banks (KRE)", "internals", "KRE", "$", True, "A fund of smaller US banks. They are the most exposed to the economy and to credit losses."),
    ("RSP", "S&P 500, equal weight", None, "RSP", "$", True, "The S&P 500 with every company counted the same."),
    ("SPY", "S&P 500, by company size", None, "SPY", "$", True, "The S&P 500 as usual: the biggest companies count the most."),
    ("VIX3M", "VIX, 3 months", None, "^VIX3M", "", False, "Expected swings over the next 3 months."),
    ("VVIX", "VVIX (volatility of the VIX)", "internals", "^VVIX", "", False, "How much options traders expect the VIX itself to jump."),
] + [(t, t, None, t, "$", True, "A big AI builder.") for t in AI_BUILDERS]]
INDICATORS += [
    dict(key="SECT_UP", name="Sectors up today (of 11)", group="internals", source="calc", unit="", dec=0, kind="level", chg="pt", freq="daily",
         bands=[(4, "Broad sell-off: most sectors down", ALERT), (8, "Mixed day", CALM), (1e9, "Broad rally: most sectors up", CALM)],
         explain="How many of the 11 S&P 500 sectors rose today."),
    dict(key="AIB", name="AI builders (average)", group="internals", source="calc", unit="", dec=1, kind="range", chg="pct", freq="daily", explain="Nvidia, Microsoft, Alphabet, Meta, Amazon, Broadcom."),
    dict(key="AIU", name="AI users: banks & industrials (average)", group="internals", source="calc", unit="", dec=1, kind="range", chg="pct", freq="daily", explain="Financials and industrials."),
    dict(key="AIB_AIU", name="AI builders vs AI users", group="internals", source="calc", unit="", dec=1, kind="range", chg="pct", freq="daily", explain="AI builders divided by AI users."),
    dict(key="DEFN", name="Defensive sectors (average)", group="internals", source="calc", unit="", dec=1, kind="range", chg="pct", freq="daily", explain="Health care, staples, utilities, real estate."),
    dict(key="BREADTH", name="Breadth: equal weight vs size weight", group="internals", source="calc", unit="", dec=1, kind="range", chg="pct", freq="daily", explain="Equal-weight S&P 500 divided by the normal one."),
    dict(key="VIXTS", name="VIX ÷ 3-month VIX", group="internals", source="calc", unit="", dec=2, kind="level", chg="pt", freq="daily",
         bands=[(0.9, "Calm: near-term fear below 3-month fear", CALM), (1.0, "Normal", CALM), (1e9, "Inverted: fear now is above fear later", ALERT)], explain="Short-term fear divided by 3-month fear."),
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
    ("yen", "Yen carry trade", "Traders borrow cheap yen to buy higher-yielding assets. It only matters because a sudden jump in the yen forces them to sell those assets, stocks included."),
    ("value", "Valuation", "How expensive the US stock market is against the economy, its own earnings and its own trend."),
    ("credit", "Credit & economy", "Bond-market and economic warning lights that rarely matter until they are far from normal."),
    ("consumer", "Consumers & prices", "What households earn, spend and borrow, and how fast prices rise: the demand side of the economy."),
    ("jobs", "Jobs & growth", "Employment, factories, housing and overall growth: whether the economy is expanding or slowing."),
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
         freq="weekly", sides="high",
         why="How many Americans filed for unemployment benefits for the first time last week, published every Thursday. People who lose their job fall behind on their loans a few months later, so claims lead household loan defaults.",
         high="Layoffs are rising fast. Household incomes — and soon their ability to pay loans — are under pressure, which usually precedes more defaults and weaker spending.",
         low=""),
    dict(key="SLOOS_CARD", name="Banks tightening credit-card lending", cat="credit", kind="fred", sym="DRTSCLCC", start="1990-01-01",
         unit="%", dec=0, freq="quarterly", signed=True,
         why="Share of US banks tightening their standards for credit cards, minus the share loosening (Federal Reserve survey of loan officers, quarterly). Banks see their customers' problems first and react by lending less — which slows spending and often comes before more defaults.",
         high="Many banks are making it harder to borrow on cards: they expect more households to struggle. Spending usually slows afterwards.",
         low="Banks are loosening card lending. This is a quarterly survey that lags market stress by a few months, so read it next to the faster signals (junk-bond spreads, jobless claims)."),
    dict(key="SLOOS_BUS", name="Banks tightening business lending", cat="credit", kind="fred", sym="DRTSCILM", start="1990-01-01",
         unit="%", dec=0, freq="quarterly", signed=True,
         why="Share of US banks tightening their standards for loans to medium and large companies, minus the share loosening (Federal Reserve survey, quarterly). Tighter lending starves companies of money, so it leads slower hiring and investment, and often recessions.",
         high="Banks are clamping down on business loans — historically a reliable warning that a slowdown or recession is coming.",
         low="Banks are loosening business lending. This is a quarterly survey that lags market stress by a few months, so read it next to the faster signals (junk-bond spreads, financial stress)."),
    dict(key="STLFSI", name="Financial stress (St. Louis Fed)", cat="credit", kind="fred", sym="STLFSI4", start="1993-01-01", unit="", dec=2,
         freq="weekly", signed=True,
         why="Index of 18 weekly market measures (interest rates, spreads, volatility). Zero is average stress; above zero is more stress than usual.",
         high="Financial markets are under unusual stress — typical of crises, when borrowers struggle and lenders pull back.",
         low="Very little stress in the financial system: money is easy and conditions are calm."),
    dict(key="INCOME", name="Household income after inflation (yearly change)", cat="consumer", kind="fred_yoy", sym="DSPIC96", start="1958-01-01",
         unit="%", dec=1, freq="monthly", signed=True,
         why="How much more (or less) US households can spend than a year ago, after taxes and inflation (Bureau of Economic Analysis, monthly, published about a month late). This is the money that actually pays the loans, so it is the base under household credit.",
         high="Real incomes are growing fast: households can carry their debts and keep spending, which supports the economy.",
         low="Real incomes are shrinking: budgets are squeezed, so spending slows and loans become harder to pay. Defaults tend to follow with a delay."),
    dict(key="SAVE", name="Personal saving rate", cat="consumer", kind="fred", sym="PSAVERT", start="1959-01-01", unit="%", dec=1, freq="monthly",
         why="Share of their income US households put aside instead of spending (monthly). It is the cushion between a bad month and missed payments.",
         high="Households are saving a lot: a thick cushion for bad times — but they are also spending less, which slows growth.",
         low="The cushion is thin: households spend almost everything they earn, so a job loss or a price jump quickly turns into missed payments."),
    dict(key="DEBTSERV", name="Household debt payments (share of income)", cat="consumer", kind="fred", sym="TDSP", start="1980-01-01", unit="%", dec=1,
         freq="quarterly",
         why="Share of disposable income that US households spend on debt payments (mortgages, cards, car loans), quarterly. The heavier it is, the less room there is for surprises.",
         high="Debt payments take an unusually large share of income: households have little room, which is when defaults start to rise.",
         low="Debt payments are light compared with income: households can carry their loans comfortably."),
    dict(key="WAGES", name="Wage growth (average hourly earnings, yearly)", cat="consumer", kind="fred_yoy", sym="CES0500000003", start="2006-01-01",
         unit="%", dec=1, freq="monthly", signed=True,
         why="How much pay per hour grew over the last year (monthly). Wages are the biggest part of household income.",
         high="Pay is rising fast: good for households' budgets, but also a source of inflation, which keeps interest rates high.",
         low="Pay growth is weak: household budgets get no help against rising prices."),
    dict(key="CORECPI", name="Core inflation (prices without food & energy, yearly)", cat="consumer", kind="fred_yoy", sym="CPILFESL", start="1958-01-01",
         unit="%", dec=1, freq="monthly", signed=True,
         why="How much consumer prices rose over a year, leaving out food and energy because they jump around. It shows the stubborn part of inflation that the Fed watches most closely.",
         high="Underlying inflation is high: the Fed keeps interest rates high for longer, which weighs on stocks and bonds.",
         low="Underlying inflation is low: the Fed has room to cut interest rates."),
    dict(key="PCEPI", name="PCE inflation (the Fed's favourite measure, yearly)", cat="consumer", kind="fred_yoy", sym="PCEPI", start="1960-01-01",
         unit="%", dec=1, freq="monthly", signed=True,
         why="The price measure the Federal Reserve targets (2%). It tracks what households actually buy, so it moves a little differently from the CPI.",
         high="Inflation is far above the Fed's 2% target: expect high rates and a hawkish Fed.",
         low="Inflation is at or below target: the Fed can ease."),
    dict(key="PPI", name="Producer prices (yearly)", cat="consumer", kind="fred_yoy", sym="PPIACO", start="1958-01-01", unit="%", dec=1, freq="monthly", signed=True,
         why="What companies pay for materials and goods. Those costs reach shop prices a few months later.",
         high="Companies face sharply higher costs: either they pass them on (more consumer inflation) or profits shrink.",
         low="Input costs are falling: it eases pressure on consumer prices and profit margins."),
    dict(key="INFEXP", name="Inflation expected by the bond market (10 years)", cat="consumer", kind="fred", sym="T10YIE", start="2003-01-01", unit="%", dec=2, freq="daily",
         why="Bond investors' guess for average yearly inflation over the next 10 years (the gap between normal and inflation-protected Treasury yields).",
         high="Investors expect lasting inflation: long-term yields tend to rise with it and the Fed gets less room.",
         low="Investors expect low inflation — or fear weak growth. Falling expectations often come with slowdowns."),
    dict(key="RETAIL", name="Retail sales (yearly change)", cat="consumer", kind="fred_yoy", sym="RSAFS", start="1993-01-01", unit="%", dec=1, freq="monthly", signed=True,
         why="What consumers spent in shops and online compared with a year ago (monthly, not adjusted for inflation). Spending is about 70% of the US economy.",
         high="Consumers are spending strongly: it supports growth and profits, but can keep inflation high.",
         low="Consumers are pulling back: a typical early sign of a slowdown."),
    dict(key="PCE_REAL", name="Real consumer spending (yearly change)", cat="consumer", kind="fred_yoy", sym="PCEC96", start="1960-01-01", unit="%", dec=1, freq="monthly", signed=True,
         why="What households spend on goods and services after inflation, compared with a year ago. It is the biggest driver of US growth.",
         high="Households are spending solidly more than a year ago: the economy has a strong engine.",
         low="Real spending is shrinking or flat: the economy loses its main engine."),
    dict(key="REVOLV", name="Credit-card borrowing (yearly growth)", cat="consumer", kind="fred_yoy", sym="REVOLSL", start="1968-01-01", unit="%", dec=1, freq="monthly", signed=True,
         why="How fast households' revolving debt (mostly credit cards) is growing. Fast growth together with weak income means households borrow to keep spending.",
         high="Card debt is growing fast: households are leaning on credit to spend, which makes them fragile if income falls.",
         low="Card debt is flat or shrinking: households are borrowing less, either carefully or because lenders pulled back."),
    dict(key="HOUST", name="Housing starts (new homes begun, thousands per year)", cat="consumer", kind="fred", sym="HOUST", start="1959-01-01", unit="", dec=0, freq="monthly",
         why="How many new homes builders started. Housing reacts first to higher mortgage rates and drives jobs in construction and spending on furniture and appliances.",
         high="Builders are very active: housing is strong and supports jobs and spending.",
         low="Building has collapsed: housing is weak, as in past recessions."),
    dict(key="UNRATE", name="Unemployment rate", cat="jobs", kind="fred", sym="UNRATE", start="1948-01-01", unit="%", dec=1, freq="monthly",
         why="Share of people who want a job but do not have one (monthly).",
         high="Unemployment is high: incomes and spending fall, and loans become harder to repay.",
         low="Unemployment is very low: workers have bargaining power, which pushes wages (and inflation) up."),
    dict(key="PAYROLL", name="Jobs added per month (thousands)", cat="jobs", kind="fred_diff", sym="PAYEMS", start="1948-01-01", unit="", dec=0, freq="monthly", signed=True,
         why="How many jobs the US economy added (or lost) in a month — the headline of the monthly jobs report.",
         high="Job creation is unusually strong: the economy is hot, which can keep the Fed from cutting rates.",
         low="Job creation has stalled or turned negative: recessions start here."),
    dict(key="JOBOPEN", name="Job openings (thousands)", cat="jobs", kind="fred", sym="JTSJOL", start="2000-12-01", unit="", dec=0, freq="monthly",
         why="How many jobs employers are trying to fill (monthly survey). It shows demand for workers before layoffs or hiring show up in the jobs report.",
         high="Employers are desperate for workers: a tight labour market that pushes wages up.",
         low="Openings are drying up: employers are no longer hiring, which usually comes before rising unemployment."),
    dict(key="CCSA", name="Continuing jobless claims (people still on benefits)", cat="jobs", kind="fred", sym="CCSA", start="1967-01-01", unit="", dec=0, freq="weekly", sides="high",
         why="How many people keep receiving unemployment benefits. New claims show layoffs; continuing claims show how hard it is to find a new job.",
         high="Laid-off people are not finding new jobs quickly: a labour market that is cooling, with weaker incomes ahead.",
         low=""),
    dict(key="INDPRO", name="Industrial production (yearly change)", cat="jobs", kind="fred_yoy", sym="INDPRO", start="1950-01-01", unit="%", dec=1, freq="monthly", signed=True,
         why="Output of factories, mines and utilities compared with a year ago.",
         high="Factories are producing much more than a year ago: the goods economy is expanding.",
         low="Output is falling: manufacturing is in a downturn, which often spreads to jobs."),
    dict(key="GDPQ", name="Economic growth (real GDP, annualised)", cat="jobs", kind="fred", sym="A191RL1Q225SBEA", start="1947-01-01", unit="%", dec=1, freq="quarterly", signed=True,
         why="How fast the US economy grew in the latest quarter, converted to a yearly rate (published about a month after the quarter ends, then revised).",
         high="The economy grew very fast: strong profits, but it can keep rates high.",
         low="The economy is shrinking: two such quarters in a row is the usual definition of a recession."),
    dict(key="M2G", name="Money supply growth (M2, yearly)", cat="consumer", kind="fred_yoy", sym="M2SL", start="1960-01-01", unit="%", dec=1, freq="monthly", signed=True,
         why="How fast the amount of money in bank accounts and cash grows. Fast growth fed the 2021–22 inflation; a shrinking money supply is rare and has come with recessions.",
         high="The money supply is growing very fast: inflation usually follows, with a lag of 1–2 years.",
         low="The money supply is shrinking: tight money — rare, and it has come before recessions."),
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
    if k == "fred_yoy":
        return yoy(hd.fred(item["sym"], item.get("start", "1950-01-01")))
    if k == "fred_diff":
        raw = hd.fred(item["sym"], item.get("start", "1950-01-01"))
        return [[raw[i][0], raw[i][1] - raw[i - 1][1]] for i in range(1, len(raw))]
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

    def _first(t):
        t = (t or "").strip()
        m = re.match(r"(.+?[.!?])(\s|$)", t)
        return m.group(1) if m else t
    if side:
        reading = item.get(side, "")
    else:
        near = "high" if pct >= 50 else "low"
        word = ("on the high side" if pct >= 70 else "a little above the middle" if pct >= 55 else
                "on the low side" if pct <= 30 else "a little below the middle" if pct <= 45 else "right in the middle")
        reading = f"Normal range: {word} (higher than {pct:.0f}% of its history). "
        if sides in ("both", "high") and item.get("high"):
            reading += f"At the top 5% it would mean: {_first(item['high'])} "
        if sides in ("both", "low") and item.get("low"):
            reading += f"At the bottom 5%: {_first(item['low'])}"
        reading = reading.strip()
    horizon, spec = special(item["freq"], mv, hub_zone=zone, pct=pct)
    return {
        "hub": True, "good": GOOD.get(item["key"], 0), "chgKind": chg, "d1": mv["d1"], "w1": mv["w1"], "m1": mv["m1"], "z1": mv["z1"], "zw": mv["zw"],
        "zm": mv["zm"], "horizon": horizon, "spec": spec, "rank": round(spec * WEIGHT.get(item["key"], 2.5), 2),
        "key": item["key"], "name": item["name"], "cat": item["cat"], "unit": item["unit"], "dec": item["dec"],
        "signed": bool(item.get("signed")), "freq": item["freq"], "why": item["why"], "sides": sides,
        "meaning": item.get(side or "", "") if side else "", "reading": reading,
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
    "NFCI": 3.5, "SAHM": 4, "UMCSENT": 2.5, "CCC": 4, "ICSA": 3.5, "SLOOS_CARD": 3, "SLOOS_BUS": 3, "STLFSI": 3, "INCOME": 3.5, "SAVE": 2.5, "DEBTSERV": 3, "WAGES": 3,
    "CORECPI": 4, "PCEPI": 4, "PPI": 2.5, "INFEXP": 3.5, "RETAIL": 3.5, "PCE_REAL": 3.5, "REVOLV": 2.5, "HOUST": 2.5,
    "UNRATE": 4, "PAYROLL": 4, "JOBOPEN": 3, "CCSA": 3, "INDPRO": 3, "GDPQ": 4, "M2G": 2.5,
    "BUFFETT": 3.5, "CAPE": 3.5, "ERP": 3.5, "SPX200": 3,
}
SPECIAL_Z = 2.0
# Is a rise good (+1) or bad (-1) for stock investors? 0 = no clear good or bad (shown in white, never green/red).
GOOD = {"SPX": 1, "IXIC": 1, "DJI": 1, "RUT": 1, "VIX": -1, "VOLRATIO": -1, "HYSPREAD": -1, "MORT30": -1, "REAL10": -1, "USCPI": -1,
        "NFCI": -1, "SAHM": -1, "UMCSENT": 1, "CCC": -1, "ICSA": -1, "SLOOS_CARD": -1, "SLOOS_BUS": -1, "STLFSI": -1, "INCOME": 1, "DEBTSERV": -1,
        "UNRATE": -1, "PAYROLL": 1, "JOBOPEN": 1, "CCSA": -1, "INDPRO": 1, "GDPQ": 1, "RETAIL": 1, "PCE_REAL": 1, "HOUST": 1,
        "CORECPI": -1, "PCEPI": -1, "PPI": -1, "INFEXP": -1}
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
         main=["US10Y", "US2Y", "US1Y", "CURVE", "MORT30", "FEDFUNDS", "DXY", "REAL10", "INFEXP"], context=[]),
    dict(id="metals", title="Gold, silver & Dr. Copper",
         intro="Gold, silver and copper move with interest rates and the dollar. Tap the ? on any card to see exactly how.",
         main=["GOLD", "SILVER", "COPPER", "CUAU", "GSR"], context=["REAL10", "US10Y", "DXY"]),
    dict(id="stocks", title="Stocks & investor mood",
         intro="The big US stock indexes and how scared investors are. Tap the ? on a card to see what usually drives it.",
         main=["SPX", "IXIC", "DJI", "RUT", "VIX", "MOOD", "VOLRATIO"], context=["US10Y", "HYSPREAD"]),
    dict(id="japan", title="Yen carry trade",
         intro="Traders borrow cheap yen to buy higher-yielding assets. When the yen jumps, they must buy yen back and sell those assets, stocks included.",
         main=["USDJPY", "GAP", "JP10Y", "BOJRATE", "JPCPI", "NIKKEI200"], context=["US10Y", "US2Y"]),
    dict(id="consumer", title="Consumers & prices",
         intro="What households earn, spend and borrow, and how fast prices rise.",
         main=["USCPI", "CORECPI", "PCEPI", "PPI", "RETAIL", "PCE_REAL", "INCOME", "WAGES", "SAVE", "REVOLV", "DEBTSERV", "UMCSENT", "HOUST", "M2G"], context=[]),
    dict(id="jobs", title="Jobs & growth",
         intro="Whether the economy is expanding or slowing.",
         main=["UNRATE", "PAYROLL", "JOBOPEN", "ICSA", "CCSA", "SAHM", "INDPRO", "GDPQ"], context=[]),
    dict(id="credit", title="Credit & bank lending",
         intro="Will loans be repaid? Markets react first, bank surveys follow months later.",
         main=["HYSPREAD", "CCC", "STLFSI", "NFCI", "SLOOS_CARD", "SLOOS_BUS"], context=["US10Y", "CURVE"]),
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
    big = sorted((k for k in items if (main is None or k in main) and items[k].get("horizon") and items[k].get("spec", 0) >= 1.0),
                 key=lambda k: -items[k].get("rank", 0))
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
    elif cid == "internals":
        _drivers_internals(sg, val, items, out)
    elif cid == "consumer":
        infl = sg("CORECPI") or sg("PCEPI") or sg("USCPI")
        if infl:
            out.append(("Inflation measures rose: the Fed keeps interest rates high for longer, which weighs on stocks and bonds." if infl > 0 else
                        "Inflation measures fell: the Fed gets room to cut rates, which supports stocks and bonds."))
        if sg("RETAIL") or sg("PCE_REAL"):
            out.append(("Consumers are spending more than usual: good for growth and profits." if (sg("RETAIL") or sg("PCE_REAL")) > 0 else "Consumer spending weakened: the biggest part of the economy is slowing."))
        if sg("INCOME") < 0 or sg("WAGES") < 0:
            out.append("Household income or wage growth weakened: spending and loan repayment come under pressure a few months later.")
        if sg("REVOLV") > 0 and sg("INCOME") <= 0:
            out.append("Card borrowing is growing while income is not: households are borrowing to keep spending, which is fragile.")
    elif cid == "jobs":
        if sg("UNRATE") > 0 or sg("ICSA") > 0 or sg("CCSA") > 0:
            out.append("Unemployment or layoffs are rising: incomes and spending follow down, and the Fed may have to cut rates.")
        if sg("PAYROLL") < 0:
            out.append("Job creation weakened sharply: recessions start with a stall in hiring.")
        if sg("INDPRO") or sg("GDPQ"):
            out.append("Output or growth moved a lot: " + ("expansion is picking up." if (sg("INDPRO") or sg("GDPQ")) > 0 else "the economy is slowing."))
    if not out:
        out.append("Nothing unusual in this group: moves are within their normal range.")
    if big:
        head = "Most unusual " + hz + ": " + ", ".join(items[k]["name"] for k in big[:3]) + "."
    else:
        head = "Quiet — no unusual move " + hz + "."
    return {"headline": head, "horizon": hz, "bullets": out[:6], "signal": bool(big) and not out[0].startswith(("Nothing unusual", "Valuation barely"))}


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
    ("ICSA", "INCOME", "inverse", "Layoffs cut household incomes: more new claims usually mean weaker income growth a few weeks to months later."),
    ("WAGES", "INCOME", "same", "Wages are the biggest part of household income."),
    ("USCPI", "INCOME", "inverse", "Higher inflation eats into income after inflation."),
    ("INCOME", "UMCSENT", "same", "Households feel better when their real income is rising."),
    ("INCOME", "SAVE", "same", "Households save more when their income grows."),
    ("DEBTSERV", "SLOOS_CARD", "same", "When payments take a bigger share of income, banks see more risk and tighten card lending."),
    ("ICSA", "SLOOS_CARD", "same", "When more people lose their jobs, banks expect more card defaults and tighten card lending months later."),
    ("CCC", "SLOOS_BUS", "same", "The weakest borrowers' spread reacts first; banks tighten their business lending standards a quarter or two later."),
    ("HYSPREAD", "SLOOS_BUS", "same", "Wider spreads show that lenders are afraid; banks follow by tightening their standards, with a delay of a quarter or two."),
    ("CORECPI", "FEDFUNDS", "same", "The Fed raises or holds rates when underlying inflation stays high, and cuts when it falls."),
    ("PCEPI", "FEDFUNDS", "same", "PCE is the inflation measure the Fed targets, so it drives its decisions most directly."),
    ("USCPI", "CORECPI", "same", "Core inflation is the CPI without food and energy: the two usually move together, apart from energy shocks."),
    ("PPI", "CORECPI", "same", "Higher costs for companies reach shop prices a few months later."),
    ("INFEXP", "US10Y", "same", "Long-term yields contain what investors expect for inflation, so higher expectations lift them."),
    ("WTI", "INFEXP", "same", "Oil is the fastest-moving part of inflation, so oil prices move inflation expectations."),
    ("WAGES", "CORECPI", "same", "Pay is the biggest cost for service companies, so strong wage growth feeds underlying inflation."),
    ("M2G", "USCPI", "same", "More money chasing the same goods lifts prices, with a delay of one to two years."),
    ("RETAIL", "PCE_REAL", "same", "Retail sales are part of consumer spending; real spending is the same thing after inflation and including services."),
    ("PCE_REAL", "GDPQ", "same", "Consumer spending is about 70% of the US economy, so it moves growth."),
    ("INCOME", "PCE_REAL", "same", "People spend what they earn: income growth carries spending."),
    ("REVOLV", "DEBTSERV", "same", "More card debt means a bigger share of income goes to payments."),
    ("UNRATE", "INCOME", "inverse", "Rising unemployment cuts household income."),
    ("UNRATE", "UMCSENT", "inverse", "People feel worse when unemployment rises."),
    ("UNRATE", "SAHM", "same", "The Sahm rule is built from the unemployment rate."),
    ("PAYROLL", "UNRATE", "inverse", "Strong job gains pull unemployment down; weak ones let it rise."),
    ("PAYROLL", "INCOME", "same", "More jobs mean more paychecks."),
    ("JOBOPEN", "UNRATE", "inverse", "Fewer openings mean the unemployed find jobs more slowly, so unemployment rises with a delay."),
    ("ICSA", "CCSA", "same", "New claims feed into continuing claims."),
    ("CCSA", "UNRATE", "same", "People who stay on benefits show up as unemployed."),
    ("INDPRO", "GDPQ", "same", "Factory output is a part of growth and a good early read on it."),
    ("HOUST", "GDPQ", "same", "Home building adds directly to growth and to spending on furniture and appliances."),
    ("MORT30", "HOUST", "inverse", "Higher mortgage rates make homes less affordable, so builders start fewer."),
    ("GDPQ", "SPX", "same", "Stocks follow company profits, and profits follow growth."),
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
    ("CAPE", "BUFFETT", "same", "Both compare stock prices with fundamentals (earnings, the size of the economy), so they get extreme together when the market becomes very expensive."),
    ("BUFFETT", "CAPE", "same", "Both compare stock prices with fundamentals, so they get extreme together when the market becomes very expensive."),
    ("SPX200", "VIX", "inverse", "A market far above its 200-day average is usually calm (low VIX); far below it, fear is high."),
    ("CAPE", "ERP", "inverse", "The stock reward over bonds is 100 ÷ the Shiller P/E minus the real yield: a more expensive market lowers it."),
    ("REAL10", "ERP", "inverse", "The stock reward over bonds is the earnings yield minus the real yield: a higher real yield lowers it."),
]
_MODE_WORDS = {"same": "usually move together", "inverse": "usually move in opposite directions"}


def stress(item):
    """-1..+1: how far out this indicator is right now — its biggest unusual move (today, this week or
    this month) or, for slow indicators, its place at the edge of its own history. Sign = direction."""
    zs = [item.get(k) or 0 for k in (("z1", "zw", "zm") if item.get("freq") == "daily" else ("z1",))]
    z = max(zs, key=abs, default=0)
    mv = max(-1.0, min(1.0, z / 3.0))
    lvl = 0.0
    if item.get("hub"):
        p, sides = item.get("pct", 50), item.get("sides", "both")
        if p >= 90 and sides != "low":
            lvl = min(1.0, (p - 80) / 20)
        elif p <= 10 and sides != "high":
            lvl = -min(1.0, (20 - p) / 20)
    return mv if abs(mv) >= abs(lvl) else lvl


def _names(items, keys):
    return ", ".join(items[k]["name"] for k in keys)


# Loose links: they hold on average but break all the time, so a gap is not worth a note or a lower ranking.
WEAK_LINKS = {("VIX", "GOLD"), ("COPPER", "SPX"), ("UMCSENT", "SPX"), ("WTI", "SPX"), ("USDJPY", "SPX"), ("DXY", "COPPER"), ("DXY", "SILVER"),
              ("DXY", "WTI"), ("GDPQ", "SPX"), ("HYSPREAD", "RUT"), ("NFCI", "SPX"), ("WTI", "USCPI"), ("GOLD", "SILVER"), ("M2G", "USCPI"),
              ("PPI", "CORECPI"), ("WAGES", "CORECPI"), ("MORT30", "HOUST"), ("ICSA", "SAHM"), ("RETAIL", "PCE_REAL")}


def rel_eval(items, driver, affected, mode, why=""):
    """Is this pair behaving the way it usually does? status: asusual / against / noreact / other / quiet."""
    d, a = items.get(driver), items.get(affected)
    if not d or not a:
        return None
    names, zk, ck = ("today", "this week", "this month"), ("z1", "zw", "zm"), ("d1", "w1", "m1")
    score = [max(abs(d.get(z) or 0), abs(a.get(z) or 0)) for z in zk]
    hi = max(range(3), key=lambda i: score[i]) if any(score) else 0
    hz = names[hi] if (d.get("freq") == "daily" or a.get("freq") == "daily") else "this month"
    pick = lambda x: x.get(ck[hi]) if x.get("freq") in ("daily", "weekly") else x.get("d1")      # slow series: change since the last release
    cd, ca = fchg(d["chgKind"], pick(d)), fchg(a["chgKind"], pick(a))
    sd, sa = stress(d), stress(a)
    expected = sd * (1 if mode == "same" else -1)
    if abs(sd) < 0.25 and abs(sa) < 0.25:
        status = "quiet"
    elif abs(sd) >= 0.35 and abs(sa) < 0.2:
        status = "noreact"
    elif abs(sd) < 0.2 and abs(sa) >= 0.35:
        status = "other"
    else:
        status = "asusual" if sa * expected > 0 else "against"
    # Other connected indicators that explain what the affected one is doing (or pulling the other way)
    helpers = []
    for dd, aa, mm, _ in RELATIONS:
        if aa == affected and dd != driver and dd in items:
            sc = stress(items[dd])
            if abs(sc) >= 0.35:
                helpers.append((dd, sc * (1 if mm == "same" else -1)))
    pull_same = [k for k, e in helpers if e * sa > 0] if abs(sa) >= 0.2 else []
    pull_other = [k for k, e in helpers if e * (expected or 1) < 0]
    slow = [x for x in (d, a) if x.get("freq") in ("monthly", "quarterly")]
    fast = [x for x in (d, a) if x.get("freq") in ("daily", "weekly")]
    lag = ""
    if slow and fast:
        sl = slow[0]
        lag = (f"{sl['name']} is a {sl['freq']} {'survey' if str(sl['key']).startswith('SLOOS') else 'figure'} that reacts months after market signals, "
               f"so a gap now does not mean the market signal is wrong — watch its next release.")
    reason = ""
    if status == "noreact":
        reason = f"{_names(items, pull_other)} pulls the other way." if pull_other else lag
    elif status in ("against", "other"):
        reason = f"{_names(items, pull_same)} explains it." if pull_same else (lag or ("Something else is driving it; check the news." if status == "against" else "No tracked driver explains it; check the news."))
    if status == "quiet":
        note = f"Neither moved much {hz}."
    elif status == "asusual":
        note = f"{d['name']} {cd}, {a['name']} {ca} {hz}: the usual link is working."
    elif status == "noreact":
        note = f"{d['name']} moved ({cd} {hz}) but {a['name']} has not followed. {reason}".strip()
    elif status == "against":
        note = f"{d['name']} {cd}, {a['name']} {ca} {hz}: moving against the usual link. {reason}"
    else:
        note = f"{a['name']} moved ({ca} {hz}) without a matching move in {d['name']}. {reason}"
    return {"driver": driver, "affected": affected, "mode": mode, "modeText": _MODE_WORDS[mode], "why": why,
            "status": status, "note": note.strip(), "reason": reason, "cd": cd, "ca": ca, "horizon": hz, "sd": round(sd, 2), "sa": round(sa, 2),
            "weak": (driver, affected) in WEAK_LINKS}


def adjusted_scores(items):
    """Importance after the links are taken into account. Two things that usually move together and both
    do (or both sit at extremes) reinforce each other; if one moves and the partner does not follow, the
    link is weaker than usual and both lose weight. Returns ({key: {"adj", "boost", "factor"}}, [edges])."""
    base = {k: it.get("rank", 0.0) for k, it in items.items()}
    boost, factor, edges = {k: 0.0 for k in items}, {k: 1.0 for k in items}, []
    for d, a, mode, why in RELATIONS:
        e = rel_eval(items, d, a, mode, why)
        if not e or e["status"] == "quiet" and max(abs(e["sd"]), abs(e["sa"])) < 0.35:
            continue
        reinforce = e["status"] == "asusual" and min(abs(e["sd"]), abs(e["sa"])) >= 0.4
        if reinforce:
            b = 0.5 * min(base[d], base[a])
            boost[d] += b
            boost[a] += b
        elif e["status"] in ("noreact", "against") and not e["weak"]:
            f = 0.85 if e["status"] == "noreact" else 0.9
            factor[d], factor[a] = max(0.6, factor[d] * f), max(0.6, factor[a] * f)
        e["reinforce"] = reinforce
        edges.append(e)
    boost = {k: min(boost[k], base[k]) for k in items}                  # a link can at most double an indicator's weight
    return {k: {"adj": round((base[k] + boost[k]) * factor[k], 2), "boost": round(boost[k], 2), "factor": round(factor[k], 2)} for k in items}, edges


def context_lines(items, edges):
    """For each indicator: what its connected indicators say about it right now, written from that card's
    point of view — divergences first (with the reason), then confirmations — so nothing stands alone."""
    by = {k: [] for k in items}
    for e in edges:
        for k in (e["driver"], e["affected"]):
            if k in by:
                by[k].append(e)
    order = {"against": 0, "noreact": 1, "other": 2, "asusual": 3, "quiet": 4}
    out = {}
    for k, es in by.items():
        es = sorted(es, key=lambda e: (order[e["status"]], -max(abs(e["sd"]), abs(e["sa"]))))
        lines = []
        for e in es:
            if len(lines) >= 2 or e["status"] == "quiet":
                break
            if e["weak"] and e["status"] != "asusual":
                continue
            is_aff = e["affected"] == k
            o = items[e["driver"] if is_aff else e["affected"]]["name"]
            me = items[k]["name"]
            if e["status"] == "asusual":
                lines.append(f"Confirmed by {o}, which moved the usual way.")
            elif e["status"] == "noreact":
                lines.append((f"{o} moved ({e['cd']}) but {me} has not followed. " if is_aff else f"{me} moved but {o} has not followed yet. ") + e["reason"])
            elif e["status"] == "against":
                lines.append((f"{o} moved ({e['cd']}), yet {me} went the other way. " if is_aff else f"{o} went the other way from what {me} normally implies. ") + e["reason"])
            elif is_aff:
                lines.append(f"{me} moved without a matching move in {o}. " + e["reason"])
        out[k] = (lines[0][:400], "confirm" if lines[0].startswith("Confirmed") else "diverge") if lines else ("", "")        # the most important link only
    return out


def explain(key, items):
    """What explains this indicator and what does it move? Each link says how the two usually behave and
    whether they are behaving that way right now; a short summary ties it together."""
    me = items.get(key)
    if not me:
        return None
    drivers = [x for x in (rel_eval(items, d, a, m, w) for d, a, m, w in RELATIONS if a == key) if x]
    effects = [x for x in (rel_eval(items, d, a, m, w) for d, a, m, w in RELATIONS if d == key) if x]
    order = {"against": 0, "other": 1, "noreact": 2, "asusual": 3, "quiet": 4}
    drivers.sort(key=lambda x: order[x["status"]])
    effects.sort(key=lambda x: order[x["status"]])
    # one connected summary
    hz = me.get("horizon")
    ck = {"today": "d1", "this week": "w1", "this month": "m1", "this quarter": "d1"}.get(hz or "")
    parts = []
    if hz and ck and me.get(ck) is not None:
        parts.append(f"{me['name']} moved {fchg(me['chgKind'], me[ck])} {hz} — unusually far.")
    elif hz == "at an extreme":
        parts.append(f"{me['name']} sits at an extreme of its own history.")
    else:
        parts.append(f"{me['name']} is not moving unusually right now.")
    ok = [x["driver"] for x in drivers if x["status"] == "asusual"]
    ag = [x["driver"] for x in drivers if x["status"] == "against"]
    nr = [x["driver"] for x in drivers if x["status"] == "noreact"]
    if ok:
        parts.append(f"{_names(items, ok)} moved the usual way and explain{'s' if len(ok) == 1 else ''} it.")
    if ag:
        parts.append(f"{_names(items, ag)} moved against the usual link, so something else is also at work.")
    if nr:
        parts.append(f"{_names(items, nr)} moved but {me['name']} did not follow.")
    out = [x["affected"] for x in effects if x["status"] == "asusual"]
    if out:
        parts.append(f"In turn it is moving {_names(items, out)}.")
    nm = lambda k: item_name(k)
    missing = [{"key": d, "name": nm(d), "side": "l", "why": w, "modeText": _MODE_WORDS[m]} for d, a, m, w in RELATIONS if a == key and d not in items] + \
              [{"key": a, "name": nm(a), "side": "r", "why": w, "modeText": _MODE_WORDS[m]} for d, a, m, w in RELATIONS if d == key and a not in items]
    return {"key": key, "drivers": drivers, "effects": effects, "summary": " ".join(parts), "missing": missing}


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
                      "bank lending standards", "Nasdaq tech stocks", "Russell 2000 small caps",
                      "Ukraine war", "Russia Ukraine peace talks", "NATO defense spending", "European defense stocks", "Iran", "Houthi Red Sea shipping",
                      "Strait of Hormuz oil", "Saudi Arabia oil production", "jet fuel prices", "diesel prices", "oil tanker rates"]


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
    "SAHM": ["recession", "sahm rule"],
    "UNRATE": ["unemployment rate", "jobs report", "jobless rate"], "PAYROLL": ["nonfarm payrolls", "payrolls", "jobs added", "job gains", "hiring"],
    "RETAIL": ["retail sales"], "PCE_REAL": ["consumer spending", "consumer spend"], "CORECPI": ["core inflation", "core cpi", "core prices"],
    "PCEPI": ["pce price", "pce inflation", "pce index"], "PPI": ["producer price", "wholesale price"], "INFEXP": ["inflation expectations", "breakeven"],
    "GDPQ": ["gdp", "economic growth", "economy grew", "economy shrank"], "HOUST": ["housing starts", "homebuilder", "home builders", "housing market"],
    "INDPRO": ["industrial production", "factory output", "manufacturing"], "JOBOPEN": ["job openings", "jolts"], "CCSA": ["continuing claims"],
    "INCOME": ["household income", "personal income", "real wages"], "WAGES": ["wage growth", "average hourly earnings", "wages"], "SAVE": ["saving rate", "savings rate"],
    "REVOLV": ["credit card balances", "credit card borrowing", "revolving credit"], "DEBTSERV": ["debt payments", "household debt"], "M2G": ["money supply", "m2"],
    "GEO_UA": ["ukraine", "ukrainian", "kyiv", "kiev", "zelensky", "zelenskyy", "putin", "kremlin", "moscow", "donbas", "crimea", "russian forces", "russia's war", "russia-ukraine",
               "nato", "rheinmetall", "european defense", "european defence", "rearmament", "defense spending", "defence spending"],
    "GEO_ME": ["iran", "iranian", "tehran", "houthi", "houthis", "red sea", "strait of hormuz", "hormuz", "yemen", "saudi", "riyadh", "opec", "persian gulf", "gaza", "israel",
               "hezbollah", "suez canal", "tanker attack", "oil tanker", "middle east"],
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


_NEWS_CTX = {}      # topic -> (name regex, regex of words that must be in the same text), for names that are also everyday words
_NEWS_PARENTS = {}  # topic -> topics it also belongs to (a company belongs to its group average and to its conflict)


NOISE_RX = re.compile(r"retire|social security|401\(k\)|\bira\b|annuit|mortgage rates?\b|refinanc|\bcd rates?\b|best (?:credit|savings|high-yield|cd)|credit cards?\b|savings account|financial advisor|how to (?:save|invest|budget|retire)"
                      r"|millionaire|billionaire(?:'s)? (?:advice|says|tips)|net worth|stimulus check|medicare|inherit|sponsored|horoscope|celebrit|lottery|powerball|should you (?:buy|sell)|stocks? to buy|"
                      r"\bmotley fool\b|zacks|\d+ (?:best|top) (?:stocks|etfs|dividend)|passive income|side hustle|dividend stocks? for|your portfolio|\bwhat to do with\b", re.I)


def is_noise(title, summary=""):
    """Personal-finance advice, ads and stock-picking listicles: not market news."""
    return bool(NOISE_RX.search(title))


def tag_news(title, summary=""):
    """Indicator keys a headline is about (a match in the title counts, one only in the summary counts less)."""
    if is_noise(title, summary):
        return []
    hits = []
    for k, rx in _NEWS_RX.items():
        if k in _NEWS_NEG and _NEWS_NEG[k].search(title):
            continue
        if rx.search(title):
            hits.append((k, 2))
        elif summary and rx.search(summary):
            hits.append((k, 1))
    text = title + " " + (summary or "")
    have = {k for k, _ in hits}
    for k, (name, ctx) in _NEWS_CTX.items():
        if k not in have and name.search(title) and ctx.search(text):
            hits.append((k, 2))
            have.add(k)
    for k in list(have):
        for p in _NEWS_PARENTS.get(k, ()):
            if p not in have:
                hits.append((p, 1))
                have.add(p)
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


# ══ Heatmap (S&P 500 / Nasdaq-100) ═════════════════════════════════════════════════════════════
# Index lists from Wikipedia, prices / market caps / sectors from Nasdaq's public screener; if those fail,
# Yahoo prices for a built-in list of the biggest companies. Each step reports what it used.
HEAT_INDEXES = {"sp500": ("S&P 500", "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"),
                "nasdaq100": ("Nasdaq-100", "https://en.wikipedia.org/wiki/Nasdaq-100")}
_BROWSER = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
# ticker|company|sector|approx. market cap in $bn|S&P 500|Nasdaq-100   (fallback only — sizes are approximate)
_HEAT_STATIC = """AAPL|Apple|Information Technology|3300|1|1;MSFT|Microsoft|Information Technology|3400|1|1;NVDA|NVIDIA|Information Technology|3300|1|1;AVGO|Broadcom|Information Technology|1100|1|1;ORCL|Oracle|Information Technology|450|1|0;CRM|Salesforce|Information Technology|250|1|0;AMD|AMD|Information Technology|250|1|1;CSCO|Cisco|Information Technology|230|1|1;ADBE|Adobe|Information Technology|190|1|1;ACN|Accenture|Information Technology|220|1|0;IBM|IBM|Information Technology|220|1|0;QCOM|Qualcomm|Information Technology|180|1|1;INTU|Intuit|Information Technology|180|1|1;TXN|Texas Instruments|Information Technology|180|1|1;AMAT|Applied Materials|Information Technology|140|1|1;NOW|ServiceNow|Information Technology|200|1|0;MU|Micron|Information Technology|120|1|1;PANW|Palo Alto Networks|Information Technology|120|1|1;ADI|Analog Devices|Information Technology|110|1|1;LRCX|Lam Research|Information Technology|100|1|1;KLAC|KLA|Information Technology|90|1|1;INTC|Intel|Information Technology|90|1|1;SNPS|Synopsys|Information Technology|80|1|1;CDNS|Cadence|Information Technology|85|1|1;CRWD|CrowdStrike|Information Technology|90|1|1;ANET|Arista Networks|Information Technology|120|1|0;PLTR|Palantir|Information Technology|200|1|1;GOOGL|Alphabet A|Communication Services|2100|1|1;GOOG|Alphabet C|Communication Services|2100|1|1;META|Meta Platforms|Communication Services|1500|1|1;NFLX|Netflix|Communication Services|400|1|1;TMUS|T-Mobile US|Communication Services|250|1|1;DIS|Disney|Communication Services|200|1|0;CMCSA|Comcast|Communication Services|150|1|1;T|AT&T|Communication Services|160|1|0;VZ|Verizon|Communication Services|170|1|0;AMZN|Amazon|Consumer Discretionary|2000|1|1;TSLA|Tesla|Consumer Discretionary|1000|1|1;HD|Home Depot|Consumer Discretionary|380|1|0;MCD|McDonald's|Consumer Discretionary|210|1|0;BKNG|Booking Holdings|Consumer Discretionary|160|1|1;LOW|Lowe's|Consumer Discretionary|130|1|0;NKE|Nike|Consumer Discretionary|100|1|0;SBUX|Starbucks|Consumer Discretionary|100|1|1;TJX|TJX|Consumer Discretionary|140|1|0;ABNB|Airbnb|Consumer Discretionary|80|1|1;WMT|Walmart|Consumer Staples|700|1|0;COST|Costco|Consumer Staples|420|1|1;PG|Procter & Gamble|Consumer Staples|380|1|0;KO|Coca-Cola|Consumer Staples|300|1|0;PEP|PepsiCo|Consumer Staples|200|1|1;PM|Philip Morris|Consumer Staples|190|1|0;MDLZ|Mondelez|Consumer Staples|90|1|1;MO|Altria|Consumer Staples|100|1|0;LLY|Eli Lilly|Health Care|700|1|0;UNH|UnitedHealth|Health Care|450|1|0;JNJ|Johnson & Johnson|Health Care|380|1|0;ABBV|AbbVie|Health Care|330|1|0;MRK|Merck|Health Care|250|1|0;TMO|Thermo Fisher|Health Care|200|1|0;ABT|Abbott|Health Care|220|1|0;AMGN|Amgen|Health Care|150|1|1;ISRG|Intuitive Surgical|Health Care|190|1|1;PFE|Pfizer|Health Care|150|1|0;DHR|Danaher|Health Care|170|1|0;GILD|Gilead|Health Care|130|1|1;BRK-B|Berkshire Hathaway B|Financials|1000|1|0;JPM|JPMorgan Chase|Financials|700|1|0;V|Visa|Financials|600|1|0;MA|Mastercard|Financials|480|1|0;BAC|Bank of America|Financials|330|1|0;WFC|Wells Fargo|Financials|250|1|0;GS|Goldman Sachs|Financials|200|1|0;MS|Morgan Stanley|Financials|200|1|0;AXP|American Express|Financials|200|1|0;BLK|BlackRock|Financials|150|1|0;C|Citigroup|Financials|140|1|0;SPGI|S&P Global|Financials|150|1|0;GE|GE Aerospace|Industrials|250|1|0;CAT|Caterpillar|Industrials|190|1|0;RTX|RTX|Industrials|180|1|0;HON|Honeywell|Industrials|140|1|1;UNP|Union Pacific|Industrials|140|1|0;BA|Boeing|Industrials|130|1|0;DE|Deere|Industrials|120|1|0;LMT|Lockheed Martin|Industrials|110|1|0;UPS|UPS|Industrials|100|1|0;XOM|Exxon Mobil|Energy|480|1|0;CVX|Chevron|Energy|270|1|0;COP|ConocoPhillips|Energy|130|1|0;NEE|NextEra Energy|Utilities|150|1|0;SO|Southern Company|Utilities|90|1|0;DUK|Duke Energy|Utilities|90|1|0;PLD|Prologis|Real Estate|100|1|0;AMT|American Tower|Real Estate|100|1|0;EQIX|Equinix|Real Estate|80|1|0;LIN|Linde|Materials|220|1|1;SHW|Sherwin-Williams|Materials|90|1|0;ASML|ASML|Information Technology|300|0|1;AZN|AstraZeneca|Health Care|230|0|1"""


def heat_static(index):
    out = []
    for row in _HEAT_STATIC.split(";"):
        t, n, s, cap, sp, nd = row.split("|")
        if not float(cap):
            continue
        if (index == "sp500" and sp == "1") or (index == "nasdaq100" and nd == "1"):
            out.append({"t": t, "n": n, "s": s, "i": "", "m": float(cap) * 1e9})
    return out


def parse_wiki_constituents(html):
    """Rows of the 'constituents' table on Wikipedia: [(ticker, company, sector, industry)]."""
    i = html.find('id="constituents"')
    if i < 0:
        raise ValueError("constituents table not found")
    j = html.find("</table>", i)
    out = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html[i:j], re.S):
        cells = [_clean_text(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
        if len(cells) >= 3 and cells[0] and cells[0].lower() not in ("symbol", "ticker"):
            out.append((norm_ticker(cells[0]), cells[1], cells[2], cells[3] if len(cells) > 3 else ""))
    if len(out) < 50:
        raise ValueError(f"only {len(out)} rows parsed")
    return out


def norm_ticker(t):
    return re.sub(r"[./\s]", "-", (t or "").strip().upper())


def _num(s):
    s = re.sub(r"[^0-9.\-]", "", str(s or ""))
    try:
        return float(s) if s not in ("", "-", ".") else 0.0
    except ValueError:
        return 0.0


def fetch_screener():
    """All US-listed stocks with price change, market cap and sector from Nasdaq's public screener."""
    import json as _json
    out = {}
    for ex in ("NASDAQ", "NYSE", "AMEX"):
        url = f"https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&offset=0&exchange={ex}&download=true"
        req = urllib.request.Request(url, headers={"User-Agent": _BROWSER, "Accept": "application/json, text/plain, */*",
                                                    "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"})
        with urllib.request.urlopen(req, timeout=40) as r:
            data = _json.loads(r.read().decode("utf-8", "replace"))
        rows = ((data.get("data") or {}).get("rows")) or []
        for r_ in rows:
            t = norm_ticker(r_.get("symbol"))
            if not t:
                continue
            pct = r_.get("pctchange")
            out[t] = {"n": r_.get("name") or t, "p": 0.0 if str(pct).strip().upper() in ("UNCH", "", "NONE") else _num(pct),
                      "m": _num(r_.get("marketCap")), "px": _num(r_.get("lastsale")), "s": r_.get("sector") or "", "i": r_.get("industry") or ""}
    if len(out) < 500:
        raise ValueError(f"screener returned only {len(out)} stocks")
    return out


def build_heatmap(index, wiki=None, screener=None, yahoo=None, caps=None):
    """Combine whatever sources worked into [{t, n, s, i, c(market cap), p(% change today)}] and say which."""
    notes = []
    base = []
    if wiki:
        base = [{"t": t, "n": n, "s": s, "i": i, "m": (caps or {}).get(t, 0.0)} for t, n, s, i in wiki]
        notes.append("index list: Wikipedia")
    else:
        base = heat_static(index)
        notes.append("index list: built-in list of the largest companies (Wikipedia not reachable)")
    rows = []
    for b in base:
        q = (screener or {}).get(b["t"])
        if q:
            rows.append({"t": b["t"], "n": q["n"] or b["n"], "s": b["s"] or q["s"] or "Other", "i": b["i"] or q["i"], "c": q["m"] or b["m"], "p": q["p"]})
        elif yahoo and b["t"] in yahoo and b["m"]:
            rows.append({"t": b["t"], "n": b["n"], "s": b["s"] or "Other", "i": b["i"], "c": b["m"], "p": yahoo[b["t"]]})
    if screener:
        notes.append("prices & market caps: Nasdaq screener")
    elif yahoo:
        notes.append("prices: Yahoo Finance; market caps: approximate (screener not reachable)")
    rows = [r for r in rows if r["c"] > 0]
    # a company with two share classes is listed twice with the company's full value: show it once (class A) with the larger value
    DUAL = {"GOOG": "GOOGL", "FOX": "FOXA", "NWS": "NWSA", "UA": "UAA", "BF-A": "BF-B", "LEN-B": "LEN", "BRK-A": "BRK-B"}
    by_t = {r["t"]: r for r in rows}
    drop = set()
    for second, first in DUAL.items():
        if second in by_t and first in by_t:
            by_t[first]["c"] = max(by_t[first]["c"], by_t[second]["c"])
            drop.add(second)
    rows.sort(key=lambda r: -r["c"])
    seen = []
    for r in rows:      # any other twin: same first word in the name and a company value within 15%
        w = (r["n"].lower().replace(".", "").split() or [""])[0]
        if any(w == x[0] and abs(r["c"] / x[1] - 1) < 0.15 and "class" in (r["n"] + x[2]).lower() for x in seen):
            drop.add(r["t"])
        else:
            seen.append((w, r["c"], r["n"]))
    rows = [r for r in rows if r["t"] not in drop]
    if not rows:
        raise ValueError("no usable rows")
    return {"rows": rows, "notes": notes}


def yahoo_changes(tickers):
    """% change from the previous close, for many tickers in one Yahoo download."""
    got = fetch_yahoo(tickers, period="5d")
    out = {}
    for t, pts in got.items():
        if len(pts) >= 2 and pts[-2][1]:
            out[t] = (pts[-1][1] / pts[-2][1] - 1) * 100
    return out


# ══ Geopolitics: two conflicts that move markets ═══════════════════════════════════════════════
def geo_key(sym):
    return "G_" + re.sub(r"[^A-Z0-9]", "_", sym.upper())


# member: (Yahoo symbol, name, unit). refs: indicators the app already has. Baskets are equal-weight averages.
GEO = {
    "ua": dict(
        title="Russia–Ukraine", topic="GEO_UA",
        blurb="The war keeps NATO governments buying weapons, so defense companies on both sides of the Atlantic follow rearmament budgets and peace hopes. It also decides how much Europe pays for gas and how much Ukrainian and Russian grain reaches the world.",
        groups=[
            dict(id="eudef", title="European defense", basket=("G_EUDEF", "European defense stocks (average)"),
                 says="European defense companies: orders follow the NATO members' rearmament budgets, which rise with the war and fall with peace hopes.",
                 members=[("RHM.DE", "Rheinmetall", "€"), ("BA.L", "BAE Systems", ""), ("LDO.MI", "Leonardo", "€"), ("HO.PA", "Thales", "€"),
                          ("SAAB-B.ST", "Saab", ""), ("HAG.DE", "Hensoldt", "€"), ("KOG.OL", "Kongsberg", "")]),
            dict(id="usdef", title="US defense", basket=("G_USDEF", "US defense stocks (average)"),
                 says="US defense companies: they sell to the Pentagon and to allies who buy American weapons, so they follow US budgets and Ukraine aid.",
                 members=[("LMT", "Lockheed Martin", "$"), ("RTX", "RTX", "$"), ("NOC", "Northrop Grumman", "$"), ("GD", "General Dynamics", "$"),
                          ("LHX", "L3Harris", "$"), ("ITA", "US aerospace & defense ETF", "$")]),
            dict(id="gas", title="Gas & grain", basket=None,
                 says="What the war costs everyone else: Europe's gas price and the world's wheat price both react to supply fears.",
                 members=[("TTF=F", "European gas (TTF)", "€"), ("ZW=F", "Wheat", ""), ("RUB=X", "Rubles per dollar", ""), ("EURUSD=X", "Euro in dollars", "$"), ("^GDAXI", "German DAX", "")],
                 refs=["NATGAS"]),
        ]),
    "me": dict(
        title="Middle East", topic="GEO_ME",
        blurb="Iran, the Houthis in the Red Sea and Saudi Arabia decide how much oil flows and how safely ships can carry it. Disruption lifts oil, diesel, jet fuel and tanker rates, and squeezes airlines.",
        groups=[
            dict(id="oil", title="Oil", basket=None, says="Crude oil: the first thing that reacts when Gulf or Red Sea supply is threatened.", members=[], refs=["WTI", "BRENT"]),
            dict(id="fuels", title="Diesel, jet fuel & petrol", basket=None,
                 says="Refined fuels: they follow crude oil, but can jump on their own when refineries or shipping routes are disrupted. Diesel and jet fuel matter most for trucks, ships and airlines.",
                 members=[("HO=F", "Diesel (heating oil futures)", "$"), ("RB=F", "Petrol (gasoline futures)", "$")], extra=[("G_JETFUEL", "Jet fuel / kerosene (US Gulf Coast)", "$")]),
            dict(id="ships", title="Tankers & shipping", basket=("G_TANKERS", "Oil tanker stocks (average)"),
                 says="Tanker companies earn more when routes get longer or riskier, because ships are tied up and rates rise. Container lines are hit by the Red Sea detours too.",
                 members=[("FRO", "Frontline", "$"), ("STNG", "Scorpio Tankers", "$"), ("DHT", "DHT Holdings", "$"), ("INSW", "International Seaways", "$"), ("TNK", "Teekay Tankers", "$"), ("NAT", "Nordic American Tankers", "$")]),
            dict(id="cont", title="Container shipping", basket=None, says="Container lines: Red Sea attacks force ships around Africa, which lifts freight rates and costs.",
                 members=[("ZIM", "ZIM Integrated Shipping", "$"), ("MAERSK-B.CO", "Maersk", "")]),
            dict(id="air", title="Airlines (the losers)", basket=("G_AIRLINES", "Airline stocks (average)"),
                 says="Airlines: fuel is their biggest cost after pay, so higher jet fuel and a risky Middle East hit their profits.",
                 members=[("DAL", "Delta", "$"), ("UAL", "United", "$"), ("AAL", "American", "$"), ("LUV", "Southwest", "$"), ("JETS", "US airlines ETF", "$")]),
            dict(id="region", title="Region & safe haven", basket=None, says="Saudi and Israeli stocks show how the region's markets see the risk; gold is where money goes when it gets scary.",
                 members=[("KSA", "Saudi Arabia ETF", "$"), ("EIS", "Israel ETF", "$")], refs=["GOLD"]),
        ]),
}
GEO_ITEMS = {}          # key -> definition (name, unit, group says, conflict)
GEO_BASKETS = {}        # basket key -> member keys
for _cid, _c in GEO.items():
    for _g in _c["groups"]:
        _keys = []
        for _sym, _nm, _u in _g["members"]:
            GEO_ITEMS[geo_key(_sym)] = dict(sym=_sym, name=_nm, unit=_u, says=_g["says"], conflict=_cid, group=_g["id"])
            _keys.append(geo_key(_sym))
        for _k, _nm, _u in _g.get("extra", []):
            GEO_ITEMS[_k] = dict(sym=None, name=_nm, unit=_u, says=_g["says"], conflict=_cid, group=_g["id"])
        if _g.get("basket"):
            GEO_ITEMS[_g["basket"][0]] = dict(sym=None, name=_g["basket"][1], unit="", says=_g["says"] + " (An equal-weight average of the companies below, starting at 100.)", conflict=_cid, group=_g["id"], basket=True)
            GEO_BASKETS[_g["basket"][0]] = [k for k in _keys if not k.endswith("_ITA") and not k.endswith("_JETS")]


def geo_symbols():
    return sorted({v["sym"] for v in GEO_ITEMS.values() if v.get("sym")})


def item_name(k):
    return (BY_KEY.get(k) or HUB_BY_KEY.get(k) or GEO_ITEMS.get(k) or {"name": k})["name"]


def build_basket(series_list):
    """Equal-weight average of the members' daily returns, as an index that starts at 100."""
    rets = {}
    for pts in series_list:
        for i in range(1, len(pts)):
            if pts[i - 1][1]:
                rets.setdefault(pts[i][0], []).append(pts[i][1] / pts[i - 1][1] - 1)
    level, out = 100.0, []
    for d in sorted(rets):
        level *= 1 + sum(rets[d]) / len(rets[d])
        out.append([d, level])
    return out


def geo_summaries(load):
    """Standard indicator summaries (value, changes, sparkline, unusual-ness) for the geopolitics items."""
    out = {}
    for k, v in GEO_ITEMS.items():
        pts = load(k)
        if not pts or len(pts) < 30:
            continue
        ind = dict(key=k, name=v["name"], group=None, source="yahoo" if v.get("sym") else "calc", unit=v["unit"], dec=2, kind="range", chg="pct", freq="daily",
                   explain=v["says"])
        try:
            s_ = summarize(ind, pts)
        except Exception:
            continue
        if s_:
            s_["geo"] = v["conflict"]
            s_["group"] = v["group"]
            out[k] = s_
    return out


GEO_RELATIONS = [
    ("G_EUDEF", "G_USDEF", "same", "European and US defense companies both live off NATO rearmament, so they usually move together; a gap points to budget news in one region."),
    ("G_TTF_F", "NATGAS", "same", "European and US gas prices react to the same supply fears, but Europe's moves first and hardest."),
    ("G_RUB_X", "G_TTF_F", "inverse", "A weaker ruble is often a sign of heavier sanctions or war stress, which tends to come with higher European gas prices."),
    ("BRENT", "G_HO_F", "same", "Diesel is made from crude oil, so it follows Brent — and can run ahead of it when refineries or shipping routes are disrupted."),
    ("BRENT", "G_RB_F", "same", "Petrol is made from crude oil, so it follows Brent."),
    ("BRENT", "G_JETFUEL", "same", "Jet fuel is made from crude oil, so it follows Brent."),
    ("G_HO_F", "G_JETFUEL", "same", "Jet fuel and diesel come from the same refinery stream (middle distillates), so they move together."),
    ("G_JETFUEL", "G_AIRLINES", "inverse", "Jet fuel is an airline's biggest cost after pay: higher fuel cuts profit, lower fuel lifts it."),
    ("BRENT", "G_AIRLINES", "inverse", "Oil sets fuel costs for airlines, and a Middle East crisis also scares travellers."),
    ("BRENT", "G_TANKERS", "same", "Gulf and Red Sea disruptions lengthen routes and lift tanker rates, which lifts tanker stocks and oil together."),
    ("G_TANKERS", "G_ZIM", "same", "Both are shipping companies that profit when routes get longer and ships get scarce."),
    ("BRENT", "G_KSA", "same", "Saudi Arabia lives off oil: its stock market follows the oil price."),
    ("GOLD", "G_EIS", "inverse", "Money runs into gold and out of risky regional markets when war news gets worse."),
    ("BRENT", "INFEXP", "same", "Oil is the fastest-moving part of inflation, so oil prices move inflation expectations."),
]
RELATIONS.extend(GEO_RELATIONS)
WEAK_LINKS.update({("G_RUB_X", "G_TTF_F"), ("GOLD", "G_EIS"), ("G_TANKERS", "G_ZIM"), ("BRENT", "G_KSA"), ("BRENT", "G_AIRLINES"), ("BRENT", "INFEXP")})
WEIGHT.update({"G_EUDEF": 3.5, "G_USDEF": 3.5, "G_TANKERS": 3, "G_AIRLINES": 3, "G_HO_F": 3, "G_JETFUEL": 3, "G_TTF_F": 3, "G_RB_F": 2})
GOOD.update({"G_AIRLINES": 1, "G_EUDEF": 0, "G_USDEF": 0})


# ══ Market internals ═════════════════════════════════════════════════════════════════════════════
SAYS.update({
    "SECT_UP": "How many of the 11 sectors rose. A falling index with most sectors up is a narrow sell-off in a few giants (today's AI names); with most sectors down it is broad and more serious.",
    "AIB": "The companies building the AI infrastructure: Nvidia, Microsoft, Alphabet, Meta, Amazon, Broadcom. They are a big share of the index, so when they fall the index falls, even if other sectors rise.",
    "AIU": "The companies that use AI and pay for it: banks and industrials. When money rotates from AI builders to these, the market is questioning the builders' spending, not the economy.",
    "AIB_AIU": "AI builders against AI users. Falling = money leaves the builders for the users: a rotation, not necessarily a crash. Rising = the AI trade is back in charge.",
    "DEFN": "Health care, staples, utilities and real estate: where money hides when growth stocks are sold. They rise on days when tech falls.",
    "XLF": "Banks reflect the economy: if banks rally, markets are not worried about growth or credit, whatever tech does.",
    "KRE": "Small US banks are the first to suffer from credit losses and funding stress, so they warn early.",
    "BREADTH": "Equal-weight against size-weight S&P 500. Falling = a few giants carry the index (narrow and fragile — a bubble sign); rising = the rally is broad and healthier.",
    "VIXTS": "Fear now against fear in three months. Above 1 (inverted) means traders pay more for protection now than later: real stress, not just noise.",
    "VVIX": "How much traders pay to protect against a jump in the VIX itself. High while the VIX is calm warns that calm can end suddenly.",
})
WEIGHT.update({"SECT_UP": 3.5, "AIB": 4, "AIU": 3, "AIB_AIU": 3.5, "DEFN": 2.5, "XLF": 3.5, "KRE": 3, "BREADTH": 3.5, "VIXTS": 4, "VVIX": 3})
GOOD.update({"AIB_AIU": 1, "SECT_UP": 1, "AIB": 1, "AIU": 1, "XLF": 1, "KRE": 1, "BREADTH": 1, "DEFN": 0, "VIXTS": -1, "VVIX": -1})
CLUSTERS.append(dict(id="internals", title="Market internals: breadth, AI & banks",
                     intro="What is under the index: how broad a move is, the AI builders against their customers, banks, and how stressed the volatility curve is.",
                     main=["SECT_UP", "AIB", "AIU", "AIB_AIU", "DEFN", "XLF", "KRE", "BREADTH", "VIXTS", "VVIX"], context=["SPX", "VIX"]))
RELATIONS.extend([
    ("SECT_UP", "SPX", "same", "When most sectors rise the index rises broadly. A falling index with most sectors up means a few giant stocks pull it down."),
    ("AIB", "IXIC", "same", "The AI builders are the biggest part of the Nasdaq, so they move it."),
    ("AIB", "SPX", "same", "The AI builders are a large share of the S&P 500, so they move it a lot."),
    ("AIB", "AIB_AIU", "same", "The builders are the top of the ratio: when they fall faster than the users, the ratio falls."),
    ("AIU", "AIB_AIU", "inverse", "The users are the bottom of the ratio: when they rise against the builders, the ratio falls."),
    ("AIB_AIU", "BREADTH", "same", "When money leaves the few AI giants for everyone else, the index gets broader."),
    ("XLF", "SPX", "same", "Banks are tied to the economy: rising banks mean the credit cycle looks fine."),
    ("XLF", "KRE", "same", "Big and small banks usually move together; small banks swing more."),
    ("KRE", "HYSPREAD", "inverse", "Regional banks fall when credit stress rises, and rise when it eases."),
    ("XLF", "AIU", "same", "Banks are half of the AI users basket."),
    ("VIXTS", "VIX", "same", "When the short-term VIX jumps above the 3-month one, the VIX is the part that is moving."),
    ("VVIX", "VIX", "same", "A high VVIX warns that the VIX can jump, which is why a quiet VIX can wake up."),
    ("DEFN", "SECT_UP", "same", "Defensive sectors are four of the eleven, so they decide the count on tech sell-off days."),
    ("SPX", "SECT_UP", "same", "A rising index usually lifts most sectors."),
])
NEWS_TOPICS.update({
    "AIB": ["nvidia", "microsoft", "alphabet", "google", "meta platforms", "amazon", "broadcom", "ai spending", "ai capex", "capital expenditure", "hyperscaler", "data center",
            "ai stocks", "ai trade", "ai bubble", "ai boom"],
    "SECT_UP": ["sector rotation", "stock market today", "market breadth", "sectors"],
    "XLF": ["bank earnings", "banks rally", "jpmorgan", "financial stocks", "bank stocks"], "KRE": ["regional banks", "regional bank"],
    "BREADTH": ["market breadth", "narrow rally", "equal-weight", "concentration", "magnificent seven", "mag 7"], "VVIX": ["vvix", "volatility spike"],
})
_NEWS_RX.update({k: re.compile(r"(?<![a-z0-9])(?:" + "|".join(re.escape(p) for p in NEWS_TOPICS[k]) + r")(?![a-z0-9])", re.I) for k in ("AIB", "SECT_UP", "XLF", "KRE", "BREADTH", "VVIX")})
SECTOR_NAMES = dict(_SECTORS)


def compute_internals(load):
    """Series for the calculated internals, from the stored sector / AI-builder / volatility series."""
    out, parts = {}, {}
    rets = {}
    for k, _ in _SECTORS:
        pts = load(k)
        for i in range(1, len(pts)):
            if pts[i - 1][1]:
                rets.setdefault(pts[i][0], {})[k] = pts[i][1] / pts[i - 1][1] - 1
    days = sorted(d for d, r in rets.items() if len(r) >= 9)
    if days:
        out["SECT_UP"] = [[d, float(sum(1 for v in rets[d].values() if v > 0))] for d in days]
        parts = {k: rets[days[-1]][k] * 100 for k in rets[days[-1]]}
    def basket(keys):
        return build_basket([load(k) for k in keys if load(k)])
    aib, aiu = basket(AI_BUILDERS), basket(["XLF", "XLI"])
    if len(aib) > 30:
        out["AIB"] = aib
    if len(aiu) > 30:
        out["AIU"] = aiu
    if len(aib) > 30 and len(aiu) > 30:
        um = dict(aiu)
        ratio = [[d, v / um[d] * 100] for d, v in aib if d in um and um[d]]
        if ratio:
            out["AIB_AIU"] = [[d, v / ratio[0][1] * 100] for d, v in ratio]
    dfn = basket(["XLV", "XLP", "XLU", "XLRE"])
    if len(dfn) > 30:
        out["DEFN"] = dfn
    rsp, spy = dict(load("RSP")), load("SPY")
    br = [[d, rsp[d] / v * 100] for d, v in spy if d in rsp and v]
    if len(br) > 30:
        out["BREADTH"] = [[d, v / br[0][1] * 100] for d, v in br]
    v3 = dict(load("VIX3M"))
    ts = [[d, v / v3[d]] for d, v in load("VIX") if d in v3 and v3[d]]
    if len(ts) > 30:
        out["VIXTS"] = ts
    return out, parts


def _drivers_internals(sg, val, items, out):
    sp, up = sg("SPX"), val("SECT_UP")
    if sp < 0 and up is not None and up >= 7:
        out.append(f"The index fell while {up:.0f} of 11 sectors rose: a narrow sell-off in a few giants (the AI builders), not a broad one. Less alarming, but it shows how much the index depends on a few names.")
    elif sp < 0 and up is not None and up <= 3:
        out.append(f"The index fell and only {up:.0f} of 11 sectors rose: a broad sell-off, which is more serious than a rotation.")
    elif sp > 0 and up is not None and up <= 4:
        out.append(f"The index rose while only {up:.0f} of 11 sectors did: a narrow rally that rests on a few names.")
    if sg("AIB_AIU") < 0:
        out.append("Money moved from the AI builders to the companies that use AI (banks, industrials): the market questions the builders' spending, not the economy.")
    elif sg("AIB_AIU") > 0:
        out.append("Money moved back into the AI builders: the AI trade is in charge again.")
    if sg("XLF") > 0 and sp < 0:
        out.append("Banks rose while the index fell: the market is not worried about the economy or credit; the sell-off is about the valuation of one theme.")
    elif sg("KRE") < 0 and sg("XLF") < 0:
        out.append("Banks fell: watch credit spreads — this is how stress in the economy shows up first.")
    ts = val("VIXTS")
    if ts is not None and ts >= 1.0:
        out.append("Short-term fear is above 3-month fear (inverted): traders pay more for protection now than later. That marks real stress, not just a bad day.")
    if sg("VVIX") > 0 and sg("VIX") <= 0:
        out.append("Traders are buying protection against a VIX jump while the VIX is calm: the calm may not last.")


# ══ Earnings reactions against what the options market expected ══════════════════════════════════
EARN_WATCH = ["AMZN", "GOOGL", "META", "MSFT", "AAPL", "NVDA", "TSLA", "INTC", "AMD", "AVGO", "ORCL", "NFLX", "CRM", "ADBE", "MU", "QCOM", "PLTR", "JPM", "BAC", "XOM"]


def implied_move_pct(spot, calls, puts):
    """Expected move in percent from the at-the-money straddle (what an option buyer pays to bet on a big move either way).
    calls / puts: [{strike, bid, ask, last}]"""
    def mid(o):
        b, a, l = o.get("bid") or 0, o.get("ask") or 0, o.get("last") or 0
        return (b + a) / 2 if b > 0 and a > 0 else l
    if not calls or not puts or not spot:
        return None
    strike = min((c["strike"] for c in calls), key=lambda k: abs(k - spot))
    c = next((x for x in calls if x["strike"] == strike), None)
    p = next((x for x in puts if x["strike"] == strike), None)
    if not c or not p:
        return None
    total = mid(c) + mid(p)
    return round(total / spot * 100, 2) if total > 0 else None


def surprise_ratio(move, implied):
    """(ratio, plain words) for an actual move against the expected one."""
    if not implied:
        return None, ""
    r = abs(move) / implied
    return round(r, 2), ("far bigger than the options market expected" if r >= 1.5 else "bigger than expected" if r >= 1.1 else
                         "about as expected" if r >= 0.7 else "smaller than expected")


# ══ Geopolitics headlines: everything that touches the two conflicts, ranked by importance and urgency ═══════════
_GEO_UA = ["ukraine", "ukrainian", "kyiv", "kiev", "kharkiv", "odesa", "donetsk", "zaporizhzhia", "donbas", "crimea", "zelensky", "zelenskyy", "putin", "kremlin", "moscow",
           "lavrov", "peskov", "medvedev", "kushner", "witkoff", "kellogg", "merz", "von der leyen", "macron", "starmer", "rutte", "stoltenberg", "nato", "russia", "russian",
           "ceasefire", "peace talks", "peace plan", "peace deal", "frozen assets", "reparations loan", "sanctions on russia", "russia sanctions", "rearmament", "defense spending",
           "defence spending", "defense budget", "defence budget", "patriot missile", "patriot system", "taurus", "tomahawk", "storm shadow", "f-35", "f-16", "gripen", "eurofighter",
           "rafale", "fighter jets", "fighter jet", "air defense", "air defence", "artillery shells", "ammunition", "drone attack", "nord stream", "black sea", "grain corridor",
           "european defense", "european defence", "defense stocks", "defence stocks", "arms deal", "arms order"]
_GEO_ME = ["iran", "iranian", "tehran", "khamenei", "pezeshkian", "araghchi", "irgc", "nuclear deal", "uranium enrichment", "houthi", "houthis", "ansar allah", "yemen", "red sea",
           "bab el-mandeb", "suez canal", "strait of hormuz", "hormuz", "persian gulf", "gulf of oman", "saudi", "riyadh", "aramco", "opec", "oil output", "production cut",
           "israel", "israeli", "netanyahu", "idf", "gaza", "hamas", "hostages", "hezbollah", "lebanon", "syria", "iraq", "qatar", "tanker attack", "tanker seized", "oil tanker",
           "shipping attack", "middle east", "jet fuel", "diesel price", "diesel prices", "kerosene", "refinery", "brent crude"]
_GEO_MEMBERS = {
    "G_RHM_DE": ["rheinmetall"], "G_BA_L": ["bae systems"], "G_HO_PA": ["thales"], "G_HAG_DE": ["hensoldt"], "G_KOG_OL": ["kongsberg"],
    "G_LDO_MI": ["leonardo spa", "leonardo shares", "leonardo stock", "leonardo earnings", "leonardo results", "italian defense group leonardo", "italy's leonardo"],
    "G_SAAB_B_ST": ["saab shares", "saab stock", "saab ab", "saab order", "saab wins", "saab results", "saab earnings", "gripen"],
    "G_LMT": ["lockheed"], "G_RTX": ["raytheon", "rtx corp"], "G_NOC": ["northrop"], "G_GD": ["general dynamics"], "G_LHX": ["l3harris"],
    "G_FRO": ["frontline plc", "frontline tanker", "frontline shares"], "G_STNG": ["scorpio tankers"], "G_DHT": ["dht holding"], "G_INSW": ["international seaways"],
    "G_TNK": ["teekay tankers"], "G_NAT": ["nordic american tankers"], "G_ZIM": ["zim shipping", "zim integrated"], "G_MAERSK_B_CO": ["maersk"],
    "G_DAL": ["delta air lines"], "G_UAL": ["united airlines"], "G_AAL": ["american airlines"], "G_LUV": ["southwest airlines"],
}
_GEO_EXTRA = {"G_TTF_F": ["european gas", "ttf gas", "gas prices in europe"], "G_ZW_F": ["wheat prices", "wheat price", "grain exports"], "G_HO_F": ["diesel price", "diesel prices"],
              "G_JETFUEL": ["jet fuel", "kerosene"], "G_TANKERS": ["tanker rates", "oil tanker"], "G_KSA": ["saudi stocks", "tadawul"], "G_EIS": ["tel aviv stock", "israeli stocks"],
              "G_EUDEF": ["european defense stocks", "european defence stocks"], "G_USDEF": ["us defense stocks", "pentagon contract", "pentagon budget", "defense contractors"],
              "G_AIRLINES": ["airline stocks", "airlines shares"]}
NEWS_TOPICS.update({"GEO_UA": _GEO_UA, "GEO_ME": _GEO_ME, **_GEO_MEMBERS, **_GEO_EXTRA})
for _bk, _ms in GEO_BASKETS.items():                                  # a basket is about its members, too
    NEWS_TOPICS[_bk] = sorted(set(NEWS_TOPICS.get(_bk, []) + [w for m in _ms for w in _GEO_MEMBERS.get(m, [])]))
for _k in list(NEWS_TOPICS):
    if _k not in _NEWS_RX or _k.startswith(("GEO_", "G_")):
        _NEWS_RX[_k] = re.compile(r"(?<![a-z0-9])(?:" + "|".join(re.escape(p) for p in NEWS_TOPICS[_k]) + r")(?![a-z0-9])", re.I)


def geo_topic_keys(cid):
    """Every headline topic that belongs to a conflict: the conflict itself, its baskets, members and the oil/gas prices around it."""
    G = GEO[cid]
    keys = {G["topic"]}
    for g in G["groups"]:
        if g.get("basket"):
            keys.add(g["basket"][0])
        keys |= {geo_key(s) for s, _, _ in g["members"]} | {k for k, _, _ in g.get("extra", [])} | set(g.get("refs", []))
    keys.discard("GOLD")
    return {k for k in keys if k in NEWS_TOPICS}


GEO_NEWS_QUERIES = [
    "Ukraine war", "Ukraine ceasefire talks", "Putin Kremlin", "Zelensky", "Kushner Russia", "Witkoff Putin", "Merz Ukraine", "von der Leyen Ukraine", "Macron Ukraine",
    "NATO summit", "NATO defense spending", "EU sanctions Russia", "Ukraine weapons aid", "fighter jets order", "defense stocks surge", "European defense stocks",
    "Rheinmetall", "Leonardo defense earnings", "Saab earnings", "BAE Systems", "Thales defence", "Hensoldt", "Kongsberg", "Lockheed Martin", "Northrop Grumman",
    "RTX Raytheon", "General Dynamics", "Pentagon contract", "European gas prices TTF", "wheat prices Black Sea",
    "Iran Trump", "Iran nuclear", "Israel Iran", "Houthi Red Sea shipping", "Strait of Hormuz", "Saudi Arabia OPEC", "OPEC+ output", "Gaza ceasefire", "oil price Brent",
    "oil tanker attack", "tanker rates", "Frontline tankers", "Scorpio Tankers", "Maersk Red Sea", "jet fuel prices", "diesel prices", "airlines fuel costs"]
GEO_NEWS_FEEDS = [("BBC World", "http://feeds.bbci.co.uk/news/world/rss.xml"), ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
                  ("The Guardian", "https://www.theguardian.com/world/rss"), ("DW", "https://rss.dw.com/rdf/rss-en-world"),
                  ("Defense News", "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml"), ("Breaking Defense", "https://breakingdefense.com/feed/"),
                  ("Kyiv Independent", "https://kyivindependent.com/feed/"), ("Times of Israel", "https://www.timesofisrael.com/feed/"),
                  ("CNBC World", "https://www.cnbc.com/id/100727362/device/rss/rss.html"), ("CNBC Energy", "https://www.cnbc.com/id/19836768/device/rss/rss.html")]


def geo_news_feeds():
    out = list(GEO_NEWS_FEEDS)
    for q in GEO_NEWS_QUERIES:
        out.append(("Google News", "https://news.google.com/rss/search?q=" + q.replace(" ", "+") + "+when:1d&hl=en-US&gl=US&ceid=US:en"))
    return out


_GEO_CATEGORIES = [
    ("Company results", ["earnings", "results", "profit", "quarterly", "guidance", "revenue", "backlog", "raises outlook", "operating income", "net income", "sales rose", "sales jump", "forecast"]),
    ("Defense orders & budgets", ["order", "contract", "budget", "spending", "jets", "fighter", "procure", "purchase", "aid package", "weapons package", "deliver", "arms deal"]),
    ("Sanctions", ["sanction", "frozen assets", "embargo", "tariff", "price cap"]),
    ("Diplomacy", ["talks", "ceasefire", "peace", "summit", "phone call", "call with", "meets", "meeting", "visit", "negotiat", "envoy", "diplomat", "agreement", "deal", "merz", "von der leyen", "macron", "starmer", "kushner", "witkoff", "kremlin", "says ukraine", "kyiv"]),
    ("Military", ["attack", "strike", "missile", "drone", "shelling", "troops", "killed", "offensive", "front line", "bombard", "intercept", "explosion", "invasion"]),
    ("Energy & shipping", ["oil", "crude", "brent", "diesel", "jet fuel", "kerosene", "tanker", "hormuz", "red sea", "shipping", "freight", "refinery", "opec", "gas price", "lng", "pipeline"]),
    ("Markets", ["shares", "stock", "stocks", "rally", "slump", "jump", "surge", "tumble"]),
]
_GEO_HIGH = ["ceasefire", "peace deal", "peace plan", "peace talks", "invasion", "nuclear", "missile", "drone attack", "strike on", "attack on", "sanctions", "blockade", "hormuz", "escalat",
             "killed", "summit", "merz", "von der leyen", "kyiv", "kremlin", "trump", "putin", "zelensky", "kushner", "witkoff", "netanyahu", "khamenei", "breaking", "troops", "red sea attack"]
_GEO_MED = ["order", "contract", "jets", "fighter", "aid package", "budget", "spending", "nato", "macron", "starmer", "opec", "output", "tanker", "red sea", "houthi",
            "oil price", "earnings", "results", "profit", "guidance", "record", "shares jump", "shares surge", "shares fall", "backlog", "raises"]


def geo_category(title, summary=""):
    t = (title + " " + (summary or "")).lower()
    for name, words in _GEO_CATEGORIES:
        if any(w in t for w in words):
            return name
    return "News"


def geo_impact(title, summary=""):
    t = (title + " " + (summary or "")).lower()
    return min(12.0, 1.0 + 3 * sum(1 for w in _GEO_HIGH if w in t) + 2 * sum(1 for w in _GEO_MED if w in t))


def geo_rank(impact, age_h, market_boost=0.0):
    """Importance × urgency: big, concrete headlines first, fading with age (half-life about 10 hours)."""
    return round(impact * (0.15 + 0.85 * 0.5 ** (age_h / 10.0)) * (1 + market_boost), 3)


def geo_urgency(age_h, impact):
    return "BREAKING" if age_h < 3 and impact >= 4 else "TODAY" if age_h < 18 else "EARLIER"


_CTX_WORDS = re.compile(r"\b(?:guidance|profit|results|earnings|orders?|defen[cs]e|shares|stock|eurofighter|helicopters?|aerospace|revenue|backlog|raises|outlook|sweden|swedish|italy|italian|quarter(?:ly)?|dividend|contract)\b", re.I)
_NEWS_CTX.update({"G_LDO_MI": (re.compile(r"\bleonardo\b(?!\s+dicaprio)", re.I), _CTX_WORDS), "G_SAAB_B_ST": (re.compile(r"\bsaab\b", re.I), _CTX_WORDS)})
for _cid, _G in GEO.items():
    for _g in _G["groups"]:
        _parents = [_G["topic"]] + ([_g["basket"][0]] if _g.get("basket") else [])
        for _s, _n, _u in _g["members"]:
            _NEWS_PARENTS.setdefault(geo_key(_s), []).extend(_parents)
        for _k, _n, _u in _g.get("extra", []):
            _NEWS_PARENTS.setdefault(_k, []).append(_G["topic"])
        if _g.get("basket"):
            _NEWS_PARENTS.setdefault(_g["basket"][0], []).append(_G["topic"])
NEWS_TOPICS.setdefault("G_LDO_MI", []); NEWS_TOPICS.setdefault("G_SAAB_B_ST", [])


# ══ Tomorrow: short-term warning signs ═══════════════════════════════════════════════════════
INDICATORS += [dict(key=k, name=n, group=None, source="yahoo", sym=sym, unit="", dec=1, kind="range", chg="pct", freq="daily", money=False, explain=ex)
               for k, n, sym, ex in [
    ("FUT_ES", "S&P 500 futures", "ES=F", "Where traders price the S&P 500 right now, also outside the stock market's hours."),
    ("FUT_NQ", "Nasdaq 100 futures", "NQ=F", "Where traders price the Nasdaq 100 right now, also outside the stock market's hours."),
    ("FUT_RTY", "Russell 2000 futures", "RTY=F", "Where traders price the small companies right now, also outside the stock market's hours.")]]
BY_KEY.update({i["key"]: i for i in INDICATORS})


MEGA_CAP = 1e12      # $1 trillion


def heat_stats(rows):
    """Breadth and weak spots from every stock of an index: [{t, s, i, c, p}] -> numbers for the 'Tomorrow' panel."""
    rows = [r for r in rows if r.get("p") is not None]
    n = len(rows)
    if n < 50:
        return None
    cap = sum(r["c"] for r in rows) or 1.0
    def avg(rs, w=False):
        return (sum(r["p"] * r["c"] for r in rs) / (sum(r["c"] for r in rs) or 1.0)) if w else sum(r["p"] for r in rs) / len(rs)
    groups = {}
    for r in rows:
        groups.setdefault(r["i"] or r["s"], []).append(r)
    inds = [{"name": k, "pct": round(avg(v, True), 2), "n": len(v)} for k, v in groups.items() if len(v) >= 4]
    sectors = {}
    for r in rows:
        sectors.setdefault(r["s"], []).append(r)
    big = sorted((r for r in rows if r["c"] >= MEGA_CAP), key=lambda r: -r["c"])
    big = [r for r in big if not (r["t"] == "GOOG" and any(x["t"] == "GOOGL" for x in big))]      # Alphabet counts once
    mega = [{"t": r["t"], "n": r["n"], "pct": round(r["p"], 2), "cap": round(r["c"] / 1e9)} for r in big]
    return {"mega": mega, "megaShare": round(100 * sum(r["c"] for r in big) / cap, 1), "megaAvg": round(avg(big, True), 2) if big else None,
            "n": n, "down": round(100 * sum(1 for r in rows if r["p"] < 0) / n, 1),
            "down2": round(100 * sum(1 for r in rows if r["p"] <= -2) / n, 1), "down2n": sum(1 for r in rows if r["p"] <= -2),
            "up2n": sum(1 for r in rows if r["p"] >= 2), "cap": round(avg(rows, True), 2), "eq": round(avg(rows), 2),
            "worst": sorted(inds, key=lambda x: x["pct"])[:5], "best": sorted(inds, key=lambda x: -x["pct"])[:3],
            "sectors": sorted(({"name": k, "pct": round(avg(v, True), 2)} for k, v in sectors.items()), key=lambda x: x["pct"])}


def build_tomorrow(chg, vix, vixts, sect_up, heat, earn, vix3=None, fng=None):
    """Turn the short-term numbers into warning signs. A sign 'hits' past a fixed, simple threshold. Gauge, not forecast."""
    signs = []
    def add(name, val, txt, hit, rule):
        signs.append({"name": name, "value": txt, "hit": bool(hit), "v": val, "rule": rule})
    for k, name, lim in [("FUT_ES", "S&P 500 futures", -0.7), ("FUT_NQ", "Nasdaq futures", -1.0), ("FUT_RTY", "Small-cap futures", -1.0)]:
        v = chg.get(k)
        if v is not None:
            add(name, v, f"{v:+.1f}%", v <= lim, f"Where traders price it now vs the last close. Red at {lim:g}% or worse.")
    if vix:
        add("VIX", vix[1], f"{vix[0]:.1f} ({vix[1]:+.0f}%)", vix[1] >= 10 or vix[0] >= 25, "The fear gauge. Red when it jumps 10% in a day or sits above 25.")
    if vixts is not None:
        calc = f"VIX {vix[0]:.1f} ÷ 3-month VIX {vix3:.1f} = {vixts:.2f}. " if vix and vix3 else ""
        add("Fear now ÷ fear in 3 months", vixts, f"{vixts:.2f}", vixts >= 1.0,
            calc + "Normally below 1: traders expect more swings later than now. Above 1.00 they pay more for protection today than for the next 3 months, which marks acute stress.")
    if fng and fng.get("score") is not None:
        sc = fng["score"]
        word = (fng.get("rating") or "").replace("_", " ") or ("fear" if sc < 45 else "greed" if sc > 55 else "neutral")
        add("Fear & Greed" + (" (estimate)" if fng.get("estimate") else ""), sc, f"{sc:.0f} · {word}", sc <= 25,
            "0 = extreme fear, 100 = extreme greed" + (" (CNN's index)." if not fng.get("estimate") else " (our own estimate; CNN's was not reachable).") + " Red at 25 or below.")
    if sect_up is not None:
        add("Sectors up", sect_up, f"{sect_up:.0f} of 11", sect_up <= 3, "How many of the 11 sectors rose today. Red at 3 or fewer: a broad sell-off.")
    if heat and heat.get("mega"):
        m = heat["mega"]; worst = min(m, key=lambda x: x["pct"])
        add("Mega caps ($1 trillion+) average", heat["megaAvg"], f"{heat['megaAvg']:+.1f}%", heat["megaAvg"] <= -2,
            f"The {len(m)} companies worth over $1 trillion are {heat['megaShare']:.0f}% of the S&P 500: when they fall, the index falls. Red at −2% or worse.")
        add("Biggest mega-cap fall", worst["pct"], f"{worst['t']} {worst['pct']:+.1f}%", worst["pct"] <= -5, "The weakest company over $1 trillion today. Red at −5% or worse.")
    if heat:
        add("S&P 500 stocks falling", heat["down"], f"{heat['down']:.0f}%", heat["down"] >= 70, "Share of the 500 stocks below yesterday's close. Red at 70% or more.")
        add("Down more than 2%", heat["down2"], f"{heat['down2n']} stocks", heat["down2"] >= 12, "Stocks that lost over 2% today. Red when it is 12% of the index or more.")
    if earn is not None:
        ah = [r for r in earn if r.get("when") == "after hours" and r["move"] <= -4]
        add("Big names after hours", -len(ah), f"{len(ah)} down 4%+" if ah else "none", bool(ah), "Large companies falling 4% or more after the close.")
    hits, total = sum(1 for s in signs if s["hit"]), len(signs)
    level = "high" if total and hits >= max(3, total * 0.4) else "elevated" if hits >= 2 else "low"
    return {"level": level, "hits": hits, "total": total, "signs": signs}


# ══ War maps: open data on a map ══════════════════════════════════════════════════════════════
# (name, lat, lon, kind, words that identify it in a headline)
GEO_SITES = {
    "ua": [
        ("Moscow refinery (Kapotnya)", 55.65, 37.80, "refinery", ["moscow refinery", "kapotnya"]),
        ("Ryazan refinery", 54.60, 39.83, "refinery", ["ryazan"]),
        ("Volgograd refinery", 48.67, 44.45, "refinery", ["volgograd refinery", "volgograd oil", "volgograd"]),
        ("Saratov refinery", 51.50, 45.90, "refinery", ["saratov"]),
        ("Syzran refinery", 53.17, 48.50, "refinery", ["syzran"]),
        ("Novokuibyshevsk refinery", 53.10, 49.90, "refinery", ["novokuibyshevsk"]),
        ("Kirishi refinery", 59.45, 32.05, "refinery", ["kirishi"]),
        ("Yaroslavl refinery", 57.60, 39.80, "refinery", ["yaroslavl"]),
        ("Tuapse refinery and port", 44.10, 39.07, "refinery", ["tuapse"]),
        ("Afipsky refinery", 44.87, 38.82, "refinery", ["afipsky", "afipskiy"]),
        ("Ilsky refinery", 44.85, 38.55, "refinery", ["ilsky", "ilskiy"]),
        ("Novoshakhtinsk refinery", 47.80, 39.90, "refinery", ["novoshakhtinsk"]),
        ("Ukhta refinery", 63.60, 53.70, "refinery", ["ukhta"]),
        ("Orsk refinery", 51.20, 58.60, "refinery", ["orsk"]),
        ("Salavat refinery", 53.40, 55.90, "refinery", ["salavat"]),
        ("Ufa refineries", 54.70, 55.90, "refinery", ["ufa refinery", "bashneft"]),
        ("Nizhnekamsk refinery", 55.60, 51.80, "refinery", ["nizhnekamsk", "taneco"]),
        ("Omsk refinery", 55.00, 73.20, "refinery", ["omsk"]),
        ("Kremenchuk refinery (Ukraine)", 49.10, 33.40, "refinery", ["kremenchuk"]),
        ("Primorsk oil port", 60.35, 28.67, "terminal", ["primorsk"]),
        ("Ust-Luga port", 59.67, 28.40, "terminal", ["ust-luga", "ust luga"]),
        ("Novorossiysk / CPC terminal", 44.70, 37.80, "terminal", ["novorossiysk", "cpc terminal", "caspian pipeline"]),
        ("Taman port", 45.20, 36.70, "terminal", ["taman"]),
        ("Crimean bridge (Kerch)", 45.30, 36.51, "chokepoint", ["kerch", "crimean bridge", "crimea bridge"]),
    ],
    "me": [
        ("Abqaiq (Saudi Arabia)", 25.94, 49.68, "refinery", ["abqaiq"]),
        ("Ras Tanura (Saudi Arabia)", 26.64, 50.16, "refinery", ["ras tanura"]),
        ("Jubail (Saudi Arabia)", 27.00, 49.65, "refinery", ["jubail"]),
        ("Yanbu (Saudi Arabia)", 24.09, 38.06, "refinery", ["yanbu"]),
        ("Jizan refinery (Saudi Arabia)", 16.90, 42.60, "refinery", ["jizan", "jazan"]),
        ("Ras Laffan (Qatar)", 25.90, 51.55, "refinery", ["ras laffan"]),
        ("Ruwais (UAE)", 24.10, 52.70, "refinery", ["ruwais"]),
        ("Fujairah (UAE)", 25.12, 56.35, "terminal", ["fujairah"]),
        ("Jebel Ali (UAE)", 25.00, 55.06, "terminal", ["jebel ali"]),
        ("Abadan refinery (Iran)", 30.34, 48.28, "refinery", ["abadan"]),
        ("Bandar Abbas refinery (Iran)", 27.15, 56.10, "refinery", ["bandar abbas"]),
        ("Kharg Island (Iran)", 29.24, 50.32, "terminal", ["kharg"]),
        ("South Pars / Asaluyeh (Iran)", 27.48, 52.60, "refinery", ["south pars", "asaluyeh", "assaluyeh"]),
        ("Basra oil terminal (Iraq)", 29.70, 48.80, "terminal", ["basra"]),
        ("Haifa refinery (Israel)", 32.80, 35.00, "refinery", ["haifa"]),
        ("Ashdod / Ashkelon (Israel)", 31.75, 34.60, "refinery", ["ashdod", "ashkelon"]),
        ("Ras Isa / Hodeidah (Yemen)", 15.10, 42.90, "terminal", ["hodeidah", "hudaydah", "ras isa"]),
        ("Aden refinery (Yemen)", 12.80, 44.95, "refinery", ["aden refinery", "aden"]),
        ("Strait of Hormuz", 26.57, 56.25, "chokepoint", ["hormuz"]),
        ("Bab-el-Mandeb (Yemen – Djibouti/Eritrea)", 12.60, 43.30, "chokepoint", ["bab-el-mandeb", "bab el-mandeb", "bab al-mandab", "bab el mandeb", "mandab"]),
        ("Suez Canal", 30.00, 32.55, "chokepoint", ["suez"]),
    ],
}
GEO_VIEW = {"ua": [49.0, 35.0, 5], "me": [24.0, 47.0, 4]}
GEO_INCIDENT_Q = {
    "ua": ['(Ukraine OR Russia) (drone OR missile OR strike OR shelling OR refinery) sourcelang:eng', 'Ukraine drone attack', 'Ukraine'],
    "me": ['(Iran OR Houthi OR Yemen OR Israel OR "Red Sea" OR Hormuz OR tanker OR refinery) (attack OR strike OR missile OR drone) sourcelang:eng', 'Houthi attack', 'Iran strike'],
}
_ATTACK_RX = re.compile(r"\b(?:drone|drones|strike[sd]?|struck|attack(?:s|ed)?|hit|hits|fire|blaze|explosion|explosions|missile|missiles|shell(?:ed|ing)|blast|bomb(?:ed|ing)?|halt(?:s|ed)?\s+(?:operations|output|production))\b", re.I)


def site_status(cid, headlines, now=None):
    """The sites of a conflict, each marked 'hit' when a headline of the last 7 days names it together with an attack word."""
    now = now or time.time()
    out = []
    for name, lat, lon, kind, words in GEO_SITES[cid]:
        hit = None
        for h in headlines:
            if now - h["ts"] > 7 * 86400:
                continue
            t = (h["title"] + " " + (h.get("summary") or "")).lower()
            if any(w in t for w in words) and _ATTACK_RX.search(t):
                if hit is None or h["ts"] > hit["ts"]:
                    hit = {"title": h["title"], "link": h["link"], "ts": h["ts"], "source": h["source"]}
        out.append({"name": name, "lat": lat, "lon": lon, "kind": kind, "hit": hit})
    return out


def _simplify(coords, step):
    return coords[::step] if len(coords) > 4 * step else coords


def _front_kind(name, props):
    """What a DeepStateMap layer is: 'occ' (held by Russia), 'occ0' (held since before 2022), 'gray' (contested) or None (liberated / other)."""
    n = name.lower()
    if any(w in n for w in ("liberat", "звільн", "звiльн", "unoccupied", "ukrain")):
        return None
    if any(w in n for w in ("gray", "grey", "сір", "contested")):
        return "gray"
    if any(w in n for w in ("before", "до 2022", "до 24", "crimea", "2014")):
        return "occ0"
    if any(w in n for w in ("occupied", "окуп", "russia")):
        return "occ"
    fill = str(props.get("fill") or props.get("fillColor") or "").lower()
    if fill.startswith("#") and len(fill) >= 7:
        try:
            r, g, b = int(fill[1:3], 16), int(fill[3:5], 16), int(fill[5:7], 16)
            if r > 1.4 * g and r > 1.4 * b:
                return "occ"
        except ValueError:
            pass
    return None


def parse_frontline(raw):
    """DeepStateMap 'last' snapshot -> [{t: 'poly'|'line', k: kind, c: [[lat, lon], ...], n: name}] without the liberated areas.
    Falls back to every polygon when no layer can be recognised."""
    data = json.loads(raw)
    fc = data.get("map") if isinstance(data, dict) and isinstance(data.get("map"), dict) else data
    feats = fc.get("features", []) if isinstance(fc, dict) else []
    def ring(r):
        pts = [[p[1], p[0]] for p in r if len(p) >= 2]
        return [[round(a, 3), round(b, 3)] for a, b in _simplify(pts, 2 if len(pts) > 600 else 1)]
    kept, every = [], []
    for f in feats:
        g, props = f.get("geometry") or {}, f.get("properties") or {}
        name = str(props.get("name") or "")
        t, c = g.get("type"), g.get("coordinates")
        kind = _front_kind(name, props)
        polys = [c[0]] if t == "Polygon" and c else [p[0] for p in c if p] if t == "MultiPolygon" and c else []
        for r in polys:
            row = {"t": "poly", "k": kind or "occ", "c": ring(r), "n": name[:60]}
            every.append(row)
            if kind:
                kept.append(row)
        if t == "LineString" and c and kind:
            kept.append({"t": "line", "k": kind, "c": ring(c), "n": name[:60]})
    return kept or every


def parse_gdelt_points(raw, limit=250):
    """GDELT GEO 2.0 GeoJSON -> [{lat, lon, n, html}]"""
    try:
        data = json.loads(raw)
    except ValueError:
        raise ValueError("not JSON: " + raw[:90].replace("\n", " "))
    out = []
    for f in data.get("features", []):
        g, p = f.get("geometry") or {}, f.get("properties") or {}
        c = g.get("coordinates")
        if g.get("type") == "Point" and c and len(c) >= 2:
            out.append({"lat": round(c[1], 3), "lon": round(c[0], 3), "n": str(p.get("name") or ""), "count": p.get("count") or 1,
                        "html": re.sub(r"<[^>]+>", " ", str(p.get("html") or ""))[:300].strip()})
    out.sort(key=lambda x: -(x["count"] or 0))
    return out[:limit]


# Ship traffic through the straits: IMF PortWatch (satellite AIS), open, no key
PORTWATCH_URL = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0/query"
PORTWATCH_STRAITS = {"me": [("Strait of Hormuz", "hormuz"), ("Bab el-Mandeb", "mandeb"), ("Suez Canal", "suez")], "ua": [("Bosporus (Black Sea exit)", "bosp")]}


def parse_portwatch(raw):
    """ArcGIS JSON -> {matchword: [[YYYY-MM-DD, ships, tankers], ...]} oldest first."""
    data = json.loads(raw)
    if data.get("error"):
        raise ValueError(str(data["error"])[:140])
    out = {}
    for f in data.get("features", []):
        a = {k.lower(): v for k, v in (f.get("attributes") or {}).items()}
        name = str(a.get("portname") or "").lower()
        ts = next((a[k] for k in a if k == "date" or k.endswith("date")), None)
        if not name or ts is None:
            continue
        tot = next((a[k] for k in a if "total" in k and isinstance(a[k], (int, float))), None)
        tan = next((a[k] for k in a if "tanker" in k and isinstance(a[k], (int, float))), None)
        if tot is None:
            continue
        try:
            day = ts[:10] if isinstance(ts, str) and "-" in ts[:5] + ts[4:6] else datetime.utcfromtimestamp(float(ts) / 1000).strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            continue
        out.setdefault(name, []).append([day, tot, tan or 0])
    for v in out.values():
        v.sort()
    return out


def ship_summary(series, label):
    """Last day, 7-day and 30-day averages from a [[day, ships, tankers]] list."""
    if len(series) < 10:
        return None
    ships = [r[1] for r in series]
    a7, a30 = sum(ships[-7:]) / 7, sum(ships[-37:-7]) / max(1, len(ships[-37:-7]))
    return {"name": label, "last": ships[-1], "day": series[-1][0], "a7": round(a7, 1), "a30": round(a30, 1), "chg": round((a7 / a30 - 1) * 100, 1) if a30 else None,
            "tankers7": round(sum(r[2] for r in series[-7:]) / 7, 1), "spark": ships[-60:]}


# Incidents read from the app's own headlines: a place named together with an attack word
GEO_PLACES = {
    "ua": [("Kyiv", 50.45, 30.52, ["kyiv", "kiev"]), ("Kharkiv", 49.99, 36.23, ["kharkiv", "kharkov"]), ("Odesa", 46.48, 30.73, ["odesa", "odessa"]),
           ("Dnipro", 48.46, 35.05, ["dnipro"]), ("Zaporizhzhia", 47.84, 35.14, ["zaporizhzhia", "zaporizhia", "zaporozhye"]), ("Kherson", 46.64, 32.62, ["kherson"]),
           ("Mykolaiv", 46.97, 32.00, ["mykolaiv", "nikolaev"]), ("Sumy", 50.91, 34.80, ["sumy"]), ("Chernihiv", 51.50, 31.30, ["chernihiv"]), ("Lviv", 49.84, 24.03, ["lviv"]),
           ("Poltava", 49.59, 34.55, ["poltava"]), ("Kramatorsk", 48.74, 37.58, ["kramatorsk"]), ("Sloviansk", 48.87, 37.62, ["sloviansk"]), ("Pokrovsk", 48.28, 37.18, ["pokrovsk"]),
           ("Donetsk", 48.00, 37.80, ["donetsk"]), ("Luhansk", 48.57, 39.31, ["luhansk"]), ("Bakhmut", 48.60, 38.00, ["bakhmut"]), ("Kupiansk", 49.71, 37.62, ["kupiansk"]),
           ("Kursk", 51.73, 36.19, ["kursk"]), ("Belgorod", 50.60, 36.60, ["belgorod"]), ("Bryansk", 53.25, 34.37, ["bryansk"]), ("Rostov", 47.23, 39.72, ["rostov"]),
           ("Voronezh", 51.67, 39.20, ["voronezh"]), ("Moscow", 55.75, 37.62, ["moscow"]), ("St Petersburg", 59.93, 30.34, ["st petersburg", "st. petersburg", "saint petersburg"]),
           ("Crimea / Sevastopol", 44.60, 33.50, ["crimea", "sevastopol"]), ("Mariupol", 47.10, 37.55, ["mariupol"]), ("Kryvyi Rih", 47.91, 33.39, ["kryvyi rih"]),
           ("Vinnytsia", 49.23, 28.47, ["vinnytsia"]), ("Black Sea", 43.5, 34.0, ["black sea"]), ("Donbas", 48.3, 38.0, ["donbas", "donbass"])],
    "me": [("Tehran", 35.69, 51.39, ["tehran"]), ("Isfahan", 32.65, 51.67, ["isfahan", "esfahan"]), ("Natanz", 33.72, 51.73, ["natanz"]), ("Tabriz", 38.08, 46.29, ["tabriz"]),
           ("Gaza", 31.50, 34.47, ["gaza"]), ("Rafah", 31.30, 34.25, ["rafah"]), ("Tel Aviv", 32.08, 34.78, ["tel aviv"]), ("Jerusalem", 31.77, 35.21, ["jerusalem"]),
           ("Beirut", 33.89, 35.50, ["beirut"]), ("Southern Lebanon", 33.30, 35.40, ["south lebanon", "southern lebanon", "hezbollah"]), ("Damascus", 33.50, 36.30, ["damascus"]),
           ("Aleppo", 36.20, 37.16, ["aleppo"]), ("Baghdad", 33.30, 44.40, ["baghdad"]), ("Erbil", 36.19, 44.01, ["erbil"]), ("Sanaa", 15.37, 44.19, ["sanaa", "sana'a"]),
           ("Hodeidah", 14.80, 42.95, ["hodeidah", "hudaydah"]), ("Aden", 12.80, 45.00, ["aden"]), ("Marib", 15.46, 45.33, ["marib"]), ("Riyadh", 24.70, 46.70, ["riyadh"]),
           ("Jeddah", 21.50, 39.20, ["jeddah"]), ("Doha", 25.29, 51.53, ["doha"]), ("Dubai", 25.20, 55.27, ["dubai"]), ("Abu Dhabi", 24.45, 54.38, ["abu dhabi"]),
           ("Red Sea", 20.00, 38.50, ["red sea"]), ("Gulf of Aden", 12.00, 47.00, ["gulf of aden"]), ("Persian Gulf", 27.00, 51.00, ["persian gulf", "arabian gulf"]),
           ("Strait of Hormuz", 26.57, 56.25, ["hormuz"]), ("Bab-el-Mandeb", 12.60, 43.30, ["mandeb", "mandab"]), ("Houthi territory", 15.5, 44.0, ["houthi"])],
}


def news_incidents(cid, rows, now=None):
    """[{lat, lon, n, count, items: [{t, l, s, ts}]}]: headlines of the last 7 days that name a place together with an attack word."""
    now = now or time.time()
    places = GEO_PLACES[cid] + [(n, la, lo, w) for n, la, lo, k, w in GEO_SITES[cid]]
    seen, out = set(), {}
    for h in rows:
        if now - h["ts"] > 7 * 86400 or h["title"] in seen:
            continue
        t = (h["title"] + " " + (h.get("summary") or "")).lower()
        if not _ATTACK_RX.search(t):
            continue
        seen.add(h["title"])
        title_l = h["title"].lower()
        for name, la, lo, words in places:
            if any(w in title_l for w in words):
                e = out.setdefault(name, {"lat": la, "lon": lo, "n": name, "count": 0, "items": []})
                e["count"] += 1
                e["items"].append({"t": h["title"], "l": h["link"], "s": h["source"], "ts": h["ts"]})
    for e in out.values():
        e["items"] = sorted(e["items"], key=lambda x: -x["ts"])[:4]
    return sorted(out.values(), key=lambda e: -e["count"])


PORTWATCH_ROOT = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer"
PORTWATCH_QUERIES = ["PortWatch chokepoints daily", "Daily Chokepoints Data", "chokepoint transit calls daily", "PortWatch daily ports"]
STRAIT_NAMES = {"hormuz": "Strait of Hormuz", "mandeb": "Bab el-Mandeb Strait", "suez": "Suez Canal", "bosp": "Bosporus Strait"}


def _pick(fields, *needles):
    for f in fields:
        n = f.lower()
        if all(x in n for x in needles):
            return f
    return None


def portwatch_roots(get):
    """Known address first, then whatever ArcGIS Online lists under the PortWatch search terms."""
    from urllib.parse import quote
    roots, seen_titles = [PORTWATCH_ROOT], []
    for q in PORTWATCH_QUERIES:
        try:
            res = json.loads(get("https://www.arcgis.com/sharing/rest/search?f=json&num=15&q=" + quote(q + ' type:"Feature Service"'))).get("results", [])
        except Exception:
            continue
        for it in res:
            u = (it.get("url") or "").rstrip("/")
            if u.endswith("FeatureServer") and u not in roots:
                roots.append(u)
                seen_titles.append(it.get("title", "?"))
    return roots[:14], seen_titles


def portwatch_series(roots, words, get, days=100):
    """Daily ship counts for the straits from whichever IMF PortWatch layer has the right fields (port name, date, total, tankers).
    Returns ({lower-case name: [[day, ships, tankers]]}, [what went wrong])."""
    from urllib.parse import quote
    why = []
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    for root in roots:
        for layer in range(4):
            url = f"{root}/{layer}"
            try:
                meta = json.loads(get(url + "?f=json"))
                if meta.get("error"):
                    break
                fields = [f["name"] for f in meta.get("fields", [])]
                fdate, fname = _pick(fields, "date"), _pick(fields, "portname") or _pick(fields, "name")
                ftot = _pick(fields, "n_total") or _pick(fields, "total")
                if not (fdate and fname and ftot):
                    why.append(f"{root.split('/services/')[-1].split('/')[0][:34]}/{layer}: {', '.join(fields)[:70]}")
                    continue
                names = ",".join("'" + STRAIT_NAMES[w] + "'" for w in words)
                wheres = [f"{fname} IN ({names}) AND {fdate} >= timestamp '{since} 00:00:00'",
                          "(" + " OR ".join(f"{fname} LIKE '%{w.capitalize()}%'" for w in words) + f") AND {fdate} >= timestamp '{since} 00:00:00'",
                          f"{fname} IN ({names})"]
                for w_ in wheres:
                    try:
                        got = parse_portwatch(get(f"{url}/query?f=json&returnGeometry=false&outFields=*&resultRecordCount=2000&orderByFields={fdate}%20DESC&where={quote(w_)}"))
                        if got:
                            return got, why
                    except Exception as e:
                        why.append(f"query: {type(e).__name__}: {e}"[:120])
            except Exception as e:
                why.append(f"{root.split('/services/')[-1][:30]}: {type(e).__name__}: {e}"[:110])
                break
    return {}, why


def parse_portwatch(raw):
    """ArcGIS JSON -> {matchword: [[YYYY-MM-DD, ships, tankers], ...]} oldest first."""
    data = json.loads(raw)
    if data.get("error"):
        raise ValueError(str(data["error"])[:140])
    out = {}
    for f in data.get("features", []):
        a = {k.lower(): v for k, v in (f.get("attributes") or {}).items()}
        name = str(a.get("portname") or "").lower()
        ts = a.get("date")
        if not name or ts is None:
            continue
        tot = next((a[k] for k in a if "total" in k and isinstance(a[k], (int, float))), None)
        tan = next((a[k] for k in a if "tanker" in k and isinstance(a[k], (int, float))), None)
        if tot is None:
            continue
        day = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
        out.setdefault(name, []).append([day, tot, tan or 0])
    for v in out.values():
        v.sort()
    return out


def ship_summary(series, label):
    """Last day, 7-day and 30-day averages from a [[day, ships, tankers]] list."""
    if len(series) < 10:
        return None
    ships = [r[1] for r in series]
    a7, a30 = sum(ships[-7:]) / 7, sum(ships[-37:-7]) / max(1, len(ships[-37:-7]))
    return {"name": label, "last": ships[-1], "day": series[-1][0], "a7": round(a7, 1), "a30": round(a30, 1), "chg": round((a7 / a30 - 1) * 100, 1) if a30 else None,
            "tankers7": round(sum(r[2] for r in series[-7:]) / 7, 1), "spark": ships[-60:]}


# Incidents read from the app's own headlines: a place named together with an attack word
GEO_PLACES = {
    "ua": [("Kyiv", 50.45, 30.52, ["kyiv", "kiev"]), ("Kharkiv", 49.99, 36.23, ["kharkiv", "kharkov"]), ("Odesa", 46.48, 30.73, ["odesa", "odessa"]),
           ("Dnipro", 48.46, 35.05, ["dnipro"]), ("Zaporizhzhia", 47.84, 35.14, ["zaporizhzhia", "zaporizhia", "zaporozhye"]), ("Kherson", 46.64, 32.62, ["kherson"]),
           ("Mykolaiv", 46.97, 32.00, ["mykolaiv", "nikolaev"]), ("Sumy", 50.91, 34.80, ["sumy"]), ("Chernihiv", 51.50, 31.30, ["chernihiv"]), ("Lviv", 49.84, 24.03, ["lviv"]),
           ("Poltava", 49.59, 34.55, ["poltava"]), ("Kramatorsk", 48.74, 37.58, ["kramatorsk"]), ("Sloviansk", 48.87, 37.62, ["sloviansk"]), ("Pokrovsk", 48.28, 37.18, ["pokrovsk"]),
           ("Donetsk", 48.00, 37.80, ["donetsk"]), ("Luhansk", 48.57, 39.31, ["luhansk"]), ("Bakhmut", 48.60, 38.00, ["bakhmut"]), ("Kupiansk", 49.71, 37.62, ["kupiansk"]),
           ("Kursk", 51.73, 36.19, ["kursk"]), ("Belgorod", 50.60, 36.60, ["belgorod"]), ("Bryansk", 53.25, 34.37, ["bryansk"]), ("Rostov", 47.23, 39.72, ["rostov"]),
           ("Voronezh", 51.67, 39.20, ["voronezh"]), ("Moscow", 55.75, 37.62, ["moscow"]), ("St Petersburg", 59.93, 30.34, ["st petersburg", "st. petersburg", "saint petersburg"]),
           ("Crimea / Sevastopol", 44.60, 33.50, ["crimea", "sevastopol"]), ("Mariupol", 47.10, 37.55, ["mariupol"]), ("Kryvyi Rih", 47.91, 33.39, ["kryvyi rih"]),
           ("Vinnytsia", 49.23, 28.47, ["vinnytsia"]), ("Black Sea", 43.5, 34.0, ["black sea"]), ("Donbas", 48.3, 38.0, ["donbas", "donbass"])],
    "me": [("Tehran", 35.69, 51.39, ["tehran"]), ("Isfahan", 32.65, 51.67, ["isfahan", "esfahan"]), ("Natanz", 33.72, 51.73, ["natanz"]), ("Tabriz", 38.08, 46.29, ["tabriz"]),
           ("Gaza", 31.50, 34.47, ["gaza"]), ("Rafah", 31.30, 34.25, ["rafah"]), ("Tel Aviv", 32.08, 34.78, ["tel aviv"]), ("Jerusalem", 31.77, 35.21, ["jerusalem"]),
           ("Beirut", 33.89, 35.50, ["beirut"]), ("Southern Lebanon", 33.30, 35.40, ["south lebanon", "southern lebanon", "hezbollah"]), ("Damascus", 33.50, 36.30, ["damascus"]),
           ("Aleppo", 36.20, 37.16, ["aleppo"]), ("Baghdad", 33.30, 44.40, ["baghdad"]), ("Erbil", 36.19, 44.01, ["erbil"]), ("Sanaa", 15.37, 44.19, ["sanaa", "sana'a"]),
           ("Hodeidah", 14.80, 42.95, ["hodeidah", "hudaydah"]), ("Aden", 12.80, 45.00, ["aden"]), ("Marib", 15.46, 45.33, ["marib"]), ("Riyadh", 24.70, 46.70, ["riyadh"]),
           ("Jeddah", 21.50, 39.20, ["jeddah"]), ("Doha", 25.29, 51.53, ["doha"]), ("Dubai", 25.20, 55.27, ["dubai"]), ("Abu Dhabi", 24.45, 54.38, ["abu dhabi"]),
           ("Red Sea", 20.00, 38.50, ["red sea"]), ("Gulf of Aden", 12.00, 47.00, ["gulf of aden"]), ("Persian Gulf", 27.00, 51.00, ["persian gulf", "arabian gulf"]),
           ("Strait of Hormuz", 26.57, 56.25, ["hormuz"]), ("Bab-el-Mandeb", 12.60, 43.30, ["mandeb", "mandab"]), ("Houthi territory", 15.5, 44.0, ["houthi"])],
}


def news_incidents(cid, rows, now=None):
    """[{lat, lon, n, count, items: [{t, l, s, ts}]}]: headlines of the last 7 days that name a place together with an attack word."""
    now = now or time.time()
    places = GEO_PLACES[cid] + [(n, la, lo, w) for n, la, lo, k, w in GEO_SITES[cid]]
    seen, out = set(), {}
    for h in rows:
        if now - h["ts"] > 7 * 86400 or h["title"] in seen:
            continue
        t = (h["title"] + " " + (h.get("summary") or "")).lower()
        if not _ATTACK_RX.search(t):
            continue
        seen.add(h["title"])
        title_l = h["title"].lower()
        for name, la, lo, words in places:
            if any(w in title_l for w in words):
                e = out.setdefault(name, {"lat": la, "lon": lo, "n": name, "count": 0, "items": []})
                e["count"] += 1
                e["items"].append({"t": h["title"], "l": h["link"], "s": h["source"], "ts": h["ts"]})
    for e in out.values():
        e["items"] = sorted(e["items"], key=lambda x: -x["ts"])[:4]
    return sorted(out.values(), key=lambda e: -e["count"])


PORTWATCH_ROOT = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer"
PORTWATCH_SEARCH = "https://www.arcgis.com/sharing/rest/search?f=json&num=10&q=" + "Daily%20Chokepoints%20Data%20type%3A%22Feature%20Service%22"
STRAIT_NAMES = {"hormuz": "Strait of Hormuz", "mandeb": "Bab el-Mandeb Strait", "suez": "Suez Canal", "bosp": "Bosporus Strait"}


def _pick(fields, *needles):
    for f in fields:
        n = f.lower()
        if all(x in n for x in needles):
            return f
    return None


def portwatch_series(roots, words, get, days=100):
    """Daily ship counts for the straits, from whichever IMF PortWatch layer answers. Looks at the layer's own field list first
    so a changed field name does not break it. Returns ({word: [[day, ships, tankers]]}, [what went wrong])."""
    why = []
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    for root in roots:
        try:
            meta = json.loads(get(root + "/0?f=json"))
            if meta.get("error"):
                raise ValueError(str(meta["error"])[:100])
            fields = [f["name"] for f in meta.get("fields", [])]
            fdate, fname = _pick(fields, "date") or "date", _pick(fields, "portname") or _pick(fields, "name")
            ftot, ftan = _pick(fields, "n_total") or _pick(fields, "total"), _pick(fields, "tanker")
            if not (fname and ftot):
                raise ValueError("layer has fields " + ", ".join(fields)[:140])
            names = ",".join("'" + STRAIT_NAMES[w] + "'" for w in words)
            wheres = [f"{fname} IN ({names}) AND {fdate} >= timestamp '{since} 00:00:00'",
                      "(" + " OR ".join(f"{fname} LIKE '%{w.capitalize()}%'" for w in words) + f") AND {fdate} >= timestamp '{since} 00:00:00'",
                      f"{fname} IN ({names})"]
            from urllib.parse import quote
            for w_ in wheres:
                try:
                    q = f"{root}/0/query?f=json&returnGeometry=false&outFields=*&resultRecordCount=2000&orderByFields={fdate}%20DESC&where={quote(w_)}"
                    got = parse_portwatch(get(q))
                    if got:
                        return got, why
                    why.append("query answered but had no rows")
                except Exception as e:
                    why.append(f"{type(e).__name__}: {e}"[:150])
        except Exception as e:
            why.append(f"{root.split('/services/')[-1][:40]}: {type(e).__name__}: {e}"[:170])
    return {}, why




# ══ Stocks: your own holdings and watch list ═════════════════════════════════════════════════
def fetch_yahoo_vol(symbols, period="1y"):
    """{symbol: {"pts": daily closes, "vol": [volumes, same days]}}"""
    import yfinance as yf
    df = yf.download(symbols, period=period, interval="1d", auto_adjust=False, progress=False, group_by="ticker", threads=False)
    out = {}
    for s in symbols:
        try:
            col, vol = df[s]["Close"], df[s]["Volume"]
            pts, vs = [], []
            for (i, v), (_, vv) in zip(col.items(), vol.items()):
                if v == v and v:
                    pts.append([i.strftime("%Y-%m-%d"), float(v)])
                    vs.append(float(vv) if vv == vv else 0.0)
            if pts:
                out[s] = {"pts": pts, "vol": vs}
        except Exception as e:
            log.warning("Yahoo %s: %s", s, e)
    return out


def stock_summary(ticker, pts):
    ind = dict(key=ticker, name=ticker, group="stocks", source="yahoo", sym=ticker, unit="$", dec=2, kind="range", chg="pct", freq="daily", money=True, explain="")
    return summarize(ind, pts)


def stock_flags(s, vol_ratio, news_n, earn_days):
    """What makes a stock unusual right now: [(label, weight)]. Score is the sum; 2 or more counts as unusual."""
    flags = []
    if s.get("horizon") and s["horizon"] != "at an extreme":
        z = max(abs(s.get("z1") or 0), abs(s.get("zw") or 0), abs(s.get("zm") or 0))
        flags.append((f"Unusual move {s['horizon']} ({z:.1f}× a typical move)", min(5.0, 1.0 + z / 2)))
    if news_n:
        flags.append((f"{news_n} new headline{'s' if news_n > 1 else ''}", 2.0))
    if earn_days is not None and -1 <= earn_days <= 2:
        flags.append(("Reports today" if earn_days == 0 else "Reported yesterday" if earn_days == -1 else f"Reports in {earn_days} day{'s' if earn_days > 1 else ''}", 2.0))
    if s.get("pos") is not None and s.get("hi") and s["value"] >= s["hi"] * 0.995:
        flags.append(("At its 52-week high", 1.5))
    elif s.get("lo") and s["value"] <= s["lo"] * 1.005:
        flags.append(("At its 52-week low", 1.5))
    if vol_ratio and vol_ratio >= 2:
        flags.append((f"Volume {vol_ratio:.1f}× normal", 1.5))
    return flags


# Sell-offs: everything about fear, volatility and big falls belongs to the volatility indicators
_SELLOFF = ["sell-off", "selloff", "sell off", "plunge", "plunges", "plunged", "tumble", "tumbles", "tumbled", "rout", "crash", "worst day", "worst week", "panic", "slump", "slumps",
            "rattled", "risk-off", "risk off", "stocks slide", "stocks fall", "stocks drop", "stocks sink", "wall street falls", "wall street drops", "dow drops", "nasdaq drops", "turmoil", "bloodbath"]
NEWS_TOPICS["VOLRATIO"] = ["volatility", "volatile", "vix", "fear gauge", "swings"] + _SELLOFF
NEWS_TOPICS["VIX"] = NEWS_TOPICS["VIX"] + ["volatility", "volatile", "panic", "turmoil"] + _SELLOFF
NEWS_TOPICS["MOOD"] = NEWS_TOPICS["MOOD"] + ["panic", "risk-off", "investor sentiment", "market sentiment", "capitulation"]
_NEWS_RX.update({k: re.compile(r"(?<![a-z0-9])(?:" + "|".join(re.escape(p) for p in NEWS_TOPICS[k]) + r")(?![a-z0-9])", re.I) for k in ("VOLRATIO", "VIX", "MOOD")})
RELATIONS.extend([
    ("VIX", "VOLRATIO", "same", "This indicator is the VIX measured against its own 50-day average: when the VIX jumps, it jumps."),
    ("SPX", "VOLRATIO", "inverse", "Sharp falls in stocks make options traders nervous, so nervousness rises when the S&P 500 drops."),
    ("VOLRATIO", "VIX", "same", "Both measure how nervous options traders are; this one compares today's VIX with its own 50-day average, so it shows how unusual the fear is."),
    ("VOLRATIO", "SPX", "inverse", "Jumps in nervousness come with sharp falls in stocks."),
    ("VOLRATIO", "MOOD", "inverse", "Volatility is one of the four parts of our fear & greed score: more nervousness lowers it."),
    ("VOLRATIO", "HYSPREAD", "same", "When stock traders panic, lenders to risky companies usually demand more interest too."),
    ("VOLRATIO", "SECT_UP", "inverse", "In a real sell-off almost all sectors fall at once, so few sectors rise."),
    ("VOLRATIO", "VIXTS", "same", "Both rise in a stress phase: fear now exceeds fear later."),
    ("VOLRATIO", "BREADTH", "inverse", "Falling stocks hit the big, crowded names first; the equal-weight index holds up better or worse depending on how broad the fear is."),
])


# Finding a stock Yahoo does not know under the typed name: Yahoo's own search (names, ISINs), other exchanges, and Stooq
def fetch_stooq(sym, days=420):
    """Daily closes and volumes from Stooq (free, many European listings). Returns {"pts": [...], "vol": [...]} or None."""
    raw = http_get(f"https://stooq.com/q/d/l/?s={sym.lower()}&i=d", timeout=20, ua=_BROWSER).decode("utf-8", "replace")
    rows = list(csv.reader(io.StringIO(raw)))
    if not rows or rows[0][:1] != ["Date"]:
        return None
    pts, vol = [], []
    for r in rows[1:][-days:]:
        try:
            pts.append([r[0], float(r[4])])
            vol.append(float(r[5]) if len(r) > 5 and r[5] else 0.0)
        except (ValueError, IndexError):
            continue
    return {"pts": pts, "vol": vol} if len(pts) >= 20 else None


def yahoo_search(q):
    from urllib.parse import quote
    data = json.loads(http_get("https://query2.finance.yahoo.com/v1/finance/search?quotesCount=8&newsCount=0&q=" + quote(q), timeout=15, ua=_BROWSER))
    return [{"sym": x["symbol"], "name": x.get("longname") or x.get("shortname") or x["symbol"], "exch": x.get("exchDisp", "")}
            for x in data.get("quotes", []) if x.get("symbol") and x.get("quoteType") in ("EQUITY", "ETF", "INDEX", "MUTUALFUND", "FUTURE", "CURRENCY", "CRYPTOCURRENCY")]


EXCHANGE_SUFFIXES = [".DE", ".L", ".AS", ".PA", ".MI", ".SW", ".ST", ".MC", ".VI", ".F", ".CO", ".OL", ".HE", ".BR", ".LS"]
STOOQ_SUFFIXES = [".us", ".de", ".uk", ".pl", ".hu", ".jp", ""]


def resolve_symbol(q):
    """Where does this stock live? Returns {"sym", "src": "yahoo"|"stooq", "name"} or None. Tries, in order: Yahoo's search for the text,
    the same ticker on other European exchanges at Yahoo, then Stooq."""
    q = q.strip()
    cands, names = [], {}
    try:
        for x in yahoo_search(q):
            cands.append(x["sym"]); names[x["sym"]] = f"{x['name']} ({x['exch']})" if x["exch"] else x["name"]
    except Exception as e:
        log.info("Yahoo search %s: %s", q, e)
    if re.fullmatch(r"[A-Za-z0-9\-]{1,10}", q):
        cands += [q.upper() + s for s in EXCHANGE_SUFFIXES]
    cands = list(dict.fromkeys(cands))[:24]
    if cands:
        try:
            got = fetch_yahoo(cands, period="1mo")
            for c in cands:
                if c in got and len(got[c]) >= 3:
                    return {"sym": c, "src": "yahoo", "name": names.get(c, c)}
        except Exception as e:
            log.info("Yahoo variants %s: %s", q, e)
    if re.fullmatch(r"[A-Za-z0-9.\-]{1,14}", q):
        base = q.lower().split(".")[0] if "." in q and q.lower().rsplit(".", 1)[1] in ("us", "de", "uk", "pl", "hu", "jp") else q.lower()
        for s in ([""] if "." in q else STOOQ_SUFFIXES):
            try:
                if fetch_stooq(base + s if not "." in q else q):
                    return {"sym": (base + s if "." not in q else q).upper(), "src": "stooq", "name": q.upper() + " (Stooq)"}
            except Exception:
                continue
    return None
