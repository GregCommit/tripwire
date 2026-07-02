"""
Tripwire Rule Backtester
========================
Standalone, event-study backtest of Tripwire's 7 technical rules over ~5 years of
daily data. Answers: which rules predict short-term (1-5 trading-day) moves, at what
threshold levels, and how thresholds should differ per ticker.

Design notes (see the plan for full rationale):
  * Standalone by necessity — importing app.py boots Flask + monitor threads. The rule
    math here is re-implemented vectorized, and `parity` checks it against the REAL
    evaluate_rules() source extracted from app.py (without importing/booting it).
  * Event study, not a portfolio sim. Each rule trigger = an event; we measure forward
    signal-direction excess return vs SPY at 1/3/5 trading days.
  * Events are de-clustered to "episode starts" (condition false->true), mirroring the
    app's 24h alert cooldown + condition persistence.
  * Free data only: yfinance (period=max, auto-adjusted), disk-cached; Stooq fallback.

Usage:
  python backtest.py fetch            # download + cache prices (once)
  python backtest.py parity           # verify rule math matches app.py
  python backtest.py grid             # Phase B: per-rule param grids
  python backtest.py combo            # Phase C: rule combinations
  python backtest.py select           # write recommended_params.json
  python backtest.py report           # build report.html
  python backtest.py apply            # write recommendations into the app DB (with backup)
  python backtest.py undo             # restore latest params backup
  python backtest.py                  # fetch(cached) -> grid -> combo -> select -> report
Flags: --refresh (re-download), --years N (default 5), --min-n N (default 30)
"""

import sys, os, json, time, argparse, sqlite3, ast, random, textwrap, io, urllib.request
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# PATHS & CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

HERE        = Path(__file__).resolve().parent
APP_PY      = HERE / "app.py"
DATA_DIR    = HERE / "backtest_data"
RESULTS_DIR = HERE / "backtest_results"
DB_PATH     = Path.home() / ".tripwire_v3" / "state.db"
BENCHMARK   = "SPY"

FWD_HORIZONS = [1, 3, 5]          # trading days
HEADLINE_H   = 5                  # selection metric horizon
CONFIRM_H    = 3                  # sign must agree here
MIN_N        = 30                 # min episodes to adopt a per-ticker threshold
OOS_FRAC     = 0.30               # last 30% of timeline = out-of-sample
DEFAULT_YEARS = 5

# Category defaults mirror app.py DEFAULT_RULES (only the numeric params we vary).
CATEGORY_DEFAULTS = {
    "high_vol": {"volatility_multiplier":2.5,"volatility_lookback":20,"support_resist_pct":7.0,"support_resist_lookback":90,"consecutive_down_days":3,"use_consecutive":True,"volume_multiplier":3.0,"volume_lookback":20,"gap_pct":3.0,"rsi_period":14,"rsi_overbought":70,"rsi_oversold":30,"ma_short":20,"ma_long":50,"ma_cross_lookback":3},
    "mod_vol":  {"volatility_multiplier":1.5,"volatility_lookback":20,"support_resist_pct":5.0,"support_resist_lookback":30,"consecutive_down_days":3,"use_consecutive":False,"volume_multiplier":2.5,"volume_lookback":20,"gap_pct":2.0,"rsi_period":14,"rsi_overbought":70,"rsi_oversold":30,"ma_short":20,"ma_long":50,"ma_cross_lookback":3},
    "low_vol":  {"volatility_multiplier":1.2,"volatility_lookback":20,"support_resist_pct":4.0,"support_resist_lookback":60,"consecutive_down_days":3,"use_consecutive":False,"volume_multiplier":2.0,"volume_lookback":20,"gap_pct":1.5,"rsi_period":14,"rsi_overbought":70,"rsi_oversold":30,"ma_short":20,"ma_long":50,"ma_cross_lookback":3},
}

RULE_TYPES = ["volatility", "support_resistance", "consecutive_down", "volume", "gap", "rsi", "ma_cross"]

def log(*a):
    print(*a, flush=True)

# ─────────────────────────────────────────────────────────────────────────────
# WATCHLIST (read from the app DB; falls back to the known default set)
# ─────────────────────────────────────────────────────────────────────────────

def get_watchlist():
    try:
        c = sqlite3.connect(str(DB_PATH))
        rows = c.execute("SELECT symbol,category FROM stocks WHERE active=1 ORDER BY symbol").fetchall()
        c.close()
        if rows:
            return {r[0]: r[1] for r in rows}
    except Exception as e:
        log(f"[warn] could not read watchlist from DB ({e}); using defaults")
    return {"AAPL":"mod_vol","ABBV":"low_vol","AMAT":"high_vol","ARM":"high_vol","EVR":"high_vol",
            "JPM":"low_vol","LLY":"low_vol","MSFT":"mod_vol","MU":"high_vol","NVDA":"high_vol","SNDK":"high_vol"}

# ─────────────────────────────────────────────────────────────────────────────
# DATA LAYER
# ─────────────────────────────────────────────────────────────────────────────

def _cache_path(sym):
    return DATA_DIR / f"{sym}.csv"

def _fetch_yf(sym):
    import yfinance as yf
    df = yf.Ticker(sym).history(period="max", auto_adjust=True)
    if df is None or df.empty:
        raise ValueError("empty from yfinance")
    df = df.rename(columns=str.lower)[["open", "high", "low", "close", "volume"]].copy()
    df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
    df.index.name = "date"
    return df

def _fetch_stooq(sym):
    url = f"https://stooq.com/q/d/l/?s={sym.lower()}.us&i=d"
    raw = urllib.request.urlopen(url, timeout=20).read().decode("utf-8", "replace")
    df = pd.read_csv(io.StringIO(raw))
    if df.empty or "Close" not in df.columns:
        raise ValueError("empty/invalid from stooq")
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")[["open", "high", "low", "close", "volume"]]
    df.index = df.index.normalize()
    return df

def fetch_one(sym, refresh=False):
    cp = _cache_path(sym)
    if cp.exists() and not refresh:
        df = pd.read_csv(cp, parse_dates=["date"]).set_index("date")
        return df
    src = "yfinance"
    try:
        df = _fetch_yf(sym)
    except Exception as e:
        log(f"  [warn] yfinance failed for {sym} ({e}); trying Stooq")
        try:
            df = _fetch_stooq(sym); src = "stooq"
        except Exception as e2:
            log(f"  [ERROR] both sources failed for {sym}: yf={e} stooq={e2}")
            return None
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df = df[df["close"] > 0]
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(cp)
    log(f"  {sym}: {len(df)} rows [{df.index.min().date()} .. {df.index.max().date()}] via {src}")
    return df

def fetch_all(refresh=False):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    wl = get_watchlist()
    syms = list(wl.keys()) + [BENCHMARK]
    log(f"Fetching {len(syms)} symbols (refresh={refresh})...")
    data = {}
    net_calls = 0
    for sym in syms:
        cached = _cache_path(sym).exists()
        if not cached or refresh:
            net_calls += 1
            time.sleep(0.4)  # be gentle on the API
        df = fetch_one(sym, refresh=refresh)
        if df is not None:
            data[sym] = df
    log(f"Network fetches this run: {net_calls}")
    return data

def fetch_earnings_dates(sym):
    """Best-effort earnings dates (cached). Returns a set of date objects; empty on failure."""
    cp = DATA_DIR / f"{sym}_earnings.json"
    if cp.exists():
        try:
            return set(datetime.strptime(d, "%Y-%m-%d").date() for d in json.loads(cp.read_text()))
        except Exception:
            pass
    try:
        import yfinance as yf
        df = yf.Ticker(sym).get_earnings_dates(limit=40)
        dates = [d.date().isoformat() for d in df.index] if df is not None and not df.empty else []
        cp.write_text(json.dumps(dates))
        return set(datetime.strptime(d, "%Y-%m-%d").date() for d in dates)
    except Exception:
        return set()

# ─────────────────────────────────────────────────────────────────────────────
# VECTORIZED RULE ENGINE  (must match app.py evaluate_rules exactly)
# Each function returns (trigger: bool Series, signal: object Series of BUY/SELL/NEUTRAL)
# aligned to df.index. NaN/insufficient-history positions -> trigger False.
# ─────────────────────────────────────────────────────────────────────────────

def _r2(x):  # round-half like python's round (banker's) — matches app's round(...,2)
    return np.round(x, 2)

def rule_volatility(df, multiplier, lookback):
    c = df["close"].to_numpy(dtype=float)
    n = len(c)
    trig = np.zeros(n, dtype=bool); sig = np.array(["NEUTRAL"]*n, dtype=object)
    prev = np.empty(n); prev[:] = np.nan; prev[1:] = c[:-1]
    dp = np.abs((c - prev) / prev) * 100.0          # daily abs % move
    # avg_vol[t] = mean of dp over the (lookback-1) values ending at t  (window of `lookback` closes)
    win = max(lookback - 1, 1)
    dp_series = pd.Series(dp)
    avg = dp_series.rolling(win).mean().to_numpy()
    for t in range(1, n):
        if not np.isfinite(prev[t]) or prev[t] <= 0 or not np.isfinite(avg[t]):
            continue
        avg_v = round(float(avg[t]), 2)
        thr = round(avg_v * multiplier, 2)
        if dp[t] > thr:
            trig[t] = True
            sig[t] = "BUY" if c[t] > prev[t] else "SELL"
    return pd.Series(trig, index=df.index), pd.Series(sig, index=df.index)

def rule_support_resistance(df, pct, lookback, exclude_today=True):
    """Breakout beyond the PRIOR `lookback`-day high/low +/- buffer. `exclude_today`
    shifts the window off the current bar (the corrected/app behavior). Passing
    exclude_today=False reproduces a window that includes today (never fires)."""
    c = df["close"].to_numpy(dtype=float)
    n = len(c)
    trig = np.zeros(n, dtype=bool); sig = np.array(["NEUTRAL"]*n, dtype=object)
    p = pct / 100.0
    cs = pd.Series(c)
    lo = cs.rolling(lookback).min(); hi = cs.rolling(lookback).max()
    if exclude_today:
        lo = lo.shift(1); hi = hi.shift(1)
    lo = lo.to_numpy(); hi = hi.to_numpy()
    for t in range(n):
        if not np.isfinite(lo[t]):
            continue
        support = round(lo[t] * (1 - p), 2)
        resistance = round(hi[t] * (1 + p), 2)
        if c[t] < support:
            trig[t] = True; sig[t] = "SELL"
        elif c[t] > resistance:
            trig[t] = True; sig[t] = "BUY"
    return pd.Series(trig, index=df.index), pd.Series(sig, index=df.index)

def _consec_runs(c):
    """Return (cur_run, max_downs) arrays computed over the trailing 12-close window, per app."""
    n = len(c)
    cur = np.zeros(n, dtype=int); mx = np.zeros(n, dtype=int)
    for t in range(n):
        window = c[max(0, t-11):t+1]            # last 12 closes
        if len(window) < 4:
            cur[t] = -1; mx[t] = -1; continue    # insufficient
        run = m = 0
        for i in range(1, len(window)):
            if window[i] < window[i-1]:
                run += 1; m = max(m, run)
            else:
                run = 0
        cur[t] = run; mx[t] = m
    return cur, mx

def rule_consecutive(df, days, cur=None, mx=None):
    c = df["close"].to_numpy(dtype=float)
    if cur is None:
        cur, mx = _consec_runs(c)
    n = len(c)
    trig = np.zeros(n, dtype=bool); sig = np.array(["NEUTRAL"]*n, dtype=object)
    for t in range(n):
        if cur[t] < 0:
            continue
        active = cur[t] >= days
        resolved = (mx[t] >= days) and cur[t] == 0
        if active:
            trig[t] = True; sig[t] = "SELL"
        elif resolved:
            trig[t] = True; sig[t] = "BUY"
    return pd.Series(trig, index=df.index), pd.Series(sig, index=df.index)

def rule_volume(df, multiplier, lookback):
    c = df["close"].to_numpy(dtype=float)
    v = df["volume"].to_numpy(dtype=float)
    n = len(c)
    trig = np.zeros(n, dtype=bool); sig = np.array(["NEUTRAL"]*n, dtype=object)
    prev = np.empty(n); prev[:] = np.nan; prev[1:] = c[:-1]
    # app: vols = last `lookback` volumes (drop falsy); base excludes today if len>3; avg=mean(base)
    for t in range(n):
        lo = max(0, t - lookback + 1)
        vols = [x for x in v[lo:t+1] if x and np.isfinite(x) and x > 0]
        if not (v[t] and np.isfinite(v[t]) and len(vols) >= 3):
            continue
        base = vols[:-1] if len(vols) > 3 else vols
        avg = float(np.mean(base))
        if avg <= 0:
            continue
        ratio = round(v[t] / avg, 2)
        if ratio > multiplier:
            trig[t] = True
            sig[t] = "BUY" if (np.isfinite(prev[t]) and c[t] >= prev[t]) else ("SELL" if np.isfinite(prev[t]) else "NEUTRAL")
    return pd.Series(trig, index=df.index), pd.Series(sig, index=df.index)

def rule_gap(df, threshold):
    c = df["close"].to_numpy(dtype=float)
    o = df["open"].to_numpy(dtype=float)
    n = len(c)
    trig = np.zeros(n, dtype=bool); sig = np.array(["NEUTRAL"]*n, dtype=object)
    prev = np.empty(n); prev[:] = np.nan; prev[1:] = c[:-1]
    for t in range(1, n):
        if not (np.isfinite(prev[t]) and prev[t] > 0 and np.isfinite(o[t])):
            continue
        gap = round((o[t] - prev[t]) / prev[t] * 100.0, 2)
        if abs(gap) > threshold:
            trig[t] = True
            sig[t] = "BUY" if gap >= 0 else "SELL"
    return pd.Series(trig, index=df.index), pd.Series(sig, index=df.index)

def _rsi_window(closes, period):
    """Exact replica of app._rsi: Wilder RSI over the given close list; None if too short."""
    if len(closes) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i-1]
        if d >= 0: gains += d
        else:      losses -= d
    avg_gain = gains / period; avg_loss = losses / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i-1]
        gain = max(d, 0.0); loss = max(-d, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)

def rsi_series(df, period):
    """RSI[t] recomputed over trailing 3*period window (matches app's windowed recompute)."""
    c = df["close"].to_numpy(dtype=float)
    n = len(c); out = np.full(n, np.nan)
    w = period * 3
    for t in range(n):
        lo = max(0, t - w + 1)
        val = _rsi_window(list(c[lo:t+1]), period)
        if val is not None:
            out[t] = val
    return out

def rule_rsi(df, period, ob, os_, rsi_vals=None):
    if rsi_vals is None:
        rsi_vals = rsi_series(df, period)
    n = len(rsi_vals)
    trig = np.zeros(n, dtype=bool); sig = np.array(["NEUTRAL"]*n, dtype=object)
    for t in range(n):
        v = rsi_vals[t]
        if not np.isfinite(v):
            continue
        if v >= ob:
            trig[t] = True; sig[t] = "SELL"
        elif v <= os_:
            trig[t] = True; sig[t] = "BUY"
    return pd.Series(trig, index=df.index), pd.Series(sig, index=df.index)

def rule_ma_cross(df, short, long, look):
    c = pd.Series(df["close"].to_numpy(dtype=float))
    n = len(c)
    trig = np.zeros(n, dtype=bool); sig = np.array(["NEUTRAL"]*n, dtype=object)
    sma_s = c.rolling(short).mean().to_numpy()
    sma_l = c.rolling(long).mean().to_numpy()
    # cross at position u (between u-1 and u)
    for t in range(n):
        if t < long + look - 1:
            continue
        crossed = None
        for back in range(1, look + 1):     # most-recent first (matches app)
            u = t - back + 1
            if u < 1: break
            s0, l0, s1, l1 = sma_s[u-1], sma_l[u-1], sma_s[u], sma_l[u]
            if not all(np.isfinite(x) for x in (s0, l0, s1, l1)):
                continue
            if s0 <= l0 and s1 > l1:
                crossed = "golden"; break
            if s0 >= l0 and s1 < l1:
                crossed = "death"; break
        if crossed:
            trig[t] = True
            sig[t] = "BUY" if crossed == "golden" else "SELL"
    return pd.Series(trig, index=df.index), pd.Series(sig, index=df.index)

# ─────────────────────────────────────────────────────────────────────────────
# PARAMETER GRIDS (Phase B)
# ─────────────────────────────────────────────────────────────────────────────

def _frange(a, b, step):
    out = []; x = a
    while x <= b + 1e-9:
        out.append(round(x, 4)); x += step
    return out

GRIDS = {
    "volatility":        [{"volatility_multiplier": m, "volatility_lookback": lb}
                          for m in _frange(1.0, 4.0, 0.25) for lb in (10, 20, 30)],
    "support_resistance":[{"support_resist_pct": p, "support_resist_lookback": lb}
                          for p in (2,3,4,5,6,7,8,10) for lb in (30, 60, 90, 120)],
    "consecutive_down":  [{"consecutive_down_days": d} for d in (2,3,4,5)],
    "volume":            [{"volume_multiplier": m, "volume_lookback": lb}
                          for m in (1.5,2,2.5,3,4,5) for lb in (10, 20)],
    "gap":               [{"gap_pct": g} for g in (1, 1.5, 2, 3, 4, 5)],
    "rsi":               [{"rsi_period": p, "rsi_overbought": ob, "rsi_oversold": o}
                          for p in (7,14,21) for ob in (65,70,75,80) for o in (20,25,30,35)],
    "ma_cross":          [{"ma_short": s, "ma_long": l, "ma_cross_lookback": 3}
                          for (s, l) in ((10,30),(20,50),(50,100),(50,200))],
}

def run_rule(rule, df, params, cache=None):
    """Dispatch to the vectorized rule with `params`. `cache` memoizes expensive per-rule
    intermediates (consecutive runs, RSI series) keyed within a single ticker."""
    if rule == "volatility":
        return rule_volatility(df, params["volatility_multiplier"], int(params["volatility_lookback"]))
    if rule == "support_resistance":
        return rule_support_resistance(df, params["support_resist_pct"], int(params["support_resist_lookback"]))
    if rule == "consecutive_down":
        cur, mx = (cache or {}).get("consec", (None, None))
        return rule_consecutive(df, int(params["consecutive_down_days"]), cur, mx)
    if rule == "volume":
        return rule_volume(df, params["volume_multiplier"], int(params["volume_lookback"]))
    if rule == "gap":
        return rule_gap(df, params["gap_pct"])
    if rule == "rsi":
        key = f"rsi{int(params['rsi_period'])}"
        rv = (cache or {}).get(key)
        return rule_rsi(df, int(params["rsi_period"]), params["rsi_overbought"], params["rsi_oversold"], rv)
    if rule == "ma_cross":
        return rule_ma_cross(df, int(params["ma_short"]), int(params["ma_long"]), int(params["ma_cross_lookback"]))
    raise ValueError(rule)

# ─────────────────────────────────────────────────────────────────────────────
# EVENT STUDY
# ─────────────────────────────────────────────────────────────────────────────

def forward_matrix(close):
    """Return dict h -> np.array of k-day forward simple returns (close-to-close)."""
    c = close.to_numpy(dtype=float); n = len(c)
    out = {}
    for h in FWD_HORIZONS:
        f = np.full(n, np.nan)
        if n > h:
            f[:n-h] = c[h:] / c[:n-h] - 1.0
        out[h] = f
    return out

def build_forward(df, spy_close):
    """Attach forward returns and SPY-excess forward returns to a per-ticker frame."""
    fwd = forward_matrix(df["close"])
    spy = spy_close.reindex(df.index)
    spy_fwd = forward_matrix(spy)
    return fwd, spy_fwd

def episode_starts(trigger):
    t = trigger.to_numpy(dtype=bool)
    prev = np.empty(len(t), dtype=bool); prev[0] = False; prev[1:] = t[:-1]
    return t & (~prev)

def event_returns(df, trigger, signal, fwd, spy_fwd, eval_start_idx):
    """Return a DataFrame of episode-start events with signal-direction excess returns."""
    starts = episode_starts(trigger)
    idx = np.where(starts)[0]
    rows = []
    n = len(df)
    for t in idx:
        if t < eval_start_idx:
            continue
        s = signal.iloc[t]
        if s not in ("BUY", "SELL"):
            continue
        if not np.isfinite(fwd[HEADLINE_H][t]):   # need full 5-day forward window
            continue
        dir_mult = 1.0 if s == "BUY" else -1.0
        rec = {"date": df.index[t], "pos": t, "signal": s}
        for h in FWD_HORIZONS:
            raw = fwd[h][t]; braw = spy_fwd[h][t]
            rec[f"ret{h}"] = raw
            rec[f"exc{h}"] = dir_mult * (raw - (braw if np.isfinite(braw) else 0.0))
            rec[f"dirret{h}"] = dir_mult * raw
        # Max adverse excursion over 1..5d (worst signal-direction cumulative return)
        c = df["close"].to_numpy(dtype=float)
        path = [dir_mult * (c[t+k]/c[t]-1.0) for k in range(1, HEADLINE_H+1) if t+k < n]
        rec["mae"] = min(path) if path else np.nan
        rows.append(rec)
    return pd.DataFrame(rows)

def summarize(events, label_fields):
    """Aggregate an events DataFrame into one metrics row."""
    if events is None or events.empty:
        row = dict(label_fields); row.update({"n": 0, "n_buy": 0, "n_sell": 0})
        for h in FWD_HORIZONS:
            row[f"mean_exc{h}"] = np.nan; row[f"med_exc{h}"] = np.nan
        row["hit5"] = np.nan; row["t5"] = np.nan; row["mae"] = np.nan
        return row
    row = dict(label_fields)
    row["n"] = len(events)
    row["n_buy"] = int((events["signal"] == "BUY").sum())
    row["n_sell"] = int((events["signal"] == "SELL").sum())
    for h in FWD_HORIZONS:
        row[f"mean_exc{h}"] = float(events[f"exc{h}"].mean())
        row[f"med_exc{h}"] = float(events[f"exc{h}"].median())
    e5 = events[f"exc{HEADLINE_H}"].dropna()
    row["hit5"] = float((e5 > 0).mean()) if len(e5) else np.nan
    if len(e5) > 1 and e5.std(ddof=1) > 0:
        row["t5"] = float(e5.mean() / (e5.std(ddof=1) / np.sqrt(len(e5))))
    else:
        row["t5"] = np.nan
    row["mae"] = float(events["mae"].mean())
    return row

# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATION: load frames, precompute caches
# ─────────────────────────────────────────────────────────────────────────────

def load_frames(years):
    data = {}
    for cp in sorted(DATA_DIR.glob("*.csv")):
        sym = cp.stem
        if sym.endswith("_earnings"):
            continue
        df = pd.read_csv(cp, parse_dates=["date"]).set_index("date").sort_index()
        data[sym] = df
    if BENCHMARK not in data:
        raise SystemExit(f"Missing {BENCHMARK} data — run `python backtest.py fetch` first.")
    return data

def trim_history(df, years):
    """Keep only what the rules need: the eval window plus ~300-row warmup (covers MA200).
    Rules read only a tail, so this is result-identical and far faster than full history."""
    keep = int(years * 252) + 300
    return df.iloc[-keep:].copy() if len(df) > keep else df

def eval_start_index(df, years):
    cutoff = pd.Timestamp(datetime.now().date()) - pd.Timedelta(days=int(years*365.25))
    pos = int(np.searchsorted(df.index.values, np.datetime64(cutoff)))
    return min(pos, len(df)-1)

def ticker_cache(df):
    cur, mx = _consec_runs(df["close"].to_numpy(dtype=float))
    cache = {"consec": (cur, mx)}
    for p in (7, 14, 21):
        cache[f"rsi{p}"] = rsi_series(df, p)
    return cache

# ─────────────────────────────────────────────────────────────────────────────
# PHASE B: GRID
# ─────────────────────────────────────────────────────────────────────────────

def params_label(rule, params):
    keymap = {
        "volatility": lambda p: f"mult={p['volatility_multiplier']},lb={int(p['volatility_lookback'])}",
        "support_resistance": lambda p: f"pct={p['support_resist_pct']},lb={int(p['support_resist_lookback'])}",
        "consecutive_down": lambda p: f"days={int(p['consecutive_down_days'])}",
        "volume": lambda p: f"mult={p['volume_multiplier']},lb={int(p['volume_lookback'])}",
        "gap": lambda p: f"pct={p['gap_pct']}",
        "rsi": lambda p: f"per={int(p['rsi_period'])},ob={p['rsi_overbought']},os={p['rsi_oversold']}",
        "ma_cross": lambda p: f"{int(p['ma_short'])}/{int(p['ma_long'])}",
    }
    return keymap[rule](params)

def run_grid(years):
    data = load_frames(years)
    wl = get_watchlist()
    rows = []
    all_events = []
    for sym, cat in wl.items():
        if sym not in data:
            log(f"  [skip] {sym}: no data"); continue
        df = trim_history(data[sym], years)
        cache = ticker_cache(df)
        esi = eval_start_index(df, years)
        fwd, spy_fwd = build_forward(df, data[BENCHMARK]["close"])
        n_eval = len(df) - esi
        short_hist = df.index[esi] > (pd.Timestamp(datetime.now().date()) - pd.Timedelta(days=int(years*365.25)) + pd.Timedelta(days=30))
        log(f"  {sym} [{cat}] eval days={n_eval} start={df.index[esi].date()}"
            + ("  *SHORT HISTORY*" if short_hist else ""))
        for rule in RULE_TYPES:
            for params in GRIDS[rule]:
                trig, sig = run_rule(rule, df, params, cache)
                ev = event_returns(df, trig, sig, fwd, spy_fwd, esi)
                # split walk-forward: first 70% / last 30% by timeline position
                split_pos = esi + int((len(df) - esi) * (1 - OOS_FRAC))
                is_ev = ev[ev["pos"] < split_pos] if not ev.empty else ev
                oos_ev = ev[ev["pos"] >= split_pos] if not ev.empty else ev
                base = summarize(ev, {"symbol": sym, "category": cat, "rule": rule,
                                      "params": params_label(rule, params),
                                      "params_json": json.dumps(params)})
                base["is_mean_exc5"] = float(is_ev[f"exc{HEADLINE_H}"].mean()) if len(is_ev) else np.nan
                base["is_n"] = len(is_ev)
                base["oos_mean_exc5"] = float(oos_ev[f"exc{HEADLINE_H}"].mean()) if len(oos_ev) else np.nan
                base["oos_n"] = len(oos_ev)
                base["short_history"] = bool(short_hist)
                rows.append(base)
                if not ev.empty:
                    ev2 = ev.copy()
                    ev2["symbol"] = sym; ev2["rule"] = rule; ev2["params"] = params_label(rule, params)
                    all_events.append(ev2)
    grid = pd.DataFrame(rows)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    grid.to_csv(RESULTS_DIR / "grid_summary.csv", index=False)
    if all_events:
        pd.concat(all_events, ignore_index=True).to_csv(RESULTS_DIR / "events.csv", index=False)
    log(f"Grid: {len(grid)} rows -> {RESULTS_DIR/'grid_summary.csv'}")
    return grid

# ─────────────────────────────────────────────────────────────────────────────
# POOLED VIEWS + SELECTION POLICY
# ─────────────────────────────────────────────────────────────────────────────

def pooled_by_category(grid):
    """Pool events across tickers in a category for each (rule, params)."""
    ev_path = RESULTS_DIR / "events.csv"
    if not ev_path.exists():
        return pd.DataFrame()
    events = pd.read_csv(ev_path, parse_dates=["date"])
    wl = get_watchlist()
    events["category"] = events["symbol"].map(wl)
    rows = []
    for (cat, rule, params), g in events.groupby(["category", "rule", "params"]):
        row = summarize(g, {"category": cat, "rule": rule, "params": params})
        rows.append(row)
    return pd.DataFrame(rows)

def pick_best(subdf):
    """Best grid row by headline metric with confirmation + plateau preference.
    Returns (row, reason) or (None, reason)."""
    cand = subdf[(subdf["n"] >= MIN_N)].copy()
    if cand.empty:
        return None, f"no params reached N>={MIN_N}"
    # require headline positive and 3d agrees in sign
    cand = cand[(cand["mean_exc5"] > 0) & (cand["mean_exc3"] > 0)]
    if cand.empty:
        return None, "no params with positive & sign-consistent edge"
    cand = cand.sort_values("mean_exc5", ascending=False)
    best = cand.iloc[0]
    return best, "ok"

def select(years):
    grid = pd.read_csv(RESULTS_DIR / "grid_summary.csv")
    pooled = pooled_by_category(grid)
    pooled.to_csv(RESULTS_DIR / "pooled_summary.csv", index=False)
    wl = get_watchlist()
    recommendations = {}
    decisions = []   # human-readable audit rows
    for sym, cat in wl.items():
        rec = {}
        for rule in RULE_TYPES:
            sub = grid[(grid["symbol"] == sym) & (grid["rule"] == rule)]
            best, reason = pick_best(sub)
            adopt_level = None; chosen = None; note = reason
            if best is not None:
                oos = best.get("oos_mean_exc5", np.nan)
                if best["n"] >= MIN_N and pd.notna(oos) and oos > 0:
                    adopt_level = "ticker"; chosen = json.loads(best["params_json"])
                    note = f"ticker: exc5={best['mean_exc5']*100:.2f}% n={int(best['n'])} oos={oos*100:.2f}%"
            if chosen is None:
                # fall back to category-pooled best
                psub = pooled[(pooled["category"] == cat) & (pooled["rule"] == rule)] if not pooled.empty else pd.DataFrame()
                pbest, preason = pick_best(psub) if not psub.empty else (None, "no pooled events")
                if pbest is not None:
                    # map pooled params label back to a params dict via the grid
                    match = grid[(grid["rule"] == rule) & (grid["params"] == pbest["params"])]
                    if not match.empty:
                        adopt_level = "category"; chosen = json.loads(match.iloc[0]["params_json"])
                        note = f"category fallback: exc5={pbest['mean_exc5']*100:.2f}% n={int(pbest['n'])} ({reason})"
            if chosen is None:
                adopt_level = "disable"
                note = f"disable: {reason}"
                rec.update(_disable_flag(rule))
            else:
                rec.update(chosen)
                rec.update(_enable_flag(rule))
            decisions.append({"symbol": sym, "category": cat, "rule": rule,
                              "decision": adopt_level, "note": note})
        recommendations[sym] = rec
    (RESULTS_DIR / "recommended_params.json").write_text(json.dumps(recommendations, indent=2))
    pd.DataFrame(decisions).to_csv(RESULTS_DIR / "decisions.csv", index=False)
    log(f"Selection -> {RESULTS_DIR/'recommended_params.json'} ({len(recommendations)} tickers)")
    log(f"Decisions -> {RESULTS_DIR/'decisions.csv'}")
    return recommendations, decisions

def _enable_flag(rule):
    return {"volatility": {"enable_volatility": True}, "support_resistance": {"enable_support_resistance": True},
            "consecutive_down": {"use_consecutive": True}, "volume": {"enable_volume": True},
            "gap": {"enable_gap": True}, "rsi": {"enable_rsi": True}, "ma_cross": {"enable_ma": True}}[rule]

def _disable_flag(rule):
    return {"volatility": {"enable_volatility": False}, "support_resistance": {"enable_support_resistance": False},
            "consecutive_down": {"use_consecutive": False}, "volume": {"enable_volume": False},
            "gap": {"enable_gap": False}, "rsi": {"enable_rsi": False}, "ma_cross": {"enable_ma": False}}[rule]

# ─────────────────────────────────────────────────────────────────────────────
# PHASE C: COMBINATIONS (agreement + ablation) over selected per-ticker rules
# ─────────────────────────────────────────────────────────────────────────────

def run_combo(years):
    data = load_frames(years)
    wl = get_watchlist()
    grid = pd.read_csv(RESULTS_DIR / "grid_summary.csv")
    rows = []
    for sym, cat in wl.items():
        if sym not in data:
            continue
        df = trim_history(data[sym], years); cache = ticker_cache(df)
        esi = eval_start_index(df, years)
        fwd, spy_fwd = build_forward(df, data[BENCHMARK]["close"])
        # winners = rules with a positive-edge best param for this ticker
        winners = {}
        for rule in RULE_TYPES:
            sub = grid[(grid["symbol"] == sym) & (grid["rule"] == rule)]
            best, _ = pick_best(sub)
            if best is not None:
                winners[rule] = json.loads(best["params_json"])
        if len(winners) < 2:
            rows.append({"symbol": sym, "mode": "ensemble", "n": 0, "note": f"only {len(winners)} winning rule(s)"})
            continue
        # per-rule daily signal, persisted for 3 trading days ("active window")
        sig_active = {}
        for rule, p in winners.items():
            trig, sig = run_rule(rule, df, p, cache)
            act = pd.Series(["NEUTRAL"]*len(df), index=df.index, dtype=object)
            s = sig.to_numpy(); starts = episode_starts(trig)
            for t in np.where(starts)[0]:
                if s[t] in ("BUY", "SELL"):
                    for k in range(CONFIRM_H):
                        if t+k < len(df):
                            act.iloc[t+k] = s[t]
            sig_active[rule] = act
        def ensemble_events(active_map):
            recs = []
            close = df["close"].to_numpy(dtype=float); n = len(df)
            prev_label = None
            for t in range(esi, n):
                buys = sum(1 for r in active_map if active_map[r].iloc[t] == "BUY")
                sells = sum(1 for r in active_map if active_map[r].iloc[t] == "SELL")
                tot = buys + sells
                label = None
                if tot >= 2 and buys/tot >= 2/3: label = "STRONG_BUY"
                elif tot >= 2 and sells/tot >= 2/3: label = "STRONG_SELL"
                elif buys > sells and tot >= 1: label = "TREND_BUY"
                elif sells > buys and tot >= 1: label = "TREND_SELL"
                if label and label != prev_label and np.isfinite(fwd[HEADLINE_H][t]):
                    d = 1.0 if "BUY" in label else -1.0
                    rec = {"date": df.index[t], "pos": t, "signal": "BUY" if "BUY" in label else "SELL", "label": label}
                    for h in FWD_HORIZONS:
                        braw = spy_fwd[h][t]
                        rec[f"exc{h}"] = d*(fwd[h][t]-(braw if np.isfinite(braw) else 0))
                    path = [d*(close[t+k]/close[t]-1) for k in range(1, HEADLINE_H+1) if t+k < n]
                    rec["mae"] = min(path) if path else np.nan
                    recs.append(rec)
                prev_label = label
            return pd.DataFrame(recs)
        full = ensemble_events(sig_active)
        base = summarize(full, {"symbol": sym, "mode": "ensemble", "rules": "+".join(winners)})
        base["note"] = f"{len(winners)} rules"
        rows.append(base)
        # ablation: drop each rule, measure delta in mean_exc5
        for rule in winners:
            reduced = {r: sig_active[r] for r in sig_active if r != rule}
            if len(reduced) >= 2:
                ab = ensemble_events(reduced)
                abrow = summarize(ab, {"symbol": sym, "mode": f"ablate:-{rule}", "rules": "+".join(reduced)})
                abrow["note"] = f"drop {rule}; full_exc5={base['mean_exc5']}"
                rows.append(abrow)
    combo = pd.DataFrame(rows)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    combo.to_csv(RESULTS_DIR / "combo_summary.csv", index=False)
    log(f"Combo: {len(combo)} rows -> {RESULTS_DIR/'combo_summary.csv'}")
    return combo

# ─────────────────────────────────────────────────────────────────────────────
# PARITY HARNESS  (extract app.py's evaluate_rules WITHOUT importing/booting it)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_app_rules():
    src = APP_PY.read_text(encoding="utf-8")
    tree = ast.parse(src)
    wanted = {"_rsi", "evaluate_rules"}
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    if len(funcs) != 2:
        raise SystemExit("could not extract _rsi/evaluate_rules from app.py")
    mod = ast.Module(body=funcs, type_ignores=[])
    ns = {"statistics": __import__("statistics")}
    exec(compile(mod, "<app_rules>", "exec"), ns)
    return ns["evaluate_rules"]

def _default_params():
    p = dict(CATEGORY_DEFAULTS["high_vol"])
    p.update({"enable_volatility": True, "enable_support_resistance": True, "enable_volume": True,
              "enable_gap": True, "enable_rsi": True, "enable_ma": True})
    return p

def run_parity(years, n_samples=25):
    app_eval = _extract_app_rules()
    data = load_frames(years)
    wl = get_watchlist()
    syms = [s for s in wl if s in data]
    rng = random.Random(42)
    mismatches = 0; checks = 0
    rule_params_variants = {
        "volatility": {"volatility_multiplier": 2.0, "volatility_lookback": 20},
        "support_resistance": {"support_resist_pct": 5, "support_resist_lookback": 60},
        "consecutive_down": {"consecutive_down_days": 3},
        "volume": {"volume_multiplier": 2.5, "volume_lookback": 20},
        "gap": {"gap_pct": 2},
        "rsi": {"rsi_period": 14, "rsi_overbought": 70, "rsi_oversold": 30},
        "ma_cross": {"ma_short": 20, "ma_long": 50, "ma_cross_lookback": 3},
    }
    for rule, rp in rule_params_variants.items():
        params = _default_params(); params.update(rp)
        cache = None
        # All rules read only a bounded tail (max ~203 for MA); a 260-row window ending
        # at t yields identical results to full history and keeps each check fast.
        W = 260
        for _ in range(n_samples):
            sym = rng.choice(syms)
            df = data[sym]
            if len(df) < W + 5:
                continue
            t = rng.randint(W, len(df)-1)
            sl = df.iloc[t-W+1:t+1].reset_index(drop=True)   # 260-row tail; "today" = last row
            vtrig, vsig = run_rule(rule, sl, params, None)
            v_tr = bool(vtrig.iloc[-1]); v_sg = vsig.iloc[-1]
            # build app inputs from the same tail
            hist = [{"close": float(sl["close"].iloc[i]), "open": float(sl["open"].iloc[i]),
                     "high": float(sl["high"].iloc[i]), "low": float(sl["low"].iloc[i]),
                     "volume": float(sl["volume"].iloc[i])} for i in range(len(sl))]
            last = len(sl) - 1
            quote = {"close": float(sl["close"].iloc[last]), "prev_close": float(sl["close"].iloc[last-1]),
                     "open": float(sl["open"].iloc[last]), "high": float(sl["high"].iloc[last]),
                     "low": float(sl["low"].iloc[last]), "volume": float(sl["volume"].iloc[last])}
            res = app_eval(sym, quote, hist, params)
            appr = next(r for r in res if r["rule_type"] == rule)
            a_tr = bool(appr.get("triggered")); a_sg = appr.get("signal", "NEUTRAL")
            if a_sg is None: a_sg = "NEUTRAL"
            checks += 1
            if a_tr != v_tr or (a_tr and a_sg != v_sg):
                mismatches += 1
                log(f"  MISMATCH {rule} {sym}@{df.index[t].date()}: app(trig={a_tr},sig={a_sg}) vs vec(trig={v_tr},sig={v_sg})")
    log(f"\nParity: {checks-mismatches}/{checks} matched ({mismatches} mismatches)")
    return mismatches == 0

# ─────────────────────────────────────────────────────────────────────────────
# APPLY / UNDO  (direct sqlite3 to the app DB; never imports app.py)
# ─────────────────────────────────────────────────────────────────────────────

def apply_recs():
    recp = RESULTS_DIR / "recommended_params.json"
    if not recp.exists():
        raise SystemExit("run `select` first")
    recs = json.loads(recp.read_text())
    if not DB_PATH.exists():
        raise SystemExit(f"app DB not found at {DB_PATH}")
    c = sqlite3.connect(str(DB_PATH))
    # backup current rule_params
    cur = {r[0]: r[1] for r in c.execute("SELECT symbol,params FROM rule_params").fetchall()}
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    (RESULTS_DIR / f"params_backup_{ts}.json").write_text(json.dumps(cur, indent=2))
    # merge recommendations over each ticker's current effective params
    wl = get_watchlist()
    applied = 0
    for sym, rec in recs.items():
        existing = json.loads(cur[sym]) if sym in cur else dict(CATEGORY_DEFAULTS.get(wl.get(sym, "high_vol")))
        existing.update(rec)
        c.execute("INSERT OR REPLACE INTO rule_params (symbol,params) VALUES (?,?)", (sym, json.dumps(existing)))
        applied += 1
    c.commit(); c.close()
    log(f"Applied recommendations to {applied} tickers. Backup: params_backup_{ts}.json")

def undo_apply():
    backups = sorted(RESULTS_DIR.glob("params_backup_*.json"))
    if not backups:
        raise SystemExit("no backup found")
    latest = backups[-1]
    recs = json.loads(latest.read_text())
    c = sqlite3.connect(str(DB_PATH))
    c.execute("DELETE FROM rule_params")
    for sym, params in recs.items():
        c.execute("INSERT OR REPLACE INTO rule_params (symbol,params) VALUES (?,?)", (sym, params))
    c.commit(); c.close()
    log(f"Restored rule_params from {latest.name} ({len(recs)} tickers)")

# ─────────────────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────────────────

def _svg_sensitivity(sub, xlabel):
    """Tiny inline SVG: mean 5d excess (%) across a rule's grid points (sorted)."""
    if sub.empty:
        return "<div style='color:#6B7280'>no data</div>"
    s = sub.sort_values("mean_exc5")
    vals = (s["mean_exc5"]*100).to_numpy()
    labels = s["params"].tolist()
    W, H, pad = 560, 150, 26
    n = len(vals)
    if n == 0: return ""
    vmax = max(abs(np.nanmax(vals)), abs(np.nanmin(vals)), 0.5)
    bw = (W-2*pad)/n
    zero_y = H/2
    bars = []
    for i, v in enumerate(vals):
        if not np.isfinite(v): continue
        x = pad + i*bw
        h = (abs(v)/vmax)*(H/2-pad)
        y = zero_y - h if v >= 0 else zero_y
        color = "#10B981" if v >= 0 else "#EF4444"
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(1,bw-2):.1f}" height="{h:.1f}" fill="{color}"><title>{labels[i]}: {v:.2f}%</title></rect>')
    axis = f'<line x1="{pad}" y1="{zero_y}" x2="{W-pad}" y2="{zero_y}" stroke="#374151" stroke-width="1"/>'
    return f'<svg viewBox="0 0 {W} {H}" style="width:100%;height:{H}px">{axis}{"".join(bars)}<text x="{pad}" y="{H-4}" fill="#6B7280" font-size="10">{xlabel} (each bar = a threshold; green=positive 5d edge)</text></svg>'

def build_report(years):
    grid = pd.read_csv(RESULTS_DIR / "grid_summary.csv")
    pooled = pd.read_csv(RESULTS_DIR / "pooled_summary.csv") if (RESULTS_DIR/"pooled_summary.csv").exists() else pd.DataFrame()
    decisions = pd.read_csv(RESULTS_DIR / "decisions.csv") if (RESULTS_DIR/"decisions.csv").exists() else pd.DataFrame()
    combo = pd.read_csv(RESULTS_DIR / "combo_summary.csv") if (RESULTS_DIR/"combo_summary.csv").exists() else pd.DataFrame()
    recs = json.loads((RESULTS_DIR/"recommended_params.json").read_text()) if (RESULTS_DIR/"recommended_params.json").exists() else {}
    wl = get_watchlist()

    def fmt_pct(x):
        return f"{x*100:+.2f}%" if pd.notna(x) else "—"

    css = """<style>
    body{background:#0A0C12;color:#E4E0D8;font-family:-apple-system,Segoe UI,sans-serif;margin:0;padding:24px;line-height:1.5}
    h1{color:#F59E0B;font-size:22px} h2{color:#F59E0B;font-size:16px;margin-top:32px;border-bottom:1px solid #1E2235;padding-bottom:6px}
    h3{font-size:14px;margin-top:20px}
    .card{background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:16px 18px;margin-bottom:14px}
    table{border-collapse:collapse;width:100%;font-size:12px;margin-top:8px}
    th,td{text-align:right;padding:5px 9px;border-bottom:1px solid #1E2235} th{color:#9CA3AF;font-weight:600}
    td:first-child,th:first-child{text-align:left}
    .pos{color:#10B981} .neg{color:#EF4444} .mut{color:#6B7280}
    .pill{display:inline-block;border-radius:6px;padding:2px 8px;font-size:11px;font-weight:700}
    .adopt-ticker{background:#10B98122;color:#10B981} .adopt-category{background:#3B82F622;color:#60A5FA} .adopt-disable{background:#6B728022;color:#9CA3AF}
    .note{color:#6B7280;font-size:12px}
    </style>"""
    H = ['<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Tripwire Backtest</title>',
         css, f"<h1>⚡ Tripwire Rule Backtest</h1>",
         f"<div class='note'>Generated {datetime.now():%Y-%m-%d %H:%M} · {years}-year window · headline metric = mean {HEADLINE_H}-day signal-direction excess return vs {BENCHMARK} · events de-clustered to episode starts · N≥{MIN_N} + positive OOS required to adopt a per-ticker threshold.</div>"]

    # Per-rule effectiveness (pooled overall)
    H.append("<h2>Rule effectiveness — best threshold per rule (pooled across all tickers)</h2>")
    H.append("<div class='card'><table><tr><th>Rule</th><th>Best params</th><th>N</th><th>Buy/Sell</th><th>Mean exc 1d</th><th>3d</th><th>5d</th><th>Hit% 5d</th><th>t(5d)</th><th>MAE</th></tr>")
    ev_path = RESULTS_DIR / "events.csv"
    overall = pd.DataFrame()
    if ev_path.exists():
        events = pd.read_csv(ev_path, parse_dates=["date"])
        rows = []
        for (rule, params), g in events.groupby(["rule", "params"]):
            rows.append(summarize(g, {"rule": rule, "params": params}))
        overall = pd.DataFrame(rows)
    for rule in RULE_TYPES:
        sub = overall[overall["rule"] == rule] if not overall.empty else pd.DataFrame()
        best, _ = pick_best(sub) if not sub.empty else (None, "")
        if best is None:
            H.append(f"<tr><td>{rule}</td><td class='mut'>no positive edge</td><td colspan='8' class='mut'>—</td></tr>")
            continue
        cls5 = 'pos' if best['mean_exc5']>0 else 'neg'
        H.append(f"<tr><td>{rule}</td><td>{best['params']}</td><td>{int(best['n'])}</td><td>{int(best['n_buy'])}/{int(best['n_sell'])}</td>"
                 f"<td>{fmt_pct(best['mean_exc1'])}</td><td>{fmt_pct(best['mean_exc3'])}</td><td class='{cls5}'>{fmt_pct(best['mean_exc5'])}</td>"
                 f"<td>{best['hit5']*100:.0f}%</td><td>{best['t5']:.2f}</td><td class='neg'>{fmt_pct(best['mae'])}</td></tr>")
    H.append("</table></div>")

    # Threshold sensitivity charts
    H.append("<h2>Threshold sensitivity (pooled)</h2>")
    for rule in RULE_TYPES:
        sub = overall[overall["rule"] == rule] if not overall.empty else pd.DataFrame()
        H.append(f"<div class='card'><h3>{rule}</h3>{_svg_sensitivity(sub, rule)}</div>")

    # Per-ticker recommendation cards
    H.append("<h2>Per-ticker recommendations</h2>")
    for sym, cat in wl.items():
        H.append(f"<div class='card'><h3>{sym} <span class='note'>[{cat}]</span></h3>")
        dsub = decisions[decisions["symbol"] == sym] if not decisions.empty else pd.DataFrame()
        H.append("<table><tr><th>Rule</th><th>Decision</th><th>Evidence</th></tr>")
        for rule in RULE_TYPES:
            d = dsub[dsub["rule"] == rule]
            if d.empty:
                H.append(f"<tr><td>{rule}</td><td class='mut'>—</td><td class='mut'>—</td></tr>"); continue
            dec = d.iloc[0]["decision"]; note = d.iloc[0]["note"]
            pill = {"ticker":"adopt-ticker","category":"adopt-category","disable":"adopt-disable"}.get(dec, "adopt-disable")
            H.append(f"<tr><td>{rule}</td><td><span class='pill {pill}'>{dec}</span></td><td class='note' style='text-align:left'>{note}</td></tr>")
        H.append("</table></div>")

    # Combinations
    if not combo.empty:
        H.append("<h2>Rule combinations (ensemble & ablation)</h2><div class='card'><table><tr><th>Symbol</th><th>Mode</th><th>N</th><th>Mean exc 5d</th><th>Hit% 5d</th><th>Note</th></tr>")
        for _, r in combo.iterrows():
            if r.get("n", 0) == 0:
                H.append(f"<tr><td>{r['symbol']}</td><td>{r['mode']}</td><td>0</td><td class='mut'>—</td><td class='mut'>—</td><td class='note' style='text-align:left'>{r.get('note','')}</td></tr>"); continue
            cls = 'pos' if r['mean_exc5']>0 else 'neg'
            H.append(f"<tr><td>{r['symbol']}</td><td>{r['mode']}</td><td>{int(r['n'])}</td><td class='{cls}'>{fmt_pct(r['mean_exc5'])}</td><td>{r['hit5']*100:.0f}%</td><td class='note' style='text-align:left'>{r.get('note','')}</td></tr>")
        H.append("</table></div>")

    H.append("<div class='note' style='margin-top:30px'>⚠ Multiple thresholds were tested per rule; positive results can arise by chance. Adoption requires N≥%d, sign-consistent 1-5d edge, and positive out-of-sample performance, with a preference for pooled priors. Treat single-ticker results with small N as suggestive, not conclusive. Past performance does not guarantee future results.</div>" % MIN_N)
    (RESULTS_DIR / "report.html").write_text("\n".join(H), encoding="utf-8")
    log(f"Report -> {RESULTS_DIR/'report.html'}")

# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    global MIN_N
    ap = argparse.ArgumentParser(description="Tripwire rule backtester")
    ap.add_argument("cmd", nargs="?", default="all",
                    choices=["all","fetch","parity","grid","combo","select","report","apply","undo"])
    ap.add_argument("--refresh", action="store_true", help="re-download price data")
    ap.add_argument("--years", type=int, default=DEFAULT_YEARS)
    ap.add_argument("--min-n", type=int, default=MIN_N)
    args = ap.parse_args()
    MIN_N = args.min_n

    if args.cmd in ("all", "fetch"):
        fetch_all(refresh=args.refresh)
        if args.cmd == "fetch":
            return
    if args.cmd == "parity":
        ok = run_parity(args.years); sys.exit(0 if ok else 1)
    if args.cmd in ("all", "grid"):
        run_grid(args.years)
        if args.cmd == "grid": return
    if args.cmd in ("all", "combo"):
        run_combo(args.years)
        if args.cmd == "combo": return
    if args.cmd in ("all", "select"):
        select(args.years)
        if args.cmd == "select": return
    if args.cmd in ("all", "report"):
        build_report(args.years)
        if args.cmd == "report": return
    if args.cmd == "apply":
        apply_recs(); return
    if args.cmd == "undo":
        undo_apply(); return

if __name__ == "__main__":
    main()
