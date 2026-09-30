"""Tripwire learning layer (phase 2): learn thresholds for the owner's watchlist from many stocks.

The watchlist stays the owner's choice; the reference universe below is used only as training
data and never produces alerts.

    python learn.py fetch          # download ~300 reference stocks + sector ETFs + VIX (15y)
    python learn.py stats          # per stock/rule/threshold/year event totals -> stats.csv.gz
    python learn.py peers          # data-driven peer group for each watchlist stock
    python learn.py walkforward    # old vs new threshold selection on unseen years
    python learn.py all            # everything above in order

Method: each watchlist stock gets a peer group (the reference stocks that moved most like it
over the last two years, with similar volatility). Rule thresholds are scored on the pooled
events of the stock plus its peers, and the stock's own history pulls its estimate away from
the pool only as far as its own sample size justifies (empirical-Bayes shrinkage).
"""
import json, sys, time, os, argparse
from datetime import datetime
from multiprocessing import Pool
import numpy as np
import pandas as pd

import backtest as bt
import portfolio_sim

UNIVERSE_DIR = bt.DATA_DIR / "universe"
LEARN_DIR = bt.RESULTS_DIR / "learn"
YEARS = 15
CORE_RULES = ["volatility", "support_resistance", "volume", "rsi"]   # the four rules that vote
PEER_K = 30            # peers per watchlist stock
PEER_LOOKBACK = 504    # trading days of returns used to pick peers (~2 years)
SHRINK_K = 60          # pseudo-events: how much the peer pool counts vs the stock's own history
POOL_MIN_N = 200       # pooled events a threshold needs before it can be adopted
POOL_MIN_T = 2.0       # ...and a pooled t-stat of at least this
# Target "w": a signal moves money out of the owner's other stocks, so it only pays if the stock
# beats the equal-weight average of the watchlist over 5 days by more than the round-trip cost
# (0.1% on each of the four trades a slice needs).
ROUND_TRIP_COST = 0.004
TEST_YEARS = (2021, 2022, 2023, 2024, 2025)

UNIVERSE = """
AAPL MSFT NVDA AVGO ORCL CRM ADBE AMD INTC CSCO QCOM TXN IBM NOW INTU AMAT MU LRCX KLAC ADI MRVL SNPS
CDNS PANW FTNT ANET MCHP NXPI ON MPWR TER SWKS HPQ HPE DELL WDC STX NTAP ADSK ACN IT CTSH EPAM AKAM
FFIV KEYS TEL APH GLW ZBRA TRMB PTC TYL FICO ROP CDW TSM ASML SMCI PLTR SNOW CRWD DDOG NET ZS WDAY
TEAM SHOP UBER ABNB MDB OKTA
GOOGL META NFLX DIS CMCSA T VZ TMUS CHTR EA TTWO WBD OMC SPOT RBLX
AMZN TSLA HD LOW MCD SBUX NKE TJX ROST BKNG MAR HLT CMG YUM DPZ ORLY AZO GM F LULU ULTA DHI LEN NVR
PHM EBAY ETSY BBY TSCO DG DLTR TGT
COST WMT PG KO PEP PM MO MDLZ CL KMB GIS HSY KHC STZ MNST KDP SYY KR EL CHD CLX
LLY JNJ UNH PFE MRK ABBV ABT TMO DHR BMY AMGN GILD VRTX REGN ISRG SYK BSX MDT BDX ZTS CI ELV HUM
CNC MCK CAH IQV IDXX DXCM EW ALGN BIIB MRNA HCA A RMD STE CVS
JPM BAC WFC C GS MS SCHW BLK AXP V MA PYPL COF USB PNC TFC BK STT MMC AON AJG CB PGR TRV ALL MET
PRU AIG AFL SPGI MCO ICE CME NDAQ MSCI FIS GPN KKR BX APO ARES LPLA RJF EVR LAZ PJT HLI IBKR SF
AMP TROW BEN IVZ NTRS FITB KEY RF HBAN MTB CFG ALLY SYF
CAT DE HON GE RTX LMT NOC GD BA UPS FDX UNP CSX NSC WM RSG ETN EMR ITW PH ROK AME DOV XYL CMI PCAR
URI GWW FAST CTAS PAYX ADP JCI CARR OTIS TT LHX TDG HWM DAL UAL LUV AAL
XOM CVX COP EOG SLB OXY PSX MPC VLO HAL BKR DVN FANG KMI WMB OKE
LIN APD SHW ECL DD DOW NEM FCX NUE STLD VMC MLM PPG IP
NEE DUK SO D AEP EXC SRE XEL PEG ED WEC
PLD AMT CCI EQIX PSA O SPG WELL DLR AVB
""".split()
CONTEXT = ["XLK", "XLF", "XLV", "XLY", "XLC", "XLI", "XLE", "XLP", "XLU", "XLB", "XLRE", "SMH", "^VIX"]

def log(*a):
    print(*a, flush=True)

def _file(sym):
    return UNIVERSE_DIR / f"{sym.replace('^', '_')}.csv"

# ── data ──────────────────────────────────────────────────────────────────────
def fetch_universe(refresh=False, chunk=40):
    import yfinance as yf
    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    todo = [s for s in dict.fromkeys(UNIVERSE + CONTEXT) if refresh or not _file(s).exists()]
    ok = bad = 0
    for i in range(0, len(todo), chunk):
        batch = todo[i:i + chunk]
        try:
            raw = yf.download(batch, period=f"{YEARS}y", auto_adjust=True, group_by="ticker",
                              progress=False, threads=True)
        except Exception as e:
            log(f"  [warn] batch {i // chunk} failed: {e}"); bad += len(batch); time.sleep(10); continue
        for s in batch:
            try:
                d = raw[s].copy() if len(batch) > 1 else raw.copy()
                d = d.rename(columns=str.lower)[["open", "high", "low", "close", "volume"]].dropna(subset=["close"])
                d = d[d["close"] > 0]
                if len(d) < 300:
                    bad += 1; continue
                d.index = pd.to_datetime(d.index).tz_localize(None).normalize()
                d.index.name = "date"
                d.to_csv(_file(s)); ok += 1
            except Exception:
                bad += 1
        log(f"  fetched {min(i + chunk, len(todo))}/{len(todo)}")
        time.sleep(3)
    log(f"Universe: {ok} downloaded, {bad} unavailable -> {UNIVERSE_DIR}")

def load_frames():
    """Reference universe + context series + the watchlist (whose own CSVs hold full history)."""
    frames = {}
    for cp in sorted(UNIVERSE_DIR.glob("*.csv")):
        frames[cp.stem.replace("_VIX", "^VIX")] = pd.read_csv(cp, parse_dates=["date"]).set_index("date").sort_index()
    for sym in watchlist():
        cp = bt.DATA_DIR / f"{sym}.csv"
        if cp.exists():
            frames[sym] = pd.read_csv(cp, parse_dates=["date"]).set_index("date").sort_index()
    spy = bt.fetch_one(bt.BENCHMARK)
    return frames, spy

def watchlist_index(frames):
    """Equal-weight daily-rebalanced index of the watchlist stocks (level series)."""
    syms = [s for s in watchlist() if s in frames]
    rets = pd.DataFrame({s: frames[s]["close"].pct_change() for s in syms})
    return (1 + rets.mean(axis=1, skipna=True).fillna(0)).cumprod()

def _fwd(series, index, h=5):
    """h-day forward return of `series` on the dates of `index` (NaN where unavailable)."""
    s = series.reindex(index).ffill().to_numpy(dtype=float)
    out = np.full(len(s), np.nan)
    if len(s) > h:
        out[:-h] = s[h:] / s[:-h] - 1
    return out

def watchlist():
    if bt.LIVE_PARAMS_PATH.exists():
        return {s: v["category"] for s, v in json.loads(bt.LIVE_PARAMS_PATH.read_text(encoding="utf-8")).items()}
    return bt.get_watchlist()

def live_params():
    if bt.LIVE_PARAMS_PATH.exists():
        return {s: v["params"] for s, v in json.loads(bt.LIVE_PARAMS_PATH.read_text(encoding="utf-8")).items()}
    return {s: dict(bt.CATEGORY_DEFAULTS.get(c, bt.CATEGORY_DEFAULTS["high_vol"])) for s, c in bt.get_watchlist().items()}

# ── per stock/rule/threshold/year event totals ──────────────────────────────────
def _ticker_stats(args):
    sym, df, spy_close, bench = args
    df = df.iloc[-(YEARS * 252 + 300):]
    if len(df) < 400:
        return []
    cache = bt.ticker_cache(df)
    fwd, spy_fwd = bt.build_forward(df, spy_close)
    b5, b3 = _fwd(bench, df.index, 5), _fwd(bench, df.index, 3)
    years = df.index.year.to_numpy()
    rows = []
    for rule in CORE_RULES:
        for p in bt.GRIDS[rule]:
            trig, sig = bt.run_rule(rule, df, p, cache)
            ev = bt.event_returns(df, trig, sig, fwd, spy_fwd, min(300, len(df) // 3))
            if ev.empty:
                continue
            pos = ev["pos"].to_numpy()
            e5 = ev["exc5"].to_numpy(); e3 = ev["exc3"].to_numpy(); yr = years[pos]
            w5 = ev["ret5"].to_numpy() - np.nan_to_num(b5[pos]) - ROUND_TRIP_COST
            w3 = ev["ret3"].to_numpy() - np.nan_to_num(b3[pos]) - ROUND_TRIP_COST
            for y in np.unique(yr):
                m = yr == y
                rows.append((sym, rule, bt.params_label(rule, p), int(y), int(m.sum()),
                             float(e5[m].sum()), float((e5[m] ** 2).sum()), int((e5[m] > 0).sum()), float(np.nansum(e3[m])),
                             float(w5[m].sum()), float((w5[m] ** 2).sum()), int((w5[m] > 0).sum()), float(np.nansum(w3[m]))))
    return rows

def build_stats():
    frames, spy = load_frames()
    syms = [s for s in frames if s not in CONTEXT]
    log(f"Stats for {len(syms)} stocks x {sum(len(bt.GRIDS[r]) for r in CORE_RULES)} thresholds ...")
    t = time.time()
    bench = watchlist_index(frames)
    jobs = [(s, frames[s], spy["close"], bench) for s in syms]
    rows = []
    with Pool(max(1, os.cpu_count() or 1)) as pool:
        for i, r in enumerate(pool.imap_unordered(_ticker_stats, jobs, chunksize=4)):
            rows += r
            if (i + 1) % 25 == 0:
                log(f"  {i + 1}/{len(jobs)} stocks ({time.time() - t:.0f}s)")
    st = pd.DataFrame(rows, columns=["sym", "rule", "params", "year", "n", "s5", "q5", "h5", "s3",
                                     "sw", "qw", "hw", "s3w"])
    LEARN_DIR.mkdir(parents=True, exist_ok=True)
    st.to_csv(LEARN_DIR / "stats.csv.gz", index=False)
    grid = [{"rule": r, "params": bt.params_label(r, p), "params_json": json.dumps(p)} for r in CORE_RULES for p in bt.GRIDS[r]]
    pd.DataFrame(grid).to_csv(LEARN_DIR / "grid_params.csv", index=False)
    log(f"Stats: {len(st)} rows from {st['sym'].nunique()} stocks in {time.time() - t:.0f}s -> {LEARN_DIR/'stats.csv.gz'}")

def load_stats():
    return pd.read_csv(LEARN_DIR / "stats.csv.gz")

# ── peers ─────────────────────────────────────────────────────────────────────
def build_peers():
    frames, _ = load_frames()
    closes = pd.DataFrame({s: f["close"] for s, f in frames.items() if s not in CONTEXT})
    rets = closes.pct_change().iloc[-PEER_LOOKBACK:]
    ok = rets.columns[rets.notna().sum() >= PEER_LOOKBACK * 0.8]
    rets = rets[ok]
    vol = rets.std()
    peers = {}
    for sym in watchlist():
        if sym not in rets.columns:
            peers[sym] = []; continue
        corr = rets.corrwith(rets[sym]).drop(sym)
        ratio = vol / vol[sym]
        cand = corr[(ratio.reindex(corr.index) > 0.5) & (ratio.reindex(corr.index) < 2.0)].sort_values(ascending=False)
        peers[sym] = [{"sym": s, "corr": round(float(c), 3)} for s, c in cand.head(PEER_K).items()]
    LEARN_DIR.mkdir(parents=True, exist_ok=True)
    (LEARN_DIR / "peers.json").write_text(json.dumps(peers, indent=1))
    for s, p in peers.items():
        log(f"  {s}: {', '.join(x['sym'] for x in p[:8])} ... ({len(p)} peers)")
    return peers

def load_peers():
    return json.loads((LEARN_DIR / "peers.json").read_text())

# ── selection ─────────────────────────────────────────────────────────────────
def aggregate(stats, syms, years, target="spy"):
    """Pooled per-threshold statistics. target 'spy' = excess vs S&P 500; 'w' = vs the watchlist
    average after round-trip costs."""
    cols = ["s5", "q5", "h5", "s3"] if target == "spy" else ["sw", "qw", "hw", "s3w"]
    sub = stats[stats["sym"].isin(syms) & stats["year"].isin(years)]
    g = sub.groupby(["rule", "params"])[["n"] + cols].sum()
    g.columns = ["n", "s5", "q5", "h5", "s3"]
    g = g[g["n"] > 0].copy()
    g["mean"] = g["s5"] / g["n"]
    var = (g["q5"] / g["n"] - g["mean"] ** 2).clip(lower=1e-12)
    g["se"] = np.sqrt(var / g["n"])
    g["t"] = g["mean"] / g["se"]
    g["hit"] = g["h5"] / g["n"]
    g["mean3"] = g["s3"] / g["n"]
    return g

def select_new(stats, sym, peers, train_years, target="spy"):
    """Pooled peer evidence with the stock's own history shrunk toward it."""
    pool = aggregate(stats, [sym] + [p["sym"] for p in peers.get(sym, [])], train_years, target)
    own = aggregate(stats, [sym], train_years, target)
    out = {}
    for rule in CORE_RULES:
        if rule not in pool.index.get_level_values(0):
            out[rule] = None; continue
        pr = pool.loc[rule]
        cand = pr[(pr["n"] >= POOL_MIN_N) & (pr["mean"] > 0) & (pr["mean3"] > 0) & (pr["t"] >= POOL_MIN_T)]
        if cand.empty:
            out[rule] = None; continue
        orow = own.loc[rule] if rule in own.index.get_level_values(0) else pd.DataFrame()
        best, best_score = None, -1e9
        for params, r in cand.iterrows():
            n_o = float(orow.loc[params, "n"]) if params in orow.index else 0.0
            m_o = float(orow.loc[params, "mean"]) if params in orow.index else 0.0
            score = (n_o * m_o + SHRINK_K * r["mean"]) / (n_o + SHRINK_K)
            if score > best_score:
                best, best_score = params, score
        out[rule] = {"params": best, "score": best_score, "pool_n": int(cand.loc[best, "n"]),
                     "own_n": int(orow.loc[best, "n"]) if best in orow.index else 0}
    return out

def select_old(stats, sym, category, cats, train_years, target="spy"):
    """Approximation of the current method: the stock's own last 5 years; category pool fallback."""
    own = aggregate(stats, [sym], train_years[-5:], target)
    catpool = aggregate(stats, [s for s, c in cats.items() if c == category], train_years[-5:], target)
    out = {}
    for rule in CORE_RULES:
        pick = None
        for src, min_n in ((own, bt.MIN_N), (catpool, bt.MIN_N)):
            if rule not in src.index.get_level_values(0):
                continue
            r = src.loc[rule]
            c = r[(r["n"] >= min_n) & (r["mean"] > 0) & (r["mean3"] > 0)]
            if not c.empty:
                pick = {"params": c["mean"].idxmax(), "score": float(c["mean"].max())}
                break
        out[rule] = pick
    return out

def params_for(selection, base, grid_lookup):
    """Full app-style params dict from a selection: chosen thresholds + enable flags."""
    p = dict(base)
    for rule in CORE_RULES:
        pick = selection.get(rule)
        flag = list(bt._enable_flag(rule))[0]
        if pick is None:
            p[flag] = False
        else:
            p.update(grid_lookup[(rule, pick["params"])]); p[flag] = True
    return p

# ── walk-forward: old vs new on unseen years ──────────────────────────────────
def walkforward(test_years=(2021, 2022, 2023, 2024, 2025)):
    stats = load_stats(); peers = load_peers(); cats = watchlist(); base = live_params()
    grid = pd.read_csv(LEARN_DIR / "grid_params.csv")
    lookup = {(r.rule, r.params): json.loads(r.params_json) for r in grid.itertuples()}
    frames, spy = load_frames()
    spy_close = spy["close"]
    report = {"generated": datetime.now().strftime("%Y-%m-%d %H:%M"), "years": {}, "method": {
        "peers": PEER_K, "shrink_k": SHRINK_K, "pool_min_n": POOL_MIN_N, "pool_min_t": POOL_MIN_T}}
    prev = {"old": {}, "new": {}}; changes = {"old": 0, "new": 0}
    tot = {m: {"rule_n": 0, "rule_s": 0.0, "rule_h": 0, "sig": [], "port": [], "unt": []} for m in ("old", "new")}
    for Y in test_years:
        train = [y for y in range(Y - 10, Y)]
        yr = {}
        for method in ("old", "new"):
            sels = {s: (select_new(stats, s, peers, train) if method == "new"
                        else select_old(stats, s, cats[s], cats, train)) for s in cats}
            # stability: how many thresholds changed vs last year
            for s, sel in sels.items():
                for rule, pick in sel.items():
                    was = prev[method].get((s, rule), "unset")
                    now = pick["params"] if pick else None
                    if was != "unset" and was != now:
                        changes[method] += 1
                    prev[method][(s, rule)] = now
            # rule level: the chosen thresholds' own events in the unseen year
            rn = rs = rh = 0
            for s, sel in sels.items():
                for rule, pick in sel.items():
                    if not pick:
                        continue
                    m = stats[(stats["sym"] == s) & (stats["rule"] == rule) & (stats["params"] == pick["params"]) & (stats["year"] == Y)]
                    rn += int(m["n"].sum()); rs += float(m["s5"].sum()); rh += int(m["h5"].sum())
            # signal + portfolio level in the unseen year
            sig_ex, signals, bars = [], [], {}
            cal = [d.strftime("%Y-%m-%d") for d in spy.index if d.year == Y]
            for s in cats:
                if s not in frames:
                    continue
                df = frames[s].iloc[-(YEARS * 252 + 300):]
                p = params_for(sels[s], base.get(s, {}), lookup)
                labels = bt.strong_labels(df, p, bt.ticker_cache(df))
                fwd, sfwd = bt.build_forward(df, spy_close)
                dates = [d.strftime("%Y-%m-%d") for d in df.index]
                bars[s] = {d: (float(o), float(c)) for d, o, c in zip(dates, df["open"], df["close"]) if d[:4] == str(Y)}
                prevlab = ""
                for i, (d, lab) in enumerate(zip(dates, labels)):
                    if d[:4] == str(Y) and lab:
                        signals.append({"date": d, "sym": s, "label": lab, "price": None})
                        if not prevlab and np.isfinite(fwd[5][i]):
                            sig_ex.append(float(fwd[5][i] - (sfwd[5][i] if np.isfinite(sfwd[5][i]) else 0)))
                    prevlab = lab
            spy_y = {d.strftime("%Y-%m-%d"): float(c) for d, c in spy_close.items() if d.year == Y}
            res = portfolio_sim.simulate(bars, spy_y, cal, list(bars), signals)
            sm = res["summary"]
            yr[method] = {"rule_events": rn, "rule_mean_exc5": round(rs / rn * 100, 3) if rn else None,
                          "rule_hit": round(rh / rn * 100, 1) if rn else None,
                          "strong_signals": len(sig_ex),
                          "signal_mean_exc5": round(float(np.mean(sig_ex)) * 100, 3) if sig_ex else None,
                          "signal_hit": round(float(np.mean(np.array(sig_ex) > 0)) * 100, 1) if sig_ex else None,
                          "portfolio_pct": sm["strategy"]["total_pct"], "untouched_pct": sm["untouched"]["total_pct"],
                          "signals_added_pts": round(sm["strategy"]["total_pct"] - sm["untouched"]["total_pct"], 2)}
            t = tot[method]; t["rule_n"] += rn; t["rule_s"] += rs; t["rule_h"] += rh; t["sig"] += sig_ex
            t["port"].append(sm["strategy"]["total_pct"]); t["unt"].append(sm["untouched"]["total_pct"])
        report["years"][str(Y)] = yr
        log(f"  {Y}: old signals {yr['old']['strong_signals']} @ {yr['old']['signal_mean_exc5']}% | "
            f"new {yr['new']['strong_signals']} @ {yr['new']['signal_mean_exc5']}% | "
            f"added pts old {yr['old']['signals_added_pts']} new {yr['new']['signals_added_pts']}")
    summ = {}
    for m, t in tot.items():
        sig = np.array(t["sig"])
        summ[m] = {"rule_events": t["rule_n"],
                   "rule_mean_exc5": round(t["rule_s"] / t["rule_n"] * 100, 3) if t["rule_n"] else None,
                   "rule_hit": round(t["rule_h"] / t["rule_n"] * 100, 1) if t["rule_n"] else None,
                   "strong_signals": int(len(sig)),
                   "signal_mean_exc5": round(float(sig.mean()) * 100, 3) if len(sig) else None,
                   "signal_se": round(float(sig.std(ddof=1) / np.sqrt(len(sig))) * 100, 3) if len(sig) > 1 else None,
                   "signal_hit": round(float((sig > 0).mean()) * 100, 1) if len(sig) else None,
                   "signals_added_pts_per_year": round(float(np.mean(np.array(t["port"]) - np.array(t["unt"]))), 2),
                   "threshold_changes": changes[m]}
    report["summary"] = summ
    (LEARN_DIR / "walkforward.json").write_text(json.dumps(report, indent=1))
    log("\nWalk-forward (thresholds chosen only from earlier years, scored on the next year):")
    for m in ("old", "new"):
        s = summ[m]
        log(f"  {m.upper():3}: rule events {s['rule_events']} @ {s['rule_mean_exc5']}% (hit {s['rule_hit']}%) | "
            f"STRONG signals {s['strong_signals']} @ {s['signal_mean_exc5']}% ±{s['signal_se']} (hit {s['signal_hit']}%) | "
            f"signals added {s['signals_added_pts_per_year']} pts/yr | threshold changes {s['threshold_changes']}")
    return report

# ── phase 3: rule-firing moments with market context, for a learned model ───────
FEATURES = ["n_buy", "n_sell", "f_volatility", "f_support_resistance", "f_volume", "f_rsi", "dir",
            "mkt_up", "spy_ret20", "vix", "rs20", "vol20", "dd52", "ret1"]

def _vol_bucket(df):
    v = df["close"].pct_change().iloc[-756:].std()
    return "high_vol" if v > 0.022 else "mod_vol" if v > 0.015 else "low_vol"

def _ticker_events(args):
    """Every day at least one voting rule fires (at loose, volatility-bucket thresholds), with
    context features and forward outcomes. Loose thresholds on purpose: the model decides."""
    sym, df, spy, bench, vix = args
    df = df.iloc[-(YEARS * 252 + 300):]
    if len(df) < 400:
        return []
    params = dict(bt.CATEGORY_DEFAULTS[_vol_bucket(df)])
    cache = bt.ticker_cache(df)
    idx = df.index; n = len(df)
    buy = np.zeros(n); sell = np.zeros(n); fired = {}
    for rule in CORE_RULES:
        trig, sig = bt.run_rule(rule, df, params, cache)
        t = trig.to_numpy(dtype=bool); s = sig.to_numpy()
        fired[rule] = t
        buy += t & (s == "BUY"); sell += t & (s == "SELL")
    close = df["close"].to_numpy(dtype=float)
    cs = pd.Series(close, index=idx)
    ret1 = cs.pct_change().to_numpy()
    r20 = cs.pct_change(20).to_numpy()
    vol20 = cs.pct_change().rolling(20).std().to_numpy()
    dd52 = close / cs.rolling(252, min_periods=60).max().to_numpy() - 1
    spy_a = spy.reindex(idx).ffill()
    spy_r20 = spy_a.pct_change(20).to_numpy()
    mkt_up = (spy_a > spy_a.rolling(200, min_periods=150).mean()).to_numpy(dtype=float)
    vix_a = vix.reindex(idx).ffill().to_numpy(dtype=float) if vix is not None else np.full(n, 20.0)
    f5, s5, b5 = _fwd(cs, idx, 5), _fwd(spy, idx, 5), _fwd(bench, idx, 5)
    rows = []
    for i in np.where(buy + sell >= 1)[0]:
        if i < 260 or not np.isfinite(f5[i]):
            continue
        rows.append({"sym": sym, "date": idx[i].strftime("%Y-%m-%d"), "year": idx[i].year,
                     "n_buy": buy[i], "n_sell": sell[i],
                     **{f"f_{r}": float(fired[r][i]) for r in CORE_RULES},
                     "dir": 1.0 if buy[i] >= sell[i] else -1.0, "mkt_up": mkt_up[i], "spy_ret20": spy_r20[i],
                     "vix": vix_a[i], "rs20": r20[i] - spy_r20[i], "vol20": vol20[i], "dd52": dd52[i], "ret1": ret1[i],
                     "exc_spy": f5[i] - s5[i],
                     "exc_w": f5[i] - (b5[i] if np.isfinite(b5[i]) else 0.0) - ROUND_TRIP_COST})
    return rows

def build_events():
    frames, spy = load_frames()
    bench = watchlist_index(frames)
    vix = frames.get("^VIX", pd.DataFrame()).get("close")
    syms = [s for s in frames if s not in CONTEXT]
    t = time.time(); rows = []
    with Pool(max(1, os.cpu_count() or 1)) as pool:
        for r in pool.imap_unordered(_ticker_events, [(s, frames[s], spy["close"], bench, vix) for s in syms], chunksize=4):
            rows += r
    ev = pd.DataFrame(rows)
    ev.to_csv(LEARN_DIR / "events.csv.gz", index=False)
    log(f"Events: {len(ev)} rule-firing moments from {ev['sym'].nunique()} stocks in {time.time() - t:.0f}s")

def _fit(train, kind):
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import HistGradientBoostingClassifier
    X = train[FEATURES].fillna(0).to_numpy(); y = (train["exc_w"] > 0).astype(int).to_numpy()
    if kind == "linear":
        m = make_pipeline(StandardScaler(), LogisticRegression(C=0.3, max_iter=1000))
    else:
        m = HistGradientBoostingClassifier(max_depth=3, learning_rate=0.05, max_iter=200,
                                           l2_regularization=1.0, min_samples_leaf=200)
    m.fit(X, y)
    # Cut-off chosen on the training years only: the most selective level whose picks still
    # number >= 300 and had the best average outcome there.
    p = m.predict_proba(X)[:, 1]
    best_thr, best_mean = None, -1e9
    for q in (0.5, 0.7, 0.8, 0.9, 0.95, 0.975):
        thr = float(np.quantile(p, q)); sel = train["exc_w"].to_numpy()[p >= thr]
        if len(sel) >= 300 and sel.mean() > best_mean:
            best_thr, best_mean = thr, float(sel.mean())
    return m, best_thr

# ── the fair replay: every approach, thresholds/models from earlier years only ──
def replay(test_years=TEST_YEARS):
    stats = load_stats(); peers = load_peers(); cats = watchlist(); base = live_params()
    events = pd.read_csv(LEARN_DIR / "events.csv.gz")
    grid = pd.read_csv(LEARN_DIR / "grid_params.csv")
    lookup = {(r.rule, r.params): json.loads(r.params_json) for r in grid.itertuples()}
    frames, spy = load_frames()
    bench = watchlist_index(frames)
    spy_close = spy["close"]
    wl = [s for s in cats if s in frames]
    # per watchlist stock: day -> (outcome vs your stocks after costs, outcome vs S&P 500)
    outcome, dfs = {}, {}
    for s in wl:
        df = frames[s].iloc[-(YEARS * 252 + 300):]
        dfs[s] = (df, bt.ticker_cache(df), [d.strftime("%Y-%m-%d") for d in df.index])
        f5, s5, b5 = _fwd(df["close"], df.index, 5), _fwd(spy_close, df.index, 5), _fwd(bench, df.index, 5)
        outcome[s] = {d: (f5[i] - b5[i] - ROUND_TRIP_COST, f5[i] - s5[i])
                      for i, d in enumerate(dfs[s][2]) if np.isfinite(f5[i]) and np.isfinite(b5[i])}

    def threshold_signals(sels, Y):
        out = []
        for s in wl:
            df, cache, dates = dfs[s]
            labels = bt.strong_labels(df, params_for(sels[s], base.get(s, {}), lookup), cache)
            out += [{"date": d, "sym": s, "label": lab, "price": None}
                    for d, lab in zip(dates, labels) if lab and d[:4] == str(Y)]
        return out

    def evaluate(signals, Y):
        cal = [d.strftime("%Y-%m-%d") for d in spy_close.index if d.year == Y]
        bars = {s: {d: (float(o), float(c)) for d, o, c in zip(dfs[s][2], dfs[s][0]["open"], dfs[s][0]["close"]) if d[:4] == str(Y)} for s in wl}
        spy_y = {d.strftime("%Y-%m-%d"): float(c) for d, c in spy_close.items() if d.year == Y}
        res = portfolio_sim.simulate(bars, spy_y, cal, wl, signals)
        ow = [outcome[g["sym"]][g["date"]] for g in signals if g["date"] in outcome[g["sym"]]]
        w = np.array([o[0] for o in ow]); sp = np.array([o[1] for o in ow])
        return {"signal_days": len(signals), "vs_your_stocks": w.tolist(), "vs_spy": sp.tolist(),
                "added_pts": res["summary"]["strategy"]["total_pct"] - res["summary"]["untouched"]["total_pct"],
                "untouched_pct": res["summary"]["untouched"]["total_pct"]}

    methods = {
        "current": "Current method (tuned to beat the S&P 500)",
        "current_w": "Current method, tuned to beat your stocks after costs",
        "pooled_w": "Peer-pooled, tuned to beat your stocks after costs",
        "model_linear": "Learned model (linear) with market context",
        "model_trees": "Learned model (decision trees) with market context",
    }
    per = {m: {} for m in methods}
    for Y in test_years:
        train = list(range(Y - 10, Y))
        sig = {
            "current": threshold_signals({s: select_old(stats, s, cats[s], cats, train, "spy") for s in wl}, Y),
            "current_w": threshold_signals({s: select_old(stats, s, cats[s], cats, train, "w") for s in wl}, Y),
            "pooled_w": threshold_signals({s: select_new(stats, s, peers, train, "w") for s in wl}, Y),
        }
        cutoff = f"{Y - 1}-12-20"   # drop training moments whose 5-day outcome reaches into the test year
        tr = events[(events["year"] >= Y - 10) & (events["date"] < cutoff)]
        te = events[(events["year"] == Y) & (events["sym"].isin(wl))]
        for kind in ("linear", "trees"):
            m, thr = _fit(tr, kind)
            if thr is None or te.empty:
                sig[f"model_{kind}"] = []; continue
            p = m.predict_proba(te[FEATURES].fillna(0).to_numpy())[:, 1]
            pick = te[p >= thr]
            sig[f"model_{kind}"] = [{"date": r.date, "sym": r.sym, "price": None,
                                     "label": "STRONG BUY" if r.dir > 0 else "STRONG BOUNCE WATCH"} for r in pick.itertuples()]
        for mth in methods:
            per[mth][str(Y)] = evaluate(sig[mth], Y)
        log(f"  {Y}: " + " | ".join(f"{mth} {per[mth][str(Y)]['signal_days']}d {per[mth][str(Y)]['added_pts']:+.1f}pts" for mth in methods))

    summary = {}
    for mth, label in methods.items():
        w = np.array([x for y in per[mth].values() for x in y["vs_your_stocks"]])
        sp = np.array([x for y in per[mth].values() for x in y["vs_spy"]])
        added = np.array([y["added_pts"] for y in per[mth].values()])
        summary[mth] = {
            "label": label,
            "signal_days_per_year": round(float(np.mean([y["signal_days"] for y in per[mth].values()])), 1),
            "right_pct": round(float((w > 0).mean()) * 100, 1) if len(w) else None,
            "vs_your_stocks_pct": round(float(w.mean()) * 100, 2) if len(w) else None,
            "vs_your_stocks_se": round(float(w.std(ddof=1) / np.sqrt(len(w))) * 100, 2) if len(w) > 1 else None,
            "vs_spy_pct": round(float(sp.mean()) * 100, 2) if len(sp) else None,
            "added_pts_per_year": round(float(added.mean()), 2),
            "added_pts_by_year": {y: round(v["added_pts"], 2) for y, v in per[mth].items()},
            "years_positive": int((added > 0).sum()),
        }
    out = {"generated": datetime.now().strftime("%Y-%m-%d %H:%M"), "test_years": list(test_years),
           "cost_round_trip_pct": ROUND_TRIP_COST * 100, "methods": summary}
    (LEARN_DIR / "replay.json").write_text(json.dumps(out, indent=1))
    log("\nFair yearly replay (chosen from earlier years only, scored on the next year):")
    for mth, s in summary.items():
        log(f"  {s['label'][:52]:52} | {s['signal_days_per_year']:6} signal days/yr | right {s['right_pct']}% | "
            f"vs your stocks {s['vs_your_stocks_pct']}% ±{s['vs_your_stocks_se']} | vs S&P {s['vs_spy_pct']}% | "
            f"portfolio {s['added_pts_per_year']:+} pts/yr ({s['years_positive']}/{len(test_years)} yrs positive)")
    return out

# ── timing aid: does waiting for a dip signal get a better price for a planned purchase? ──
def no_signal_price(month):
    """Price paid for a planned monthly purchase when no dip signal appeared that month.
    `month` is that stock's daily bars for the calendar month (columns open/close, date index).
    Deadline rule: having waited all month in vain, the buyer buys at the month's last close —
    so waiting through a rising month counts as a real cost, not a free pass."""
    return float(month["close"].iloc[-1])

def _sell_votes(df, params, cache):
    """Per-day count of voting rules pointing to a dip (the BOUNCE WATCH side)."""
    votes = np.zeros(len(df))
    for rule in CORE_RULES:
        if params.get(list(bt._enable_flag(rule))[0], True) is False:
            continue
        trig, sig = bt.run_rule(rule, df, params, cache)
        votes += trig.to_numpy(dtype=bool) & (sig.to_numpy() == "SELL")
    return votes

def _after_dip(frames, base, wl, horizons=(10, 21)):
    """Buying right after a STRONG dip vs buying h trading days later, compared with the same
    comparison on ordinary days (stocks drift up, so buying earlier usually wins anyway — the dip's
    real value is the difference)."""
    acc = {}
    for sym, f in frames.items():
        if sym in CONTEXT:
            continue
        df = f.iloc[-(YEARS * 252 + 300):]
        if len(df) < 400:
            continue
        params = base.get(sym) or dict(bt.CATEGORY_DEFAULTS[_vol_bucket(df)])
        votes = _sell_votes(df, params, bt.ticker_cache(df))
        o = df["open"].to_numpy(dtype=float); c = df["close"].to_numpy(dtype=float); n = len(df)
        grp = "watchlist" if sym in wl else "reference"
        a = acc.setdefault(grp, {h: {"dip": [], "any": []} for h in horizons})
        for h in horizons:
            idx = np.arange(260, n - h - 1)
            later = c[idx + h] / o[idx + 1] - 1               # buy at next open vs close h days later
            a[h]["any"].append(later[::5])
            a[h]["dip"].append(later[votes[idx] >= 2])
    out = {}
    for grp, a in acc.items():
        out[grp] = {}
        for h, d in a.items():
            dip = np.concatenate(d["dip"]); anyd = np.concatenate(d["any"])
            out[grp][str(h)] = {"dip_days": int(len(dip)),
                                "dip_pct": round(float(dip.mean()) * 100, 2),
                                "dip_se": round(float(dip.std(ddof=1) / np.sqrt(len(dip))) * 100, 2) if len(dip) > 1 else None,
                                "dip_better_share": round(float((dip > 0).mean()) * 100, 1),
                                "ordinary_pct": round(float(anyd.mean()) * 100, 2),
                                "dip_bonus_pts": round(float(dip.mean() - anyd.mean()) * 100, 2)}
    return out

def timing_test():
    frames, _ = load_frames(); base = live_params(); wl = set(watchlist())
    rows = []
    for sym, f in frames.items():
        if sym in CONTEXT:
            continue
        df = f.iloc[-(YEARS * 252 + 300):]
        if len(df) < 400:
            continue
        params = base.get(sym) or dict(bt.CATEGORY_DEFAULTS[_vol_bucket(df)])
        votes = _sell_votes(df, params, bt.ticker_cache(df))
        opens = df["open"].to_numpy(dtype=float); closes = df["close"].to_numpy(dtype=float)
        months = df.index.to_period("M")
        pos = np.arange(len(df))
        for m in months.unique()[1:-1]:                       # full months only
            mi = pos[months == m]
            if len(mi) < 15 or mi[0] < 260:
                continue
            first_open, avg_close = opens[mi[0]], float(closes[mi].mean())
            month_df = df.iloc[mi[0]:mi[-1] + 1]
            for variant, need in (("any_dip", 1), ("strong_dip", 2)):
                hit = next((i for i in mi if votes[i] >= need and i + 1 < len(df)), None)
                price = opens[hit + 1] if hit is not None else no_signal_price(month_df)
                rows.append({"sym": sym, "watchlist": sym in wl, "month": str(m), "year": m.year,
                             "variant": variant, "signalled": hit is not None,
                             "vs_first_day": first_open / price - 1, "vs_month_avg": avg_close / price - 1})
    t = pd.DataFrame(rows)
    t.to_csv(LEARN_DIR / "timing.csv.gz", index=False)
    out = {"generated": datetime.now().strftime("%Y-%m-%d %H:%M"), "groups": {}, "after_dip": _after_dip(frames, base, wl)}
    for (variant, is_wl), g in t.groupby(["variant", "watchlist"]):
        key = f"{variant}|{'watchlist' if is_wl else 'reference'}"
        sig = g[g["signalled"]]
        out["groups"][key] = {
            "months": len(g), "signalled_share": round(float(g["signalled"].mean()) * 100, 1),
            "vs_first_day_pct": round(float(g["vs_first_day"].mean()) * 100, 2),
            "vs_first_day_se": round(float(g["vs_first_day"].std(ddof=1) / np.sqrt(len(g))) * 100, 2),
            "better_than_first_day": round(float((g["vs_first_day"] > 0).mean()) * 100, 1),
            "vs_month_avg_pct": round(float(g["vs_month_avg"].mean()) * 100, 2),
            "signal_months_vs_first_day_pct": round(float(sig["vs_first_day"].mean()) * 100, 2) if len(sig) else None,
            "by_year_vs_first_day": {int(y): round(float(v.mean()) * 100, 2) for y, v in g.groupby("year")["vs_first_day"]},
        }
    (LEARN_DIR / "timing.json").write_text(json.dumps(out, indent=1))
    for grp, hs in out["after_dip"].items():
        for h, v in hs.items():
            log(f"  after a STRONG dip ({grp}), buy now vs {h} days later: {v['dip_pct']:+.2f}% ±{v['dip_se']} "
                f"(ordinary day {v['ordinary_pct']:+.2f}%, dip bonus {v['dip_bonus_pts']:+.2f} pts, {v['dip_days']} dips)")
    log("\nTiming aid (planned monthly purchase; + = more shares for the same money):")
    for k, v in out["groups"].items():
        log(f"  {k:26} months {v['months']:6} | signal in {v['signalled_share']}% of months | "
            f"vs first-day buy {v['vs_first_day_pct']:+.2f}% ±{v['vs_first_day_se']} (better {v['better_than_first_day']}% of months) | "
            f"vs month average {v['vs_month_avg_pct']:+.2f}% | signal months only {v['signal_months_vs_first_day_pct']}%")
    return out

def main():
    ap = argparse.ArgumentParser(description="Tripwire learning layer")
    ap.add_argument("cmd", choices=["fetch", "stats", "peers", "walkforward", "events", "replay", "timing", "all"])
    ap.add_argument("--refresh", action="store_true")
    a = ap.parse_args()
    if a.cmd in ("fetch", "all"):
        fetch_universe(refresh=a.refresh)
    if a.cmd in ("stats", "all"):
        build_stats()
    if a.cmd in ("peers", "all"):
        build_peers()
    if a.cmd == "walkforward":
        walkforward()
    if a.cmd in ("events", "all"):
        build_events()
    if a.cmd in ("replay", "all"):
        replay()
    if a.cmd == "timing":
        timing_test()

if __name__ == "__main__":
    main()
