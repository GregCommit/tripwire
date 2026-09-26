"""
Tripwire v4 - Single file, self-contained
==========================================
Run:  python app.py
Open: http://localhost:5000
"""

import sqlite3, threading, time, json, os, logging, csv, io, smtplib, urllib.parse, urllib.request, subprocess, sys, hmac, secrets, hashlib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from html import escape as h_esc
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import statistics
try:
    from zoneinfo import ZoneInfo
    EASTERN = ZoneInfo("America/New_York")
except Exception:
    EASTERN = None
from flask import Flask, jsonify, request, Response, session, redirect, url_for, stream_with_context
from flask_cors import CORS
import yfinance as yf
import portfolio_sim

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tripwire")

app = Flask(__name__)
app.secret_key = os.environ.get("TRIPWIRE_SECRET_KEY", "tripwire-dev-secret-change-me")
CORS(app)

AUTH_PASSWORD = os.environ.get("TRIPWIRE_PASSWORD")
if not AUTH_PASSWORD:
    AUTH_PASSWORD = "tripwire"
    log.warning("TRIPWIRE_PASSWORD not set — using default password 'tripwire'. Set TRIPWIRE_PASSWORD env var for real use.")

worker_pool = ThreadPoolExecutor(max_workers=4)
state_lock = threading.Lock()
extended_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH = Path.home() / ".tripwire_v3" / "state.db"

def get_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT PRIMARY KEY,
                category TEXT DEFAULT 'high_vol',
                active INTEGER DEFAULT 1,
                added_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY,
                symbol TEXT, timestamp INTEGER,
                open REAL, high REAL, low REAL,
                close REAL, prev_close REAL,
                pre_market REAL, post_market REAL,
                UNIQUE(symbol, timestamp)
            );
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY,
                symbol TEXT, timestamp INTEGER,
                rule_type TEXT, message TEXT, price REAL,
                detail TEXT
            );
            CREATE TABLE IF NOT EXISTS rule_params (
                symbol TEXT PRIMARY KEY,
                params TEXT
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY,
                role TEXT, content TEXT, ts INTEGER
            );
            CREATE TABLE IF NOT EXISTS access_log (
                id INTEGER PRIMARY KEY,
                ts INTEGER, kind TEXT, role TEXT,
                vid TEXT, ip TEXT, device TEXT, detail TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_access_ts ON access_log(ts);
            CREATE TABLE IF NOT EXISTS sim_daily (
                date TEXT PRIMARY KEY, start TEXT,
                strategy REAL, untouched REAL, spy REAL
            );
            CREATE TABLE IF NOT EXISTS sim_backtest_runs (
                id INTEGER PRIMARY KEY, ts INTEGER, start_date TEXT, end_date TEXT,
                strategy_end REAL, untouched_end REAL, spy_end REAL,
                strategy_cagr REAL, untouched_cagr REAL, spy_cagr REAL,
                fair_strategy_pct REAL, fair_untouched_pct REAL, fair_spy_pct REAL,
                fingerprint TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_prices_symbol_ts ON prices(symbol, timestamp);
            CREATE INDEX IF NOT EXISTS idx_alerts_symbol_rule_ts ON alerts(symbol, rule_type, timestamp);
        """)
        for tbl, col, coldef in [
            ("alerts", "detail", "TEXT"),
            ("alerts", "ack", "INTEGER DEFAULT 0"),
            ("prices", "volume", "REAL"),
            ("stocks", "added_by", "TEXT"),           # 'guest' for tickers added via guest login
            # Signal events: alerts logged for one symbol in one check cycle share event_id
            ("alerts", "event_id", "INTEGER"),
            ("alerts", "decision", "TEXT"),           # 'acted' | 'passed' (owner's own call)
            ("alerts", "decision_ts", "INTEGER"),
            ("alerts", "followup_sent", "INTEGER"),   # 1 once the 5-day follow-up was emailed
            # Outcome tracking: filled in ~5 trading days after each alert (see resolve_outcomes)
            ("alerts", "outcome_ret", "REAL"),        # stock return since alert price, %
            ("alerts", "outcome_excess", "REAL"),     # signal-direction excess vs SPY, %
            ("alerts", "outcome_correct", "INTEGER"), # 1 if signal-direction excess > 0
            ("alerts", "outcome_ts", "INTEGER"),      # when it was resolved
        ]:
            try:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {coldef}")
                conn.commit()
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    log.warning("Migration '%s.%s' skipped: %s", tbl, col, e)
        defaults = [
            ("MSFT","mod_vol"),("AAPL","mod_vol"),
            ("NVDA","high_vol"),("MU","high_vol"),
            ("ARM","high_vol"),("SNDK","high_vol"),
            ("JPM","low_vol"),("ABBV","low_vol"),
            ("LLY","low_vol"),("EVR","high_vol"),
        ]
        for sym, cat in defaults:
            conn.execute(
                "INSERT OR IGNORE INTO stocks (symbol,category,active,added_at) VALUES (?,?,1,?)",
                (sym, cat, int(time.time()))
            )
        conn.commit()

init_db()

# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

SETTINGS_DEFAULTS = {
    "check_interval_seconds":        "60",
    "check_interval_closed_seconds": "900",
    "market_hours_only":             "0",
    "alert_cooldown_hours":          "24",
    "show_info_tier":                "1",
    "notify_email_enabled":          "0",
    "smtp_host":                     "",
    "smtp_port":                     "587",
    "smtp_user":                     "",
    "smtp_password":                 "",
    "notify_email_to":               "",
    "notify_whatsapp_enabled":       "0",
    "callmebot_phone":               "",
    "callmebot_apikey":              "",
    "browser_notifications_enabled": "1",
    "alert_sound_enabled":           "0",
    # Deliberate behavior change (see BACKTESTING.md): default ON so push/email/WhatsApp only
    # fire for STRONG (multi-rule-confirmed) signals — every alert still logs to the DB/UI.
    "notify_strong_only":            "1",
    # Daily digest: when on, instant push/email/WhatsApp is suppressed and a single
    # once-a-day summary is sent at digest_hour (local time) instead.
    "daily_digest_enabled":          "0",
    "auto_recal_enabled":            "1",
    "digest_hour":                   "8",
    "ai_synthesis_model":            "claude-sonnet-4-6",
    "ai_assistant_model":            "claude-opus-4-8",
    "anthropic_api_key":             "",
}
# Keys never returned to the client (write-only secrets)
SETTINGS_SECRET = {"smtp_password", "callmebot_apikey", "anthropic_api_key"}

_settings_cache = {}
_settings_lock = threading.Lock()

def _load_settings():
    with get_db() as conn:
        rows = conn.execute("SELECT key,value FROM settings").fetchall()
    with _settings_lock:
        _settings_cache.clear()
        _settings_cache.update(SETTINGS_DEFAULTS)
        for r in rows:
            _settings_cache[r["key"]] = r["value"]

def get_setting(key, default=None):
    with _settings_lock:
        if key in _settings_cache:
            return _settings_cache[key]
    return SETTINGS_DEFAULTS.get(key, default) if default is None else default

def get_setting_int(key, default=0):
    try:
        return int(float(get_setting(key, str(default))))
    except (TypeError, ValueError):
        return default

def get_setting_bool(key):
    return str(get_setting(key, "0")) in ("1", "true", "True", "on", "yes")

def set_setting(key, value):
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, str(value)))
        conn.commit()
    with _settings_lock:
        _settings_cache[key] = str(value)

def _get_calibrated_at_fresh():
    """Read 'calibrated_at' straight from the DB rather than the boot-time settings cache.
    backtest.py's `apply` writes this key directly into the same settings table while the
    app may already be running, so a cached read could show a stale (or missing) date."""
    try:
        with get_db() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key='calibrated_at'").fetchone()
        return row["value"] if row else None
    except Exception as e:
        log.warning("_get_calibrated_at_fresh failed: %s", e)
        return None

_load_settings()

def get_anthropic_key():
    """API key from the Settings tab (stored in the local DB), falling back to
    the ANTHROPIC_API_KEY environment variable. Settings value takes priority."""
    return (get_setting("anthropic_api_key", "") or os.environ.get("ANTHROPIC_API_KEY", "")).strip()

# ─────────────────────────────────────────────────────────────────────────────
# MARKET HOURS  &  YAHOO RESILIENCE
# ─────────────────────────────────────────────────────────────────────────────

def market_phase(now=None):
    """Return 'regular' | 'pre' | 'post' | 'closed' in US/Eastern.
    Holidays are treated as normal trading days (documented v1 limitation)."""
    if EASTERN is None:
        return "regular"  # can't determine tz — never gate
    now = now or datetime.now(EASTERN)
    if now.tzinfo is None:
        now = now.replace(tzinfo=EASTERN)
    if now.weekday() >= 5:  # Sat/Sun
        return "closed"
    minutes = now.hour * 60 + now.minute
    if 4 * 60 <= minutes < 9 * 60 + 30:      return "pre"
    if 9 * 60 + 30 <= minutes < 16 * 60:     return "regular"
    if 16 * 60 <= minutes < 20 * 60:         return "post"
    return "closed"

_yf_cache = {}          # key -> (expires_ts, value)
_yf_cache_lock = threading.Lock()

def yf_cached(key, ttl, fn):
    """Cache fn() under key for ttl seconds. On exception: retry once, then
    fall back to the last cached value (even if expired) if available."""
    now = time.time()
    with _yf_cache_lock:
        hit = _yf_cache.get(key)
        if hit and hit[0] > now:
            return hit[1]
    try:
        val = fn()
    except Exception:
        time.sleep(0.5)
        try:
            val = fn()
        except Exception as e:
            with _yf_cache_lock:
                stale = _yf_cache.get(key)
            if stale is not None:
                log.warning("yf_cached(%s) failed, serving stale: %s", key, e)
                return stale[1]
            raise
    with _yf_cache_lock:
        _yf_cache[key] = (now + ttl, val)
    return val

def _fetch_spy_regime_raw():
    hist = yf.Ticker("SPY").history(period="300d")
    if hist is None or hist.empty or len(hist) < 200:
        return None
    closes = hist["Close"]
    sma200 = float(closes.tail(200).mean())
    last = float(closes.iloc[-1])
    return "bear" if last < sma200 else "bull"

def get_market_regime():
    """'bull' | 'bear' based on SPY close vs its 200-day SMA. On any failure (including
    insufficient history) defaults to 'bull' — the calibration window itself was bull-heavy,
    so silently assuming bull is the conservative default (no unwarranted bear caveat)."""
    try:
        regime = yf_cached("spy_regime", 6 * 3600, _fetch_spy_regime_raw)
        return regime or "bull"
    except Exception as e:
        log.warning("get_market_regime failed: %s", e)
        return "bull"

# ─────────────────────────────────────────────────────────────────────────────
# RULE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# Params shared by every category (rule toggles + new-rule thresholds).
# Thresholds below reflect backtest.py findings (5yr event-study, see BACKTESTING.md):
# MA crossover showed ~no edge at the app's alert horizon, so it's off by default —
# individual tickers can still have it enabled via a per-symbol override (see rule_params).
_COMMON_RULE_DEFAULTS = {
    "enable_volatility": True,
    "enable_support_resistance": True,
    # Volume spike
    "enable_volume": True, "volume_multiplier": 3.0, "volume_lookback": 20,
    # Gap at open
    "enable_gap": True, "gap_pct": 3.0,
    # RSI(14)
    "enable_rsi": True, "rsi_period": 14, "rsi_overbought": 70, "rsi_oversold": 30,
    # Moving-average crossover — disabled by default; see comment above.
    "enable_ma": False, "ma_short": 20, "ma_long": 50, "ma_cross_lookback": 3,
}

def _mk_rules(base):
    d = dict(_COMMON_RULE_DEFAULTS)
    d.update(base)
    return d

DEFAULT_RULES = {
    "high_vol": _mk_rules({"volatility_multiplier":3.75,"volatility_lookback":10,"support_resist_pct":8.0,"support_resist_lookback":120,"consecutive_down_days":3,"use_consecutive":True,"volume_multiplier":3.0,"volume_lookback":20,"gap_pct":5.0,"rsi_period":21,"rsi_overbought":80,"rsi_oversold":35}),
    "mod_vol":  _mk_rules({"volatility_multiplier":3.75,"volatility_lookback":30,"support_resist_pct":5.0,"support_resist_lookback":30,"consecutive_down_days":3,"use_consecutive":False,"volume_multiplier":2.0,"volume_lookback":20,"gap_pct":2.0,"rsi_period":14,"rsi_overbought":80,"rsi_oversold":30}),
    "low_vol":  _mk_rules({"volatility_multiplier":4.0,"volatility_lookback":30,"support_resist_pct":3.0,"support_resist_lookback":30,"consecutive_down_days":3,"use_consecutive":False,"volume_multiplier":2.5,"volume_lookback":10,"gap_pct":3.0,"rsi_period":21,"rsi_overbought":80,"rsi_oversold":35}),
}

# Pre-retune (v3-era) category thresholds, kept only for the informational "activity" tier
# (see INFO_RULES usage in run_check / evaluate_rules). These are deliberately looser than
# DEFAULT_RULES above — the backtest found DEFAULT_RULES' tighter thresholds are the ones
# worth alerting on, but the old looser thresholds still give day-to-day awareness without
# claiming any predictive edge. Mirrors backtest.py's CATEGORY_DEFAULTS exactly.
INFO_RULES = {
    "high_vol": _mk_rules({"volatility_multiplier":2.5,"volatility_lookback":20,"support_resist_pct":7.0,"support_resist_lookback":90,"consecutive_down_days":3,"use_consecutive":True,"volume_multiplier":3.0,"volume_lookback":20,"gap_pct":3.0,"rsi_period":14,"rsi_overbought":70,"rsi_oversold":30}),
    "mod_vol":  _mk_rules({"volatility_multiplier":1.5,"volatility_lookback":20,"support_resist_pct":5.0,"support_resist_lookback":30,"consecutive_down_days":3,"use_consecutive":False,"volume_multiplier":2.5,"volume_lookback":20,"gap_pct":2.0,"rsi_period":14,"rsi_overbought":70,"rsi_oversold":30}),
    "low_vol":  _mk_rules({"volatility_multiplier":1.2,"volatility_lookback":20,"support_resist_pct":4.0,"support_resist_lookback":60,"consecutive_down_days":3,"use_consecutive":False,"volume_multiplier":2.0,"volume_lookback":20,"gap_pct":1.5,"rsi_period":14,"rsi_overbought":70,"rsi_oversold":30}),
}

# ── Backtest evidence (optional; absent file = feature silently off) ──────────────
RULE_STATS_PATH = Path(__file__).parent / "backtest_results" / "rule_stats.json"

def _load_rule_stats():
    try:
        if RULE_STATS_PATH.exists():
            return json.loads(RULE_STATS_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("Failed to load rule_stats.json: %s", e)
    return {}

RULE_STATS = _load_rule_stats()

def _rule_stats_for(symbol, rule_type):
    """Backtested evidence dict for one symbol/rule, or None if unavailable."""
    return RULE_STATS.get(symbol, {}).get(rule_type)

def _rule_stats_line(symbol, rule_type):
    """Compact human-readable evidence string for AI prompts, e.g.
    'Backtested: +3.2% avg vs SPY over 5d, 62% hit rate, n=41, worst case -2.8% (low confidence)'.
    Returns '' if no backtest evidence is available for this symbol/rule."""
    st = _rule_stats_for(symbol, rule_type)
    if not st:
        return ""
    parts = [f"{st['exc5']:+.1f}% avg vs SPY over 5d"]
    if st.get("hit5") is not None:
        parts.append(f"{st['hit5']:.0f}% hit rate")
    parts.append(f"n={st['n']}")
    if st.get("mae") is not None:
        parts.append(f"worst case {st['mae']:+.1f}%")
    conf = " (low confidence — short trading history)" if st.get("short_history") else (" (low confidence — n<30)" if st["n"] < 30 else "")
    return f"Backtested: {', '.join(parts)}{conf}"

def get_stocks():
    with get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM stocks WHERE active=1 ORDER BY symbol")]

def is_guest_stock(symbol):
    """Guest-added tickers are monitored and shown like any other, but never trigger the
    owner's outbound notifications or paid AI news synthesis."""
    with get_db() as conn:
        row = conn.execute("SELECT added_by FROM stocks WHERE symbol=?", (symbol,)).fetchone()
    return bool(row) and row["added_by"] == "guest"

SENSITIVITY_LEVELS = ("conservative", "calibrated", "sensitive")

def _apply_sensitivity(params, category, level):
    """Shift a category's calibrated thresholds toward fewer (conservative) or more (sensitive)
    alerts. 'calibrated' is the backtest-tuned baseline and is returned unchanged. 'sensitive'
    adopts the looser pre-tuning (info-tier) thresholds; 'conservative' tightens them ~30%."""
    p = dict(params)
    if level == "sensitive":
        info = INFO_RULES.get(category, {})
        for k in ("volatility_multiplier", "support_resist_pct", "gap_pct",
                  "volume_multiplier", "rsi_overbought", "rsi_oversold"):
            if k in info:
                p[k] = info[k]
    elif level == "conservative":
        if "volatility_multiplier" in p: p["volatility_multiplier"] = round(p["volatility_multiplier"] * 1.3, 2)
        if "volume_multiplier" in p:     p["volume_multiplier"] = round(p["volume_multiplier"] * 1.3, 2)
        if "support_resist_pct" in p:    p["support_resist_pct"] = round(p["support_resist_pct"] + 2, 1)
        if "gap_pct" in p:               p["gap_pct"] = round(p["gap_pct"] * 1.5, 2)
        if "rsi_overbought" in p:        p["rsi_overbought"] = min(90, p["rsi_overbought"] + 5)
        if "rsi_oversold" in p:          p["rsi_oversold"] = max(10, p["rsi_oversold"] - 5)
    return p

def get_params(symbol, category):
    base = DEFAULT_RULES.get(category, DEFAULT_RULES["high_vol"]).copy()
    saved = {}
    with get_db() as conn:
        row = conn.execute("SELECT params FROM rule_params WHERE symbol=?", (symbol,)).fetchone()
    if row:
        saved = json.loads(row["params"])
    level = saved.get("_sensitivity", "calibrated")
    base = _apply_sensitivity(base, category, level)   # preset first…
    for k, v in saved.items():                          # …then any explicit (advanced) overrides win
        if k == "_sensitivity":
            continue
        base[k] = v
    base["_sensitivity"] = level
    return base

def save_params(symbol, params):
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO rule_params (symbol,params) VALUES (?,?)",
                     (symbol, json.dumps(params)))
        conn.commit()

def reset_params(symbol):
    with get_db() as conn:
        conn.execute("DELETE FROM rule_params WHERE symbol=?", (symbol,))
        conn.commit()

# ─────────────────────────────────────────────────────────────────────────────
# PRICE FETCHING
# ─────────────────────────────────────────────────────────────────────────────

extended_state = {}  # symbol -> {lifetime_high, lifetime_low, earnings_date, updated}

def _fetch_quote_raw(symbol):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="5d")
    if hist.empty or len(hist) < 1:
        raise ValueError(f"No data for {symbol}")
    current = hist.iloc[-1]
    prev    = hist.iloc[-2] if len(hist) > 1 else current
    vol = current.get("Volume") if hasattr(current, "get") else current["Volume"]
    result = {
        "close":      round(float(current["Close"]), 2),
        "prev_close": round(float(prev["Close"]), 2),
        "open":       round(float(current["Open"]), 2),
        "high":       round(float(current["High"]), 2),
        "low":        round(float(current["Low"]), 2),
        "volume":     int(vol) if vol and vol == vol else None,  # NaN guard
        "timestamp":  int(time.time()),
        "pre_market": None, "post_market": None,
        "week52_high": None, "week52_low": None,
    }
    try:
        info = ticker.fast_info
        pre  = getattr(info, "pre_market_price",  None)
        post = getattr(info, "post_market_price", None)
        w52h = getattr(info, "year_high", None)
        w52l = getattr(info, "year_low",  None)
        if pre  and pre  > 0: result["pre_market"]  = round(float(pre),  2)
        if post and post > 0: result["post_market"] = round(float(post), 2)
        if w52h and w52h > 0: result["week52_high"] = round(float(w52h), 2)
        if w52l and w52l > 0: result["week52_low"]  = round(float(w52l), 2)
    except: pass
    return result

def fetch_quote(symbol):
    return yf_cached(f"quote:{symbol}", 30, lambda: _fetch_quote_raw(symbol))

def fetch_earnings_date(symbol):
    """Next upcoming earnings date as a 'YYYY-MM-DD' string, or None."""
    def _raw():
        ticker = yf.Ticker(symbol)
        cand = []
        try:
            cal = ticker.calendar
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date")
                if isinstance(ed, (list, tuple)): cand.extend(ed)
                elif ed is not None: cand.append(ed)
            elif cal is not None and hasattr(cal, "loc") and "Earnings Date" in getattr(cal, "index", []):
                cand.append(cal.loc["Earnings Date"][0])
        except Exception:
            pass
        try:
            df = ticker.get_earnings_dates(limit=8)
            if df is not None and not df.empty:
                cand.extend(list(df.index))
        except Exception:
            pass
        today = datetime.now().date()
        best = None
        for c in cand:
            try:
                d = c.date() if hasattr(c, "date") else c
                if d >= today and (best is None or d < best):
                    best = d
            except Exception:
                continue
        return best.strftime("%Y-%m-%d") if best else None
    try:
        return yf_cached(f"earnings:{symbol}", 12 * 3600, _raw)
    except Exception as e:
        log.warning("fetch_earnings_date(%s) failed: %s", symbol, e)
        return None

def fetch_extended_data(symbol):
    try:
        hist = yf_cached(f"maxhist:{symbol}", 12 * 3600,
                         lambda: yf.Ticker(symbol).history(period="max"))
        earnings = fetch_earnings_date(symbol)
        if hist is not None and not hist.empty:
            with extended_lock:
                prev = dict(extended_state.get(symbol, {}))
                prev.update({
                    "lifetime_high": round(float(hist["High"].max()), 2),
                    "lifetime_low":  round(float(hist["Low"].min()),  2),
                    "earnings_date": earnings,
                    "updated": int(time.time()),
                })
                extended_state[symbol] = prev
    except Exception as e:
        log.warning("fetch_extended_data(%s) failed: %s", symbol, e)

def populate_history(symbol):
    """Seed prices DB with 90 days of daily closes from Yahoo (runs once per stock at startup)."""
    try:
        hist = yf_cached(f"hist90:{symbol}", 3600,
                         lambda: yf.Ticker(symbol).history(period="90d"))
        if hist is None or hist.empty:
            return
        rows = list(hist.iterrows())
        with get_db() as conn:
            for i, (ts, row) in enumerate(rows):
                if row["Close"] != row["Close"]:  # Yahoo sometimes returns an empty (NaN) bar
                    continue
                day_ts = int(ts.timestamp())
                prev_close = round(float(rows[i-1][1]["Close"]), 2) if i > 0 else round(float(row["Close"]), 2)
                vol = row.get("Volume")
                vol_val = int(vol) if vol and vol == vol else None  # NaN guard
                conn.execute("""
                    INSERT OR IGNORE INTO prices
                    (symbol,timestamp,open,high,low,close,prev_close,pre_market,post_market,volume)
                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (symbol, day_ts,
                     round(float(row["Open"]),2), round(float(row["High"]),2),
                     round(float(row["Low"]),2),  round(float(row["Close"]),2),
                     prev_close, None, None, vol_val))
                # Heal pre-v4 rows: INSERT OR IGNORE is a no-op when the (symbol,timestamp) row
                # already exists, so a row seeded before the `volume` column existed would keep
                # a permanently NULL volume forever even after this v4 re-fetch — which is why
                # the volume-spike rule was reporting "Insufficient volume data" in production.
                if vol_val is not None:
                    conn.execute(
                        "UPDATE prices SET volume=? WHERE symbol=? AND timestamp=? AND volume IS NULL",
                        (vol_val, symbol, day_ts))
            conn.commit()
    except Exception as e:
        log.warning("populate_history(%s) failed: %s", symbol, e)

def refresh_extended_loop():
    stocks = get_stocks()
    for s in stocks:
        fetch_extended_data(s["symbol"])
        populate_history(s["symbol"])
    while True:
        time.sleep(86400)
        for s in get_stocks():
            fetch_extended_data(s["symbol"])

threading.Thread(target=refresh_extended_loop, daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────
# OUTCOME TRACKING — score each alert ~5 trading days later (self-audit vs backtest)
# ─────────────────────────────────────────────────────────────────────────────
OUTCOME_HORIZON_DAYS = 5   # trading days after the alert to measure
# Every signal is judged by whether the stock then beats the market: BUY is a momentum call and a
# BOUNCE WATCH (downside trigger) is a rebound call. Must match SIGNAL_DIR in backtest.py.
SIGNAL_DIR = {"BUY": 1.0, "SELL": 1.0}

def _rescore_bounce_watch_once():
    """Outcomes scored before SIGNAL_DIR treated BOUNCE WATCH as a rebound call stored the
    opposite sign for SELL-signal alerts; flip them once so history matches current scoring."""
    if get_setting("bounce_rescored", "") == "1":
        return
    with get_db() as conn:
        rows = conn.execute("SELECT id, detail, outcome_excess FROM alerts WHERE outcome_excess IS NOT NULL").fetchall()
        n = 0
        for r in rows:
            try:
                sig = (json.loads(r["detail"]) or {}).get("signal") if r["detail"] else None
            except Exception:
                sig = None
            if sig == "SELL":
                ex = -r["outcome_excess"]
                conn.execute("UPDATE alerts SET outcome_excess=?, outcome_correct=? WHERE id=?",
                             (ex, 1 if ex > 0 else 0, r["id"]))
                n += 1
        conn.commit()
    set_setting("bounce_rescored", "1")
    log.info("Re-scored %d BOUNCE WATCH outcomes as rebound calls", n)

def _daily_close_series(symbol):
    """Cached {date -> close} from yfinance for outcome scoring (13mo of daily bars)."""
    def _fetch():
        h = yf.Ticker(symbol).history(period="13mo")
        if h is None or h.empty:
            return {}
        return {ts.strftime("%Y-%m-%d"): round(float(r["Close"]), 2) for ts, r in h.iterrows()}
    try:
        return yf_cached(f"outcome_daily:{symbol}", 3600, _fetch)
    except Exception:
        return {}

def resolve_outcomes():
    """For alerts old enough to have matured (>= OUTCOME_HORIZON_DAYS trading days) but not yet
    scored, compute the stock's return since the alert and its signal-direction excess vs SPY."""
    cutoff = int(time.time()) - int((OUTCOME_HORIZON_DAYS + 3) * 86400)  # calendar buffer for weekends
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id,symbol,timestamp,price,detail FROM alerts "
            "WHERE outcome_ts IS NULL AND timestamp <= ? ORDER BY timestamp ASC LIMIT 500",
            (cutoff,)
        ).fetchall()
    if not rows:
        return 0
    spy = _daily_close_series("SPY")
    resolved = 0
    for r in rows:
        try:
            detail = json.loads(r["detail"]) if r["detail"] else {}
        except Exception:
            detail = {}
        sdir = SIGNAL_DIR.get(detail.get("signal"))
        series = _daily_close_series(r["symbol"])
        if not series:
            continue
        adate = datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d")
        dates = sorted(series.keys())
        # first trading day on/after the alert date, then OUTCOME_HORIZON_DAYS later
        start_i = next((i for i, d in enumerate(dates) if d >= adate), None)
        if start_i is None or start_i + OUTCOME_HORIZON_DAYS >= len(dates):
            continue  # not enough forward data yet
        entry = r["price"] or series[dates[start_i]]
        exit_close = series[dates[start_i + OUTCOME_HORIZON_DAYS]]
        ret = (exit_close / entry - 1.0) * 100 if entry else 0.0
        excess = None; correct = None
        if sdir is not None:
            spy_ret = 0.0
            if spy:
                sd = sorted(spy.keys())
                si = next((i for i, d in enumerate(sd) if d >= adate), None)
                if si is not None and si + OUTCOME_HORIZON_DAYS < len(sd):
                    spy_ret = (spy[sd[si + OUTCOME_HORIZON_DAYS]] / spy[sd[si]] - 1.0) * 100
            excess = sdir * (ret - spy_ret)
            correct = 1 if excess > 0 else 0
        with get_db() as conn:
            conn.execute(
                "UPDATE alerts SET outcome_ret=?, outcome_excess=?, outcome_correct=?, outcome_ts=? WHERE id=?",
                (round(ret, 2), round(excess, 2) if excess is not None else None, correct, int(time.time()), r["id"])
            )
            conn.commit()
        resolved += 1
    if resolved:
        log.info("resolve_outcomes: scored %d matured alerts", resolved)
    return resolved

def outcome_loop():
    time.sleep(90)  # let first checks/history settle
    try:
        _rescore_bounce_watch_once()
    except Exception as e:
        log.warning("bounce-watch rescore failed: %s", e)
    while True:
        try:
            resolve_outcomes()
            send_followups()
        except Exception as e:
            log.warning("resolve_outcomes/followups failed: %s", e)
        time.sleep(6 * 3600)

threading.Thread(target=outcome_loop, daemon=True).start()

def store_price(symbol, q):
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO prices
            (symbol,timestamp,open,high,low,close,prev_close,pre_market,post_market,volume)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (symbol,q["timestamp"],q["open"],q["high"],q["low"],
             q["close"],q["prev_close"],q.get("pre_market"),q.get("post_market"),q.get("volume")))
        conn.commit()

def get_latest(symbol):
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM prices WHERE symbol=? ORDER BY timestamp DESC LIMIT 1", (symbol,)
        ).fetchone()
        return dict(row) if row else None

def get_history(symbol, days=90):
    with get_db() as conn:
        cutoff = int(time.time()) - days * 86400
        rows = conn.execute(
            "SELECT timestamp,close,high,low,volume FROM prices WHERE symbol=? AND timestamp>? AND close IS NOT NULL ORDER BY timestamp ASC",
            (symbol, cutoff)
        ).fetchall()
    # One row per calendar date — keep the latest intraday update for each day
    seen = {}
    for r in rows:
        date_key = datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d")
        seen[date_key] = dict(r)
    return list(seen.values())

def _cooldown_seconds():
    return get_setting_int("alert_cooldown_hours", 24) * 3600

def log_alert(symbol, rule_type, message, price, detail=None, should_notify=True):
    now = int(time.time())
    with get_db() as conn:
        recent = conn.execute(
            "SELECT id FROM alerts WHERE symbol=? AND rule_type=? AND timestamp>? LIMIT 1",
            (symbol, rule_type, now - _cooldown_seconds())
        ).fetchone()
        if recent:
            return None
        cur = conn.execute(
            "INSERT INTO alerts (symbol,timestamp,rule_type,message,price,detail,ack) VALUES (?,?,?,?,?,?,0)",
            (symbol, now, rule_type, message, price, detail)
        )
        conn.commit()
        new_id = cur.lastrowid
    # Alert is always written to the DB/UI regardless of notify_strong_only — that setting only
    # gates the outbound notification. run_check passes should_notify=False and sends one
    # grouped brief per signal event instead (notify_signal_brief).
    if should_notify and not is_guest_stock(symbol):
        worker_pool.submit(notify_alert, symbol, rule_type, message, price)
    return new_id

def get_alerts(limit=200):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

def is_in_cooldown(symbol, rule_type):
    now = int(time.time())
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM alerts WHERE symbol=? AND rule_type=? AND timestamp>? LIMIT 1",
            (symbol, rule_type, now - _cooldown_seconds())
        ).fetchone()
        return row is not None

def fetch_news(symbol):
    def _raw():
        news = yf.Ticker(symbol).news or []
        items = []
        for article in news[:8]:
            # yfinance >= 0.2.x wraps content in a nested dict
            content = article.get("content") or article
            if isinstance(content, dict):
                title = content.get("title", "")
                provider = content.get("provider", {})
                pub = provider.get("displayName", "") if isinstance(provider, dict) else str(provider)
                summary = content.get("summary", "")
            else:
                title = article.get("title", "")
                pub = article.get("publisher", "")
                summary = ""
            if title:
                items.append({"title": title, "publisher": pub, "summary": summary})
        return items
    try:
        return yf_cached(f"news:{symbol}", 1800, _raw)
    except Exception as e:
        log.warning("fetch_news(%s) failed: %s", symbol, e)
        return []

def _earnings_hint(symbol):
    with extended_lock:
        ed = extended_state.get(symbol, {}).get("earnings_date")
    if not ed:
        return ""
    try:
        days = (datetime.strptime(ed, "%Y-%m-%d").date() - datetime.now().date()).days
    except Exception:
        return ""
    if days < 0:
        return ""
    when = "today" if days == 0 else ("tomorrow" if days == 1 else f"in {days} days")
    return f" Note: {symbol} is scheduled to report earnings {when} ({ed}) — factor this in if relevant."

def _is_near_earnings(symbol, window_days=2):
    """True if symbol's next known earnings date is within +/- window_days of today —
    used to tag alerts that may be earnings-driven rather than purely technical."""
    with extended_lock:
        ed = extended_state.get(symbol, {}).get("earnings_date")
    if not ed:
        return False
    try:
        days = (datetime.strptime(ed, "%Y-%m-%d").date() - datetime.now().date()).days
    except Exception:
        return False
    return abs(days) <= window_days

def _bear_regime_caveat(signal):
    """Standalone bear-regime caveat sentence for bounce-watch (SELL-side) signals only — the
    backtest's calibration window (see BACKTESTING.md) was bull-heavy, so bounce statistics are
    unreliable evidence in a bear regime. Returns '' outside bear regime or for BUY signals —
    used both in the alert detail blob (run_check) and the synthesize_news() prompt."""
    if signal != "SELL":
        return ""
    try:
        if get_market_regime() != "bear":
            return ""
    except Exception:
        return ""
    return ("Caution: the market is currently in a bear regime, but this rule's bounce-watch "
            "statistics were calibrated on a mostly bull-market window — treat the backtested "
            "edge as less reliable right now.")

def _bear_regime_hint(signal):
    """Same as _bear_regime_caveat but pre-fixed with a leading space for inline sentence-joining
    into the synthesize_news() prompt string."""
    caveat = _bear_regime_caveat(signal)
    return f" {caveat}" if caveat else ""

def synthesize_news(symbol, news_items, move_pct, direction, rule_label, rule_type=None, signal=None):
    key = get_anthropic_key()
    if not key:
        return None
    if not news_items:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key)
        headlines = "\n".join(
            f"- {item['title']}" + (f" ({item['publisher']})" if item.get("publisher") else "")
            for item in news_items[:6]
        )
        dir_word = "up" if direction == "UP" else "down"
        stats_line = _rule_stats_line(symbol, rule_type) if rule_type else ""
        # Bear-regime caveat is keyed off the rule's actual BUY/SELL signal, NOT today's price
        # direction — some rules' SELL (e.g. RSI overbought) doesn't imply today's move was down.
        prompt = (
            f"{symbol} triggered a {rule_label} alert. The stock moved {abs(move_pct):.1f}% {dir_word} today.\n\n"
            f"Recent news headlines:\n{headlines}\n\n"
            f"In 2-3 sentences, explain what may be driving this unusual move based on the news. "
            f"Be concise and specific to {symbol}.{_earnings_hint(symbol)}"
            + (f" Backtested track record for this rule on {symbol}: {stats_line}. Cite this edge "
               f"(or lack of confidence) rather than speaking generically." if stats_line else "")
            + _bear_regime_hint(signal)
        )
        response = client.messages.create(
            model=get_setting("ai_synthesis_model", "claude-sonnet-4-6"),
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.warning("synthesize_news(%s) failed: %s", symbol, e)
        return None

# ─────────────────────────────────────────────────────────────────────────────
# OUTBOUND NOTIFICATIONS
# ─────────────────────────────────────────────────────────────────────────────

def _send_email(subject, body, html=None):
    """Returns (ok, message). Callers that only care about fire-and-forget (real alerts)
    can ignore the return value; api_test_notify uses it to show the user what actually
    happened instead of a blind "sent" — silent failures here were exactly why email
    notifications could be enabled yet never arrive with no way to tell why."""
    if not get_setting_bool("notify_email_enabled"):
        return False, "Email notifications are turned off"
    host = get_setting("smtp_host", ""); user = get_setting("smtp_user", "")
    pwd  = get_setting("smtp_password", ""); to = get_setting("notify_email_to", "") or user
    missing = [n for n, v in (("SMTP host", host), ("SMTP username", user), ("recipient", to)) if not v]
    if missing:
        msg = f"Missing: {', '.join(missing)} — fill these in and Save first"
        log.warning("Email enabled but incomplete: %s", msg)
        return False, msg
    try:
        msg_obj = EmailMessage()
        msg_obj["Subject"] = subject
        msg_obj["From"] = user
        msg_obj["To"] = to
        msg_obj.set_content(body)
        if html:
            msg_obj.add_alternative(html, subtype="html")
        with smtplib.SMTP(host, get_setting_int("smtp_port", 587), timeout=15) as s:
            s.starttls()
            if pwd:
                s.login(user, pwd)
            s.send_message(msg_obj)
        return True, f"Sent to {to}"
    except Exception as e:
        log.warning("Email send failed: %s", e)
        return False, f"{type(e).__name__}: {e}"

def _send_whatsapp(text):
    """Returns (ok, message) — see _send_email docstring."""
    if not get_setting_bool("notify_whatsapp_enabled"):
        return False, "WhatsApp notifications are turned off"
    phone = get_setting("callmebot_phone", ""); key = get_setting("callmebot_apikey", "")
    missing = [n for n, v in (("phone", phone), ("CallMeBot API key", key)) if not v]
    if missing:
        msg = f"Missing: {', '.join(missing)} — fill these in and Save first"
        log.warning("WhatsApp enabled but incomplete: %s", msg)
        return False, msg
    try:
        url = "https://api.callmebot.com/whatsapp.php?" + urllib.parse.urlencode(
            {"phone": phone, "text": text, "apikey": key})
        resp = urllib.request.urlopen(url, timeout=15).read().decode("utf-8", "replace")
        return True, "Sent"
    except Exception as e:
        log.warning("WhatsApp send failed: %s", e)
        return False, f"{type(e).__name__}: {e}"

def build_daily_digest():
    """Summarize the last 24h of alerts (grouped by symbol, STRONG flagged first) into a short
    text block for the once-a-day digest. Returns (subject, body) or None if nothing to report."""
    since = int(time.time()) - 86400
    with get_db() as conn:
        rows = conn.execute(
            "SELECT symbol,rule_type,message,detail,timestamp FROM alerts WHERE timestamp>? "
            "AND symbol NOT IN (SELECT symbol FROM stocks WHERE added_by='guest') ORDER BY symbol,timestamp",
            (since,)).fetchall()
    if not rows:
        return None
    by_sym = {}
    for r in rows:
        try:
            strong = (json.loads(r["detail"]) or {}).get("strong") if r["detail"] else False
        except Exception:
            strong = False
        by_sym.setdefault(r["symbol"], {"strong": False, "items": []})
        by_sym[r["symbol"]]["items"].append(f"  • {r['rule_type']}: {r['message']}")
        if strong:
            by_sym[r["symbol"]]["strong"] = True
    order = sorted(by_sym.items(), key=lambda kv: (not kv[1]["strong"], kv[0]))
    lines = []
    for sym, info in order:
        tag = " ⚡STRONG" if info["strong"] else ""
        lines.append(f"{sym}{tag}")
        lines.extend(info["items"])
        lines.append("")
    n = len(rows)
    subject = f"⚡ Tripwire daily digest — {n} alert{'s' if n != 1 else ''} across {len(by_sym)} stock{'s' if len(by_sym) != 1 else ''}"
    body = "Your last 24 hours of Tripwire alerts:\n\n" + "\n".join(lines) + \
           "\n(Daily digest mode. Turn off in Settings for instant alerts.)"
    return subject, body

def digest_loop():
    """Send one digest per day at the configured local hour, if digest mode is on."""
    last_sent_date = None
    time.sleep(120)
    while True:
        try:
            if get_setting_bool("daily_digest_enabled"):
                now = datetime.now()
                hour = get_setting_int("digest_hour", 8)
                today = now.strftime("%Y-%m-%d")
                if now.hour >= hour and last_sent_date != today:
                    dg = build_daily_digest()
                    last_sent_date = today  # mark regardless so we don't retry all day on empty
                    if dg:
                        subject, body = dg
                        worker_pool.submit(_send_email, subject, body)
                        worker_pool.submit(_send_whatsapp, subject)
        except Exception as e:
            log.warning("digest_loop error: %s", e)
        time.sleep(600)

threading.Thread(target=digest_loop, daemon=True).start()

def notify_alert(symbol, rule_type, message, price):
    label = {"volatility":"Unusual Move","support_resistance":"S/R Breach",
             "consecutive_down":"Consecutive Down","volume":"Volume Spike",
             "gap":"Gap Open","rsi":"RSI Extreme","ma_cross":"MA Crossover"}.get(rule_type, rule_type)
    subject = f"⚡ Tripwire: {symbol} {label}"
    body = f"{symbol} — {label}\n{message}\nPrice: ${price}\n\n(Tripwire alert)"
    _send_email(subject, body)
    _send_whatsapp(f"⚡ {symbol} {label}: {message} (${price})")

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL EVENTS — one per symbol per check cycle: email brief, acted/passed, 5-day follow-up
# ─────────────────────────────────────────────────────────────────────────────
PUBLIC_URL = os.environ.get("TRIPWIRE_PUBLIC_URL", "").strip().rstrip("/")
ACTION_WINDOW_TRADING_DAYS = 4  # mirrors the dashboard's ACTION_WINDOW_TRADING_DAYS
RULE_NAMES = {"volatility": "Unusual daily move", "support_resistance": "Support/resistance break",
              "volume": "Volume spike", "gap": "Opening gap", "rsi": "RSI extreme",
              "ma_cross": "Moving-average crossover", "consecutive_down": "Consecutive down days"}
SIGNAL_MEANING = {
    "STRONG BUY": "Several independent rules agree on upward momentum.",
    "STRONG BOUNCE WATCH": "Several independent rules flag a sharp drop. Historically such drops "
                           "tended to rebound within days: a possible dip-buy setup, not a sell signal.",
    "TRENDING BUY": "One rule points up; not yet confirmed by a second rule.",
    "BOUNCE WATCH": "One rule flags a drop that has historically tended to rebound; not yet confirmed.",
}

def trading_days_after(ts, n):
    d = datetime.fromtimestamp(ts).date()
    while n > 0:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d

def _fmt_day(d):
    return f"{d:%a} {d.day} {d:%b}"

def _detail_of(row):
    try:
        return json.loads(row["detail"]) if row.get("detail") else {}
    except Exception:
        return {}

def _event_label(rows):
    """Ensemble label for an event; older alerts predate the stored label, so infer it."""
    for r in rows:
        ens = _detail_of(r).get("ensemble")
        if ens:
            return ens
    sigs = [_detail_of(r).get("signal") for r in rows]
    strong = any(_detail_of(r).get("strong") for r in rows)
    up = sigs.count("BUY") >= sigs.count("SELL")
    return ("STRONG " if strong else ("TRENDING " if up else "")) + ("BUY" if up else "BOUNCE WATCH")

def _event_rows(event_id):
    with get_db() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM alerts WHERE COALESCE(event_id,id)=? ORDER BY id", (event_id,))]

def _decision_sig(event_id, decision):
    return hmac.new(app.secret_key.encode(), f"decide:{event_id}:{decision}".encode(), "sha256").hexdigest()[:24]

def decision_link(event_id, decision):
    if not PUBLIC_URL:
        return None
    return f"{PUBLIC_URL}/decide/{event_id}/{decision}/{_decision_sig(event_id, decision)}"

def set_decision(ids, decision):
    ids = [int(i) for i in ids][:200]
    if not ids:
        return
    with get_db() as conn:
        conn.executemany("UPDATE alerts SET decision=?, decision_ts=? WHERE id=?",
                         [(decision, int(time.time()) if decision else None, i) for i in ids])
        if decision:   # deciding on a signal also means you've seen its alerts
            conn.executemany("UPDATE alerts SET ack=1 WHERE id=?", [(i,) for i in ids])
        conn.commit()

def _evidence_plain(sym, rule_type):
    st = _rule_stats_for(sym, rule_type)
    if not st:
        return ""
    s = f"In the backtest, this rule on {sym} was followed by {st['exc5']:+.1f}% vs the market over 5 days on average"
    if st.get("hit5") is not None:
        s += f", beating it {st['hit5']:.0f}% of the time"
    s += f" ({st['n']} cases)"
    if st.get("mae") is not None:
        s += f"; typical worst dip along the way {st['mae']:+.1f}%"
    if st.get("short_history") or st["n"] < 30:
        s += " — low confidence, little history"
    return s + "."

def _btn(url, text, color):
    return (f'<a href="{h_esc(url)}" style="display:inline-block;background:{color};color:#fff;text-decoration:none;'
            f'font-weight:700;padding:10px 16px;border-radius:8px;margin:0 8px 8px 0">{h_esc(text)}</a>')

def _email_shell(inner):
    return ('<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:560px;color:#111;'
            f'font-size:15px;line-height:1.5">{inner}<p style="color:#888;font-size:12px;margin-top:22px">'
            'Tripwire · informational only, not financial advice.</p></div>')

def build_signal_brief(event_id):
    rows = _event_rows(event_id)
    if not rows:
        return None
    r0 = rows[0]; d0 = _detail_of(r0); sym = r0["symbol"]
    label = _event_label(rows)
    move = d0.get("move_pct")
    move_txt = f" ({move:+.1f}% today)" if isinstance(move, (int, float)) else ""
    act_by = _fmt_day(trading_days_after(r0["timestamp"], ACTION_WINDOW_TRADING_DAYS))
    with state_lock:
        live = list(state["results"].get(sym, {}).get("rules", []))
    voters = [r for r in live if r.get("triggered") and not r.get("disabled") and _rule_weight(r["rule_type"]) > 0]
    agree = f"{len(voters)} of 4 core rules agree: " + ", ".join(RULE_NAMES.get(r["rule_type"], r["rule_type"]) for r in voters) if voters else ""
    synthesis = next((_detail_of(r).get("news_synthesis") for r in rows if _detail_of(r).get("news_synthesis")), None)
    caveats = []
    if any(_detail_of(r).get("near_earnings") for r in rows):
        caveats.append("Earnings are within 2 days — the move may be earnings-driven, which behaves differently.")
    bear = next((_detail_of(r).get("bear_regime_caveat") for r in rows if _detail_of(r).get("bear_regime_caveat")), None)
    if bear:
        caveats.append(bear)
    fired = [(RULE_NAMES.get(r["rule_type"], r["rule_type"]), r["message"], _evidence_plain(sym, r["rule_type"])) for r in rows]

    subject = f"⚡ {label} · {sym} ${r0['price']}{move_txt} — act by {act_by}"
    acted, passed = decision_link(event_id, "acted"), decision_link(event_id, "passed")
    open_url = f"{PUBLIC_URL}/?stock={sym}" if PUBLIC_URL else None

    t = [f"{sym} — {label}", SIGNAL_MEANING.get(label, ""), ""]
    if agree: t.append(agree)
    t.append(f"Price ${r0['price']}{move_txt}. Act by {act_by} — the signal is calibrated for 1–5 trading days.")
    t += ["", "What fired:"]
    for name, msg, ev in fired:
        t.append(f"• {name}: {msg}")
        if ev: t.append(f"  {ev}")
    if synthesis: t += ["", "News: " + synthesis]
    for c in caveats: t += ["", "⚠ " + c]
    if acted: t += ["", f"I acted: {acted}", f"I passed: {passed}", f"Open in Tripwire: {open_url}"]
    else: t += ["", "Mark whether you acted in the Tripwire dashboard."]
    text = "\n".join(t)

    h = [f'<h2 style="margin:0 0 4px">{h_esc(sym)} — {h_esc(label)}</h2>',
         f'<p style="margin:0 0 10px;color:#444">{h_esc(SIGNAL_MEANING.get(label, ""))}</p>']
    if agree: h.append(f'<p style="margin:0 0 6px"><b>{h_esc(agree)}</b></p>')
    h.append(f'<p style="margin:0 0 14px">Price ${h_esc(str(r0["price"]))}{h_esc(move_txt)} · '
             f'<b style="color:#B45309">act by {h_esc(act_by)}</b> <span style="color:#666">(calibrated for 1–5 trading days)</span></p>')
    h.append('<p style="margin:0 0 4px"><b>What fired</b></p><ul style="margin:0 0 12px;padding-left:20px">')
    for name, msg, ev in fired:
        h.append(f'<li><b>{h_esc(name)}</b>: {h_esc(msg)}' + (f'<br><span style="color:#555;font-size:13px">{h_esc(ev)}</span>' if ev else '') + '</li>')
    h.append('</ul>')
    if synthesis: h.append(f'<p style="background:#F3F4F6;border-radius:8px;padding:10px 12px"><b>News:</b> {h_esc(synthesis)}</p>')
    for c in caveats: h.append(f'<p style="color:#B45309">⚠ {h_esc(c)}</p>')
    if acted:
        h.append('<p style="margin:16px 0 4px">' + _btn(acted, "I acted", "#059669") + _btn(passed, "I passed", "#6B7280")
                 + _btn(open_url, f"Open {sym}", "#2563EB") + '</p>')
    else:
        h.append('<p style="color:#666">Mark whether you acted in the Tripwire dashboard.</p>')
    html = _email_shell("".join(h))
    wa = f"⚡ {sym} {label}{move_txt} — act by {act_by}." + (f" {open_url}" if open_url else "")
    return {"subject": subject, "text": text, "html": html, "whatsapp": wa}

def notify_signal_brief(event_id):
    try:
        b = build_signal_brief(event_id)
        if not b:
            return
        _send_email(b["subject"], b["text"], b["html"])
        _send_whatsapp(b["whatsapp"])
    except Exception as e:
        log.warning("notify_signal_brief(%s) failed: %s", event_id, e)

def _event_outcome(rows):
    """(stock %, market %, stock-minus-market pts) once every alert in the event is scored."""
    if any(r.get("outcome_ts") is None for r in rows):
        return None
    for r in rows:
        if r.get("outcome_excess") is not None and r.get("outcome_ret") is not None:
            ex = r["outcome_excess"]  # SIGNAL_DIR is +1 for every signal: excess = stock - market
            return r["outcome_ret"], round(r["outcome_ret"] - ex, 2), ex
    return None

def send_followups():
    """One batched email with how recently-notified STRONG signals played out ~5 trading days
    later, including whether the owner acted — the loop the brief opens is closed here."""
    since = int(time.time()) - 30 * 86400
    with get_db() as conn:
        eids = [r[0] for r in conn.execute(
            "SELECT DISTINCT event_id FROM alerts WHERE event_id IS NOT NULL AND followup_sent IS NULL "
            "AND outcome_ts IS NOT NULL AND timestamp > ?", (since,))]
    items, done = [], []
    for eid in eids:
        rows = _event_rows(eid)
        out = _event_outcome(rows)
        if out is None:
            continue
        done.append(eid)
        if not any(_detail_of(r).get("strong") for r in rows) or is_guest_stock(rows[0]["symbol"]):
            continue
        items.append((eid, rows, out))
    if done:
        with get_db() as conn:
            conn.executemany("UPDATE alerts SET followup_sent=1 WHERE COALESCE(event_id,id)=?", [(e,) for e in done])
            conn.commit()
    if not items:
        return 0
    beat = sum(1 for _, _, (_, _, ex) in items if ex > 0)
    subject = f"📬 How your signals played out — {beat} of {len(items)} beat the market"
    t, h = ["5 trading days later:", ""], ['<h2 style="margin:0 0 10px">How your signals played out</h2>',
                                          '<p style="color:#555;margin:0 0 14px">5 trading days after each signal:</p>']
    for eid, rows, (ret, mkt, ex) in items:
        r0 = rows[0]; label = _event_label(rows)
        day = _fmt_day(datetime.fromtimestamp(r0["timestamp"]).date())
        verdict = f"✓ beat the market by {ex:.1f} pts" if ex > 0 else f"✗ lagged the market by {abs(ex):.1f} pts"
        dec = r0.get("decision")
        you = {"acted": "You acted.", "passed": "You passed."}.get(dec, "Not marked yet.")
        t.append(f"{day} · {r0['symbol']} {label}: stock {ret:+.1f}%, market {mkt:+.1f}% → {verdict}. {you}")
        links = ""
        if not dec and decision_link(eid, "acted"):
            t.append(f"   I acted: {decision_link(eid, 'acted')}   I passed: {decision_link(eid, 'passed')}")
            links = ('<br><a href="' + h_esc(decision_link(eid, "acted")) + '">I acted</a> · <a href="'
                     + h_esc(decision_link(eid, "passed")) + '">I passed</a>')
        color = "#059669" if ex > 0 else "#DC2626"
        h.append(f'<p style="margin:0 0 12px"><b>{h_esc(r0["symbol"])} {h_esc(label)}</b> <span style="color:#666">({h_esc(day)})</span><br>'
                 f'stock {ret:+.1f}%, market {mkt:+.1f}% → <b style="color:{color}">{h_esc(verdict)}</b>. '
                 f'<span style="color:#555">{h_esc(you)}</span>{links}</p>')
    t += ["", "BOUNCE WATCH counts as right when the stock rebounds ahead of the market.",
          "Full record: Tripwire → Performance."]
    h.append('<p style="color:#666;font-size:13px">BOUNCE WATCH counts as right when the stock rebounds ahead of the market. '
             'Full record: Tripwire → Performance.</p>')
    _send_email(subject, "\n".join(t), _email_shell("".join(h)))
    return len(items)

# ─────────────────────────────────────────────────────────────────────────────
# RULE EVALUATION
# ─────────────────────────────────────────────────────────────────────────────

def _rsi(closes, period=14):
    """Wilder's RSI over a list of closes. Returns None if not enough data."""
    if len(closes) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i-1]
        if d >= 0: gains += d
        else:      losses -= d
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i-1]
        gain = max(d, 0.0); loss = max(-d, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)

def evaluate_rules(symbol, quote, history, params):
    results = []
    close, prev = quote["close"], quote["prev_close"]
    vol_disabled = not params.get("enable_volatility", True)

    # Rule 1: Volatility
    r1 = {"rule_type":"volatility","label":"Unusual Daily Move"}
    if vol_disabled:
        r1.update({"triggered":False,"disabled":True,"message":"Rule disabled","actual_value":None,"threshold":None})
    elif prev and prev > 0:
        daily_pct = abs((close - prev) / prev) * 100
        closes = [h["close"] for h in history[-params["volatility_lookback"]:]]
        if len(closes) >= 2:
            changes = [abs((closes[i]-closes[i-1])/closes[i-1])*100 for i in range(1,len(closes))]
            avg_vol   = round(statistics.mean(changes), 2)
            threshold = round(avg_vol * params["volatility_multiplier"], 2)
            triggered = daily_pct > threshold
            direction = "UP" if close > prev else "DOWN"
            signal = ("BUY" if direction == "UP" else "SELL") if triggered else "NEUTRAL"
            r1.update({
                "description": f"Fires when today's move exceeds {params['volatility_multiplier']}× the {params['volatility_lookback']}-day average daily move.",
                "rationale": "Large single-day moves relative to a stock's own history often precede or follow major catalysts — earnings, institutional repositioning, or macro events. When a stock moves far beyond its typical daily range, the cause warrants investigation before acting.",
                "param_summary": f"Multiplier: {params['volatility_multiplier']}×  |  Lookback: {params['volatility_lookback']} days",
                "actual_value": round(daily_pct,2), "threshold": threshold,
                "avg_daily_vol": avg_vol, "triggered": triggered,
                "direction": direction, "signal": signal,
                "message": f"{'▲' if close>prev else '▼'} {daily_pct:.2f}% vs threshold {threshold:.2f}% — {'ALERT' if triggered else 'OK'}",
            })
        else:
            r1.update({"triggered":False,"message":"Insufficient history","actual_value":None,"threshold":None})
    else:
        r1.update({"triggered":False,"message":"No previous close","actual_value":None,"threshold":None})
    results.append(r1)

    # Rule 2: Support / Resistance
    r2 = {"rule_type":"support_resistance","label":"Support / Resistance Breach"}
    sr_pct = params["support_resist_pct"] / 100
    # Range is the PRIOR N closes, excluding today — otherwise today's close is part of
    # its own min/max and the buffered band can never be broken (the rule would never fire).
    _lb = params["support_resist_lookback"]
    closes = [h["close"] for h in history[-(_lb+1):-1]]
    if not params.get("enable_support_resistance", True):
        r2.update({"triggered":False,"disabled":True,"message":"Rule disabled","actual_value":None})
    elif len(closes) >= 5:
        lo, hi = min(closes), max(closes)
        support    = round(lo * (1 - sr_pct), 2)
        resistance = round(hi * (1 + sr_pct), 2)
        below = close < support
        above = close > resistance
        triggered = below or above
        signal2 = ("SELL" if below else "BUY" if above else "NEUTRAL") if triggered else "NEUTRAL"
        r2.update({
            "description": f"Fires when price breaks {params['support_resist_pct']}% beyond the {params['support_resist_lookback']}-day high or low.",
            "rationale": "Price levels where a stock has repeatedly found buying (support) or selling (resistance) act as psychological anchors. A decisive break through these zones signals a potential regime shift — either a breakdown or a breakout — and often leads to accelerated price movement in the breakout direction.",
            "param_summary": f"Buffer: {params['support_resist_pct']}%  |  Lookback: {params['support_resist_lookback']} days",
            "support": support, "resistance": resistance,
            "period_low": round(lo,2), "period_high": round(hi,2),
            "actual_value": close, "triggered": triggered, "signal": signal2,
            "message": (f"${close} BELOW support ${support}" if below else
                        f"${close} ABOVE resistance ${resistance}" if above else
                        f"${close} within range ${support}–${resistance}"),
        })
    else:
        r2.update({"triggered":False,"message":"Insufficient history","actual_value":None})
    results.append(r2)

    # Rule 3: Consecutive down days
    r3 = {"rule_type":"consecutive_down","label":"Consecutive Down Days"}
    if params.get("use_consecutive"):
        closes = [h["close"] for h in history[-12:]]
        if len(closes) >= 4:
            cur_run = max_downs = 0
            for i in range(1, len(closes)):
                if closes[i] < closes[i-1]:
                    cur_run += 1
                    max_downs = max(max_downs, cur_run)
                else:
                    cur_run = 0
            # cur_run > 0 means the streak is still active today
            streak_active = cur_run >= params["consecutive_down_days"]
            streak_resolved = (max_downs >= params["consecutive_down_days"]) and cur_run == 0
            threshold = params["consecutive_down_days"]
            # Only the resolved (bounce) case is an actionable alert. A 5-year backtest found
            # no reliable short-term edge in treating an active losing streak as a sell signal
            # on this watchlist (mean 5-day signal-direction excess return was negative) — an
            # active streak is shown for context only, never alerted.
            triggered = streak_resolved
            if streak_active:
                rationale = f"The stock has closed lower for {cur_run} consecutive days. Backtesting found no reliable short-term edge in treating an active losing streak as a sell signal on this watchlist — shown for context only, not alerted. Watch for either a bounce or continuation."
                description = f"Active streak: {cur_run} consecutive down days (context only — not an actionable alert)."
            elif streak_resolved:
                rationale = f"The stock had a streak of {max_downs} consecutive down days but has since reversed upward. The rule fired on the streak; today's move up may be the technical bounce that streak often precedes. Monitor whether the recovery holds."
                description = f"Streak of {max_downs} consecutive down days detected recently; stock has since bounced."
            else:
                rationale = "A losing streak can precede a technical bounce. This rule only alerts once the streak resolves upward — backtesting found an active streak alone has no reliable predictive edge at this horizon."
                description = f"Fires after a {threshold}+ day losing streak resolves with an up day."
            signal3 = "BUY" if streak_resolved else "NEUTRAL"
            r3.update({
                "description": description,
                "rationale": rationale,
                "param_summary": f"Min consecutive days: {threshold}",
                "actual_value": cur_run if streak_active else max_downs,
                "current_run": cur_run, "max_run": max_downs,
                "streak_active": streak_active, "streak_resolved": streak_resolved,
                "threshold": threshold, "triggered": triggered, "signal": signal3,
                "message": (
                    f"Streak of {max_downs} down days detected; stock has since bounced — ALERT" if streak_resolved else
                    f"Active streak: {cur_run} consecutive down days — context only, no alert" if streak_active else
                    f"Max streak {max_downs} days — OK (threshold {threshold})"
                ),
            })
        else:
            r3.update({"triggered":False,"message":"Insufficient history","actual_value":None,"threshold":params["consecutive_down_days"]})
    else:
        r3.update({"triggered":False,"disabled":True,"message":"Disabled for this category","actual_value":None,"threshold":params["consecutive_down_days"]})
    results.append(r3)

    # Rule 4: Volume spike
    r4 = {"rule_type":"volume","label":"Volume Spike"}
    vols = [h.get("volume") for h in history[-int(params.get("volume_lookback",20)):] if h.get("volume")]
    today_vol = quote.get("volume")
    if not params.get("enable_volume", True):
        r4.update({"triggered":False,"disabled":True,"message":"Rule disabled","actual_value":None})
    elif today_vol and len(vols) >= 3:
        # Exclude today's own bar from the baseline if present
        base = vols[:-1] if len(vols) > 3 else vols
        avg_vol = statistics.mean(base)
        mult = params.get("volume_multiplier", 3.0)
        ratio = round(today_vol / avg_vol, 2) if avg_vol else 0
        threshold = round(mult, 2)
        triggered = ratio > mult
        direction = "UP" if close >= prev else "DOWN"
        signal = ("BUY" if direction == "UP" else "SELL") if triggered else "NEUTRAL"
        r4.update({
            "description": f"Fires when today's volume exceeds {mult}× the {int(params.get('volume_lookback',20))}-day average volume.",
            "rationale": "A sudden surge in trading volume signals unusual institutional interest or a reaction to news. Volume confirms conviction behind a price move — a breakout on heavy volume is far more significant than one on thin trading.",
            "param_summary": f"Multiplier: {mult}×  |  Lookback: {int(params.get('volume_lookback',20))} days",
            "actual_value": ratio, "threshold": threshold,
            "direction": direction, "signal": signal, "triggered": triggered,
            "message": f"Volume {ratio}× average — {'ALERT' if triggered else 'OK'} (threshold {threshold}×)",
        })
    else:
        r4.update({"triggered":False,"message":"Insufficient volume data","actual_value":None,"threshold":round(params.get('volume_multiplier',3.0),2)})
    results.append(r4)

    # Rule 5: Gap at open
    r5 = {"rule_type":"gap","label":"Opening Gap"}
    if not params.get("enable_gap", True):
        r5.update({"triggered":False,"disabled":True,"message":"Rule disabled","actual_value":None})
    elif prev and prev > 0 and quote.get("open"):
        gap_pct = round((quote["open"] - prev) / prev * 100, 2)
        threshold = params.get("gap_pct", 3.0)
        triggered = abs(gap_pct) > threshold
        direction = "UP" if gap_pct >= 0 else "DOWN"
        signal = ("BUY" if direction == "UP" else "SELL") if triggered else "NEUTRAL"
        r5.update({
            "description": f"Fires when the stock opens more than {threshold}% away from the prior close.",
            "rationale": "Opening gaps reflect information that arrived overnight — earnings, guidance, or macro news — repriced before regular trading. Large gaps often set the day's tone and can either continue or fade, making them key decision points.",
            "param_summary": f"Gap threshold: {threshold}%",
            "actual_value": gap_pct, "threshold": threshold,
            "direction": direction, "signal": signal, "triggered": triggered,
            "message": f"{'▲' if gap_pct>=0 else '▼'} Gap {gap_pct:+.2f}% at open — {'ALERT' if triggered else 'OK'} (threshold ±{threshold}%)",
        })
    else:
        r5.update({"triggered":False,"message":"No open/prev-close data","actual_value":None,"threshold":params.get('gap_pct',3.0)})
    results.append(r5)

    # Rule 6: RSI(14)
    r6 = {"rule_type":"rsi","label":"RSI Overbought / Oversold"}
    period = int(params.get("rsi_period", 14))
    ob = params.get("rsi_overbought", 70); os_ = params.get("rsi_oversold", 30)
    rsi_closes = [h["close"] for h in history[-(period*3):]]
    rsi_val = _rsi(rsi_closes, period) if not (not params.get("enable_rsi", True)) else None
    if not params.get("enable_rsi", True):
        r6.update({"triggered":False,"disabled":True,"message":"Rule disabled","actual_value":None})
    elif rsi_val is not None:
        over = rsi_val >= ob; under = rsi_val <= os_
        triggered = over or under
        signal = ("SELL" if over else "BUY" if under else "NEUTRAL") if triggered else "NEUTRAL"
        r6.update({
            "description": f"Fires when {period}-day RSI reaches overbought (≥{ob}) or oversold (≤{os_}).",
            "rationale": "RSI measures the speed and magnitude of recent price changes. Readings above 70 suggest a stock is overbought and may be due for a pullback; below 30 suggests oversold and a potential bounce. Extremes flag momentum exhaustion.",
            "param_summary": f"Period: {period}  |  OB ≥ {ob}  |  OS ≤ {os_}",
            "actual_value": rsi_val, "threshold": ob if over else os_,
            "signal": signal, "triggered": triggered,
            "message": (f"RSI {rsi_val} — OVERBOUGHT (≥{ob})" if over else
                        f"RSI {rsi_val} — OVERSOLD (≤{os_})" if under else
                        f"RSI {rsi_val} — neutral ({os_}–{ob})"),
        })
    else:
        r6.update({"triggered":False,"message":"Insufficient history for RSI","actual_value":None,"threshold":ob})
    results.append(r6)

    # Rule 7: Moving-average crossover
    r7 = {"rule_type":"ma_cross","label":"MA Crossover"}
    ms = int(params.get("ma_short", 20)); ml = int(params.get("ma_long", 50))
    look = int(params.get("ma_cross_lookback", 3))
    ma_closes = [h["close"] for h in history]
    if not params.get("enable_ma", True):
        r7.update({"triggered":False,"disabled":True,"message":"Rule disabled","actual_value":None})
    elif len(ma_closes) >= ml + look:
        def sma(seq, n, end):  # SMA of n closes ending at index `end` (inclusive)
            window = seq[end-n+1:end+1]
            return sum(window)/n if len(window) == n else None
        last = len(ma_closes) - 1
        cur_s, cur_l = sma(ma_closes, ms, last), sma(ma_closes, ml, last)
        crossed = None
        for back in range(1, look+1):
            e0, e1 = last-back, last-back+1
            s0, l0 = sma(ma_closes, ms, e0), sma(ma_closes, ml, e0)
            s1, l1 = sma(ma_closes, ms, e1), sma(ma_closes, ml, e1)
            if None in (s0,l0,s1,l1): continue
            if s0 <= l0 and s1 > l1: crossed = "golden"; break
            if s0 >= l0 and s1 < l1: crossed = "death"; break
        triggered = crossed is not None
        signal = ("BUY" if crossed == "golden" else "SELL" if crossed == "death" else "NEUTRAL")
        r7.update({
            "description": f"Fires when the {ms}-day SMA crosses the {ml}-day SMA within the last {look} days.",
            "rationale": "A moving-average crossover is a classic trend-shift signal. A 'golden cross' (short MA rising above long MA) marks emerging upward momentum; a 'death cross' (short falling below long) marks downward momentum. These confirm regime changes rather than one-day noise.",
            "param_summary": f"Short: {ms}d  |  Long: {ml}d  |  Window: {look}d",
            "actual_value": round(cur_s,2) if cur_s else None,
            "ma_short_val": round(cur_s,2) if cur_s else None,
            "ma_long_val": round(cur_l,2) if cur_l else None,
            "signal": signal, "triggered": triggered,
            "message": (f"Golden cross: {ms}d rose above {ml}d SMA — ALERT" if crossed=="golden" else
                        f"Death cross: {ms}d fell below {ml}d SMA — ALERT" if crossed=="death" else
                        f"No recent {ms}/{ml} crossover — OK"),
        })
    else:
        r7.update({"triggered":False,"message":"Insufficient history for MA cross","actual_value":None})
    results.append(r7)

    return results

# Enable-flag keys carried over from a symbol's live params into the info-tier pass, so a rule
# the user explicitly disabled doesn't reappear as "activity" — informational only, never a
# signal, but still respects the user's own on/off choice for that rule.
_RULE_ENABLE_KEYS = ("enable_volatility", "enable_support_resistance", "use_consecutive",
                     "enable_volume", "enable_gap", "enable_rsi", "enable_ma")

def _build_info_params(params, category):
    """INFO_RULES params for this category, with the symbol's own enable/disable toggles
    carried over (informational tier respects explicit user disablement)."""
    info = dict(INFO_RULES.get(category, INFO_RULES["high_vol"]))
    for k in _RULE_ENABLE_KEYS:
        if k in params:
            info[k] = params[k]
    return info

def compute_info_events(symbol, quote, history, params, category, signal_rules):
    """Second, pure in-memory pass using the looser pre-tuning (v3-era) INFO_RULES thresholds
    for day-to-day awareness, without diluting the validated signal tier. Reuses evaluate_rules'
    exact math (no separate rule implementation, no extra network calls — same quote/history
    already fetched for the signal pass). A rule that crosses the info threshold but NOT the
    signal threshold becomes a transient, non-persisted 'activity' event — never a BUY/SELL
    claim, never written to the alerts table, never sent to notify_alert."""
    if not get_setting_bool("show_info_tier"):
        return []
    info_params = _build_info_params(params, category)
    info_rules = evaluate_rules(symbol, quote, history, info_params)
    signal_by_type = {r["rule_type"]: r for r in signal_rules}
    events = []
    for ir in info_rules:
        if not ir.get("triggered") or ir.get("disabled"):
            continue
        sr = signal_by_type.get(ir["rule_type"])
        if sr and sr.get("triggered") and not sr.get("disabled"):
            continue  # already a real, validated signal — don't also show it as "info"
        events.append({
            "rule_type": ir["rule_type"],
            "label": ir.get("label", ir["rule_type"]),
            "message": f"activity: {ir.get('message', '')} (info · below signal threshold)",
        })
    return events

# Server-side mirror of the frontend's computeSignal() (see RULE_SIGNAL_WEIGHT / computeSignal
# in the dashboard JS) — MUST stay in lock-step with that function. Only used to decide whether
# a check cycle's ensemble is STRONG enough to warrant an outbound notification when
# notify_strong_only is on; the DB/UI always log every individual rule trigger regardless.
# Vote membership is backtest-derived (BACKTESTING.md "weight-check"): rules with near-zero
# solo edge (consecutive_down, gap, ma_cross) are excluded from the ensemble VOTE — their
# individual alerts still fire and log — because removing noise voters roughly doubled the
# quality of STRONG events (+0.93%→+1.89% 5d excess, 55%→61% hit), whereas up-weighting the
# strongest rules was also tested and made things worse.
RULE_SIGNAL_WEIGHT = {"consecutive_down": 0, "gap": 0, "ma_cross": 0}

# Rules shown on the card for context but which never generate a logged alert, notification,
# or news-synthesis call. consecutive_down had near-zero backtested edge yet was the single
# largest alert generator (~1/4 of all alerts) — demoted to context to cut noise + API spend.
CONTEXT_ONLY_RULES = {"consecutive_down"}

def _rule_weight(rule_type):
    return RULE_SIGNAL_WEIGHT.get(rule_type, 1)

def compute_signal(rules):
    """Mirrors the frontend's computeSignal(rules) exactly. Returns a label string
    ('STRONG BUY' | 'STRONG BOUNCE WATCH' | 'TRENDING BUY' | 'BOUNCE WATCH' | 'PENDING')
    or None if no weighted rule triggered."""
    triggered = [r for r in (rules or [])
                 if r.get("triggered") and not r.get("disabled") and _rule_weight(r["rule_type"]) > 0]
    if not triggered:
        return None
    buys  = sum(_rule_weight(r["rule_type"]) for r in triggered if r.get("signal") == "BUY")
    sells = sum(_rule_weight(r["rule_type"]) for r in triggered if r.get("signal") == "SELL")
    total = buys + sells
    if not total:
        return "PENDING"
    if total >= 2 and buys / total >= 2 / 3:
        return "STRONG BUY"
    if total >= 2 and sells / total >= 2 / 3:
        return "STRONG BOUNCE WATCH"
    if buys > sells:
        return "TRENDING BUY"
    if sells > buys:
        return "BOUNCE WATCH"
    return "PENDING"

STRONG_SIGNALS = {"STRONG BUY", "STRONG BOUNCE WATCH"}

# ─────────────────────────────────────────────────────────────────────────────
# MONITOR LOOP
# ─────────────────────────────────────────────────────────────────────────────

state = {"last_check": None, "results": {}, "checking": False}

def run_check(symbols=None):
    with state_lock:
        state["checking"] = True
    stocks = get_stocks()
    if symbols:
        stocks = [s for s in stocks if s["symbol"] in symbols]
    notify_strong_only = get_setting_bool("notify_strong_only")
    digest_mode = get_setting_bool("daily_digest_enabled")  # digest replaces instant pushes
    for s in stocks:
        sym, cat = s["symbol"], s["category"]
        try:
            quote   = fetch_quote(sym)
            store_price(sym, quote)
            history = get_history(sym, days=400)
            params  = get_params(sym, cat)
            rules   = evaluate_rules(sym, quote, history, params)
            move_pct = ((quote["close"] - quote["prev_close"]) / quote["prev_close"] * 100) if quote.get("prev_close") else 0
            # One ensemble read per symbol per check cycle — every rule-trigger alert logged
            # below for this symbol/cycle shares the same STRONG-only notification gate.
            ensemble_signal = compute_signal(rules)
            symbol_is_strong = ensemble_signal in STRONG_SIGNALS
            # The rules that voted, stored on every alert of this cycle: a rule still in cooldown
            # isn't re-logged, so the event's own alerts don't always show everything that agreed.
            votes = [{"rule_type": r["rule_type"], "signal": r.get("signal")} for r in rules
                     if r.get("triggered") and not r.get("disabled") and _rule_weight(r["rule_type"]) > 0]
            near_earnings = _is_near_earnings(sym)
            # Context-only rules are shown on the card but never alert/notify/synthesize; rules
            # still in cooldown were already logged recently.
            to_log = [r for r in rules
                      if r.get("rule_type") not in CONTEXT_ONLY_RULES
                      and r.get("triggered") and not r.get("disabled")
                      and not is_in_cooldown(sym, r["rule_type"])]
            # One news synthesis per symbol per cycle, shared by every rule that fired with it
            # (previously one paid AI call per rule, with near-identical content).
            synthesis = None
            if to_log and not is_guest_stock(sym):
                first = to_log[0]
                direction = first.get("direction") or ("UP" if move_pct >= 0 else "DOWN")
                synthesis = synthesize_news(sym, fetch_news(sym), move_pct, direction,
                                            " + ".join(r.get("label", r["rule_type"]) for r in to_log),
                                            first["rule_type"], first.get("signal"))
            new_ids = []
            for rule in to_log:
                if synthesis:
                    rule["news_synthesis"] = synthesis
                detail = json.dumps({
                    "actual_value":   rule.get("actual_value"),
                    "threshold":      rule.get("threshold"),
                    "direction":      rule.get("direction"),
                    "signal":         rule.get("signal"),
                    "support":        rule.get("support"),
                    "resistance":     rule.get("resistance"),
                    "avg_daily_vol":  rule.get("avg_daily_vol"),
                    "period_low":     rule.get("period_low"),
                    "period_high":    rule.get("period_high"),
                    "description":    rule.get("description"),
                    "rationale":      rule.get("rationale"),
                    "news_synthesis": synthesis,
                    "near_earnings":  near_earnings,
                    "strong":         symbol_is_strong,
                    "ensemble":       ensemble_signal,
                    "votes":          votes,
                    "move_pct":       round(move_pct, 2),
                    "bear_regime_caveat": _bear_regime_caveat(rule.get("signal")) or None,
                })
                aid = log_alert(sym, rule["rule_type"], rule["message"], quote["close"], detail, should_notify=False)
                if aid:
                    new_ids.append(aid)
            if new_ids:
                event_id = min(new_ids)
                with get_db() as conn:
                    conn.executemany("UPDATE alerts SET event_id=? WHERE id=?", [(event_id, i) for i in new_ids])
                    conn.commit()
                should_notify = False if digest_mode else (symbol_is_strong if notify_strong_only else True)
                if should_notify and not is_guest_stock(sym):
                    worker_pool.submit(notify_signal_brief, event_id)
            info_events = compute_info_events(sym, quote, history, params, cat, rules)
            hist30 = get_history(sym, days=30)
            result = {
                "quote": quote, "rules": rules, "params": params, "error": None,
                "week52_high":   quote.get("week52_high"),
                "week52_low":    quote.get("week52_low"),
                "history_closes": [h["close"] for h in hist30],
                "info_events": info_events,
            }
        except Exception as e:
            log.warning("run_check(%s) failed: %s", sym, e)
            result = {"error": str(e), "rules": [], "params": {}, "history_closes": [], "info_events": []}
        with state_lock:
            state["results"][sym] = result
    with state_lock:
        state["last_check"] = datetime.now().isoformat()
        state["checking"] = False

def monitor_loop():
    run_check()
    while True:
        phase = market_phase()
        if phase == "closed":
            if get_setting_bool("market_hours_only"):
                time.sleep(min(get_setting_int("check_interval_closed_seconds", 900), 300))
                continue
            interval = get_setting_int("check_interval_closed_seconds", 900)
        else:
            interval = get_setting_int("check_interval_seconds", 60)
        time.sleep(max(10, interval))
        run_check()

threading.Thread(target=monitor_loop, daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────────────────────

LOGIN_PAGE = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tripwire — Login</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0A0C12;color:#E4E0D8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  min-height:100vh;display:flex;align-items:center;justify-content:center}
.box{background:#12151F;border:1px solid #1E2235;border-radius:14px;padding:32px;width:300px;max-width:90vw}
.logo{font-weight:800;font-size:20px;color:#F59E0B;margin-bottom:18px;text-align:center}
input{width:100%;background:#0A0C12;border:1px solid #1E2235;color:#E4E0D8;border-radius:8px;
  padding:12px 14px;font-size:15px;margin-bottom:12px}
button{width:100%;background:#F59E0B;color:#000;border:none;border-radius:8px;padding:12px;
  font-weight:700;font-size:14px;cursor:pointer}
.err{color:#EF4444;font-size:13px;margin-bottom:10px;text-align:center}
</style></head><body>
<form class="box" method="POST">
  <div class="logo">⚡ Tripwire</div>
  __ERROR_HTML__
  <input type="password" name="password" placeholder="Password" autofocus>
  <button type="submit">Sign in</button>
</form>
</body></html>"""

GUEST_PASSWORD = os.environ.get("TRIPWIRE_GUEST_PASSWORD", "").strip()
GUEST_EXPIRES = os.environ.get("TRIPWIRE_GUEST_EXPIRES", "").strip()  # YYYY-MM-DD, last valid day
# POSTs a guest may make. Add/remove and the AI chat enforce their own guest limits in-handler.
GUEST_ALLOWED_POSTS = {"/login", "/api/check", "/api/stocks/add", "/api/stocks/remove",
                       "/api/ai/chat", "/api/ai/clear", "/api/track"}
GUEST_BLOCKED_MSG = "View-only guest access — nothing was changed."
GUEST_MAX_TICKERS = 10
GUEST_AI_PER_LOGIN = 5
GUEST_AI_DAILY_CAP = 50  # across all guests — bounds API cost even if someone logs in repeatedly

def guest_expired():
    if not GUEST_EXPIRES:
        return False
    try:
        return datetime.now().date() > datetime.strptime(GUEST_EXPIRES, "%Y-%m-%d").date()
    except ValueError:
        log.warning("Invalid TRIPWIRE_GUEST_EXPIRES '%s' — guest access disabled", GUEST_EXPIRES)
        return True

def is_guest():
    return session.get("role") == "guest"

# ── Access log: logins (both passwords + failures) and what guests do, with timestamps ──
def client_ip():
    # Behind Caddy every request arrives from localhost; Caddy appends the real client address
    # as the last X-Forwarded-For entry. Only trust the header when the peer is the local proxy.
    xff = request.headers.get("X-Forwarded-For", "")
    if request.remote_addr in ("127.0.0.1", "::1") and xff:
        return xff.split(",")[-1].strip()
    return request.remote_addr or "?"

def device_label(ua):
    ua = ua or ""
    os_ = next((n for k, n in (("iPhone", "iPhone"), ("iPad", "iPad"), ("Android", "Android"),
                               ("Windows", "Windows"), ("Macintosh", "Mac"), ("CrOS", "ChromeOS"),
                               ("Linux", "Linux")) if k in ua), "Unknown device")
    br = next((n for k, n in (("Edg/", "Edge"), ("OPR/", "Opera"), ("SamsungBrowser", "Samsung Internet"),
                              ("Firefox/", "Firefox"), ("CriOS", "Chrome"), ("Chrome/", "Chrome"),
                              ("Safari/", "Safari"), ("curl/", "curl")) if k in ua), "unknown browser")
    return f"{os_} · {br}"

_event_counts = {}
_event_last = {}
MAX_EVENTS_PER_VISIT = 1000

def record_access(kind, role, detail=""):
    vid = session.get("vid", "")
    if kind == "event":
        if _event_counts.get(vid, 0) >= MAX_EVENTS_PER_VISIT:
            return
        now = time.time()
        last = _event_last.get(vid)
        if last and last[0] == detail and now - last[1] < 3:
            return
        _event_last[vid] = (detail, now)
        _event_counts[vid] = _event_counts.get(vid, 0) + 1
    try:
        with get_db() as conn:
            conn.execute("INSERT INTO access_log (ts,kind,role,vid,ip,device,detail) VALUES (?,?,?,?,?,?,?)",
                         (int(time.time()), kind, role, vid, client_ip(),
                          device_label(request.headers.get("User-Agent")), (detail or "")[:300]))
            conn.commit()
    except Exception as e:
        log.warning("record_access failed: %s", e)

def _login_email(role, ip, device):
    """Guest logins always email the owner; main-password logins only from an IP+device
    combination never seen before, so the owner's own routine logins don't flood the inbox."""
    with get_db() as conn:
        if role == "owner":
            seen = conn.execute("SELECT COUNT(*) FROM access_log WHERE kind='login' AND role='owner' AND ip=? AND device=?",
                                (ip, device)).fetchone()[0]
            if seen > 1:
                return
        n, ips = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ip) FROM access_log WHERE kind='login' AND role=?",
                              (role,)).fetchone()
    who = "Guest password" if role == "guest" else "MAIN password (new device/address)"
    when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = f"🔑 Tripwire login — {'guest' if role == 'guest' else 'main password, new device'} · {device}"
    body = (f"{who} login\n\nTime:    {when}\nAddress: {ip}\nDevice:  {device}\n\n"
            f"{'Guest' if role == 'guest' else 'Main-password'} logins so far: {n} from {ips} different address{'es' if ips != 1 else ''}.\n\n"
            + ("If this wasn't you, change TRIPWIRE_PASSWORD on the server.\n\n" if role == "owner" else "")
            + "Full details: Settings → Access log.")
    worker_pool.submit(_send_email, subject, body)

GUEST_ACTION_NAMES = [
    ("/api/check", "Pressed Check Now"),
    ("/api/stocks/add", "Add ticker"),
    ("/api/stocks/remove", "Remove ticker"),
    ("/api/ai/clear", "Started a new AI conversation"),
    ("/api/settings/test-notify", "Send test notification"),
    ("/api/settings", "Save settings"),
    ("/api/alerts/ack", "Acknowledge alerts"),
    ("/api/alerts/clear", "Clear alert history"),
    ("/api/recalibrate", "Recalibrate"),
    ("/api/decision", "Mark acted/passed"),
    ("/api/portfolio-sim", "Restart live portfolio test"),
    ("/sensitivity", "Change sensitivity"),
    ("/params/reset", "Reset rules"),
    ("/params", "Edit rules"),
    ("/category", "Change category"),
]

def _guest_action_text(path, blocked):
    body = request.get_json(silent=True) or {}
    if path == "/api/ai/chat":
        return f"Asked AI: “{(body.get('message') or '').strip()[:200]}”"
    name = next((n for k, n in GUEST_ACTION_NAMES if k in path), path)
    sym = (body.get("symbol") or "").upper() if isinstance(body, dict) else ""
    if not sym and path.startswith("/api/stock/"):
        sym = path.split("/")[3].upper()
    txt = f"{name} {sym}".strip()
    return f"Tried (blocked): {txt}" if blocked else txt

@app.route("/login", methods=["GET", "POST"])
def login():
    error_html = ""
    if request.method == "POST":
        pw = request.form.get("password") or ""
        role = None
        if hmac.compare_digest(pw, AUTH_PASSWORD):
            role = "owner"
        elif GUEST_PASSWORD and hmac.compare_digest(pw, GUEST_PASSWORD):
            if guest_expired():
                record_access("login_expired", "guest")
                error_html = '<div class="err">Guest access has expired</div>'
            else:
                role = "guest"
        else:
            record_access("login_failed", "")
            error_html = '<div class="err">Incorrect password</div>'
        if role:
            nxt = session.get("next", "")
            session.clear()
            session["authed"] = True
            session["role"] = role
            session["vid"] = secrets.token_hex(6)
            record_access("login", role)
            _login_email(role, client_ip(), device_label(request.headers.get("User-Agent")))
            # Only ever redirect to a dashboard deep link (e.g. /?stock=NVDA from an email).
            return redirect(nxt if nxt.startswith("/?") else url_for("dashboard"))
    return Response(LOGIN_PAGE.replace("__ERROR_HTML__", error_html), mimetype="text/html")

DECIDE_PAGE = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tripwire — Decision</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0A0C12;color:#E4E0D8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  min-height:100vh;display:flex;align-items:center;justify-content:center;padding:16px}
.box{background:#12151F;border:1px solid #1E2235;border-radius:14px;padding:28px;width:360px;max-width:100%;text-align:center}
.logo{font-weight:800;font-size:20px;color:#F59E0B;margin-bottom:14px}
p{margin-bottom:14px;line-height:1.5}
button{width:100%;background:#F59E0B;color:#000;border:none;border-radius:8px;padding:12px;font-weight:700;font-size:14px;cursor:pointer}
a{color:#93C5FD}
.muted{color:#9CA3AF;font-size:13px}
</style></head><body><form class="box" method="POST"><div class="logo">⚡ Tripwire</div>__BODY__</form></body></html>"""

def _decide_page(body, status=200):
    return Response(DECIDE_PAGE.replace("__BODY__", body), status=status, mimetype="text/html")

@app.route("/decide/<int:event_id>/<decision>/<sig>", methods=["GET", "POST"])
def decide(event_id, decision, sig):
    """Acted/passed buttons in emails. The signed link needs no login; GET only asks for
    confirmation, because mail link-scanners open links automatically and would record a
    decision nobody made."""
    if decision not in ("acted", "passed") or not hmac.compare_digest(sig, _decision_sig(event_id, decision)):
        return _decide_page("<p>This link isn't valid.</p>", 404)
    rows = _event_rows(event_id)
    if not rows:
        return _decide_page("<p>That signal no longer exists.</p>", 404)
    r0 = rows[0]
    what = f"{h_esc(r0['symbol'])} {h_esc(_event_label(rows))} from {h_esc(_fmt_day(datetime.fromtimestamp(r0['timestamp']).date()))}"
    open_link = f'<p class="muted"><a href="/?stock={h_esc(r0["symbol"])}">Open Tripwire</a></p>'
    if request.method == "POST":
        set_decision([r["id"] for r in rows], decision)
        return _decide_page(f"<p>Recorded: you <b>{decision}</b> on {what}. ✓</p>"
                            "<p class='muted'>You'll see how it played out in the follow-up email and under Performance.</p>" + open_link)
    other = "passed" if decision == "acted" else "acted"
    return _decide_page(f"<p>{what}</p><button type='submit'>Confirm: I {decision}</button>"
                        f"<p class='muted' style='margin-top:14px'>Wrong button? "
                        f"<a href='{h_esc(decision_link(event_id, other) or '')}'>I {other} instead</a></p>")

@app.route("/logout")
def logout():
    if is_guest():
        record_access("event", "guest", "Logged out")
    session.clear()
    return redirect(url_for("login"))

@app.before_request
def enforce_guest_limits():
    if not is_guest():
        return None
    if guest_expired():
        session.clear()
        if request.path.startswith("/api/"):
            return jsonify({"success": False, "error": "Guest access has expired"}), 401
        return redirect(url_for("login"))
    if request.path == "/api/alerts/export.csv":
        record_access("event", "guest", "Exported alerts CSV")
    if request.method not in ("GET", "HEAD", "OPTIONS") and request.path not in ("/login", "/api/track"):
        blocked = request.path not in GUEST_ALLOWED_POSTS
        record_access("event", "guest", _guest_action_text(request.path, blocked))
        if blocked:
            return jsonify({"success": False, "guest": True, "error": GUEST_BLOCKED_MSG}), 403
    return None

def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get("authed"):
            if request.path.startswith("/api/"):
                return jsonify({"success": False, "error": "Not authenticated"}), 401
            if request.path == "/" and request.query_string:
                session["next"] = request.full_path
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapped

# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────

def _days_until(date_str):
    if not date_str:
        return None
    try:
        return (datetime.strptime(date_str, "%Y-%m-%d").date() - datetime.now().date()).days
    except Exception:
        return None

@app.route("/api/status")
@login_required
def api_status():
    stocks = get_stocks()
    with state_lock:
        checking, last_check = state["checking"], state["last_check"]
    return jsonify({
        "status": "checking" if checking else "running",
        "stocks": len(stocks),
        "last_check": last_check,
        "market_phase": market_phase(),
        "ai_enabled": bool(get_anthropic_key()),
        "guest": is_guest(),
        "guest_expires": GUEST_EXPIRES if is_guest() else None,
        "guest_ai_left": guest_ai_left() if is_guest() else None,
        "guest_ai_limit": GUEST_AI_PER_LOGIN,
        "regime": get_market_regime(),
        # Read fresh from the DB (not the boot-time settings cache) since `backtest.py apply`
        # may run against the live DB while the app is up and we want the banner to notice.
        "calibrated_at": _get_calibrated_at_fresh(),
    })

@app.route("/api/rule-stats")
@login_required
def api_rule_stats():
    return jsonify(RULE_STATS)

@app.route("/api/stocks")
@login_required
def api_stocks():
    results = []
    for s in get_stocks():
        sym, cat = s["symbol"], s["category"]
        row    = get_latest(sym)
        with state_lock:
            cached = dict(state["results"].get(sym, {}))
        rules  = cached.get("rules", [])
        any_alert = any(r.get("triggered") and not r.get("disabled")
                        and r.get("rule_type") not in CONTEXT_ONLY_RULES for r in rules)
        with extended_lock:
            ext = dict(extended_state.get(sym, {}))
        if row:
            pct = ((row["close"] - row["prev_close"]) / row["prev_close"] * 100) if row["prev_close"] else 0
            results.append({
                "symbol": sym, "category": cat,
                "price": row["close"], "prev_close": row["prev_close"],
                "pct": round(pct, 2),
                "high": row["high"], "low": row["low"],
                "pre_market": row["pre_market"], "post_market": row["post_market"],
                "time": datetime.fromtimestamp(row["timestamp"]).strftime("%H:%M"),
                "date": datetime.fromtimestamp(row["timestamp"]).strftime("%Y-%m-%d"),
                "alert": any_alert, "rules": rules,
                "params": cached.get("params", get_params(sym, cat)),
                "error": cached.get("error"),
                "week52_high":   cached.get("week52_high"),
                "week52_low":    cached.get("week52_low"),
                "lifetime_high": ext.get("lifetime_high"),
                "lifetime_low":  ext.get("lifetime_low"),
                "earnings_date": ext.get("earnings_date"),
                "earnings_in_days": _days_until(ext.get("earnings_date")),
                "volume": row["volume"] if "volume" in row.keys() else None,
                "history_closes": cached.get("history_closes", []),
                "info_events": cached.get("info_events", []),
                "guest_added": s.get("added_by") == "guest",
            })
        else:
            results.append({"symbol": sym, "category": cat, "price": None,
                            "error": cached.get("error", "Waiting for first check..."),
                            "rules": [], "history_closes": [], "info_events": [],
                            "guest_added": s.get("added_by") == "guest"})
    return jsonify(results)

@app.route("/api/history/<symbol>")
@login_required
def api_history(symbol):
    """Daily closes (~13 months, covers 12M/YTD/3M/1M) plus a 5-day intraday series
    for the detail-panel price chart. Cached so opening a card is cheap and repeatable."""
    symbol = symbol.upper()
    def _daily():
        h = yf.Ticker(symbol).history(period="13mo")
        if h is None or h.empty:
            return []
        return [{"date": ts.strftime("%Y-%m-%d"), "close": round(float(r["Close"]), 2)}
                for ts, r in h.iterrows()]
    def _intraday():
        h = yf.Ticker(symbol).history(period="5d", interval="30m")
        if h is None or h.empty:
            return []
        return [{"t": ts.strftime("%m-%d %H:%M"), "close": round(float(r["Close"]), 2)}
                for ts, r in h.iterrows()]
    try:
        daily = yf_cached(f"chart_daily:{symbol}", 3600, _daily)
        intraday = yf_cached(f"chart_intraday:{symbol}", 900, _intraday)
        return jsonify({"symbol": symbol, "daily": daily, "intraday": intraday})
    except Exception as e:
        return jsonify({"symbol": symbol, "daily": [], "intraday": [], "error": str(e)})

@app.route("/api/alerts")
@login_required
def api_alerts():
    rows = get_alerts(200)
    return jsonify({"alerts": [{
        "id": r["id"], "symbol": r["symbol"], "rule_type": r["rule_type"],
        "message": r["message"], "price": r["price"],
        "detail": r["detail"],
        "ack": (r["ack"] if "ack" in r.keys() else 0) or 0,
        "time": datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M"),
        "ts": r["timestamp"],
        "event_id": r["event_id"] if "event_id" in r.keys() else None,
        "decision": r["decision"] if "decision" in r.keys() else None,
    } for r in rows]})

@app.route("/api/decision", methods=["POST"])
@login_required
def api_decision():
    data = request.json or {}
    decision = data.get("decision")
    if decision not in ("acted", "passed", None):
        return jsonify({"success": False, "error": "decision must be acted, passed or null"}), 400
    set_decision(data.get("ids") or [], decision)
    return jsonify({"success": True})

@app.route("/api/alerts/ack", methods=["POST"])
@login_required
def api_alerts_ack():
    data = request.json or {}
    ids = data.get("ids"); symbol = data.get("symbol")
    with get_db() as conn:
        if ids:
            conn.executemany("UPDATE alerts SET ack=1 WHERE id=?", [(i,) for i in ids])
        elif symbol:
            conn.execute("UPDATE alerts SET ack=1 WHERE symbol=?", (symbol.upper(),))
        else:
            conn.execute("UPDATE alerts SET ack=1")
        conn.commit()
    return jsonify({"success": True})

@app.route("/api/alerts/clear", methods=["POST"])
@login_required
def api_alerts_clear():
    symbol = (request.json or {}).get("symbol")
    with get_db() as conn:
        if symbol:
            conn.execute("DELETE FROM alerts WHERE symbol=?", (symbol.upper(),))
        else:
            conn.execute("DELETE FROM alerts")
        conn.commit()
    return jsonify({"success": True})

@app.route("/api/alerts/export.csv")
@login_required
def api_alerts_export():
    rows = get_alerts(100000)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["timestamp","symbol","rule_type","message","price","signal","actual_value","threshold","ack"])
    for r in rows:
        d = {}
        try: d = json.loads(r["detail"]) if r["detail"] else {}
        except Exception: pass
        w.writerow([
            datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
            r["symbol"], r["rule_type"], r["message"], r["price"],
            d.get("signal",""), d.get("actual_value",""), d.get("threshold",""),
            (r["ack"] if "ack" in r.keys() else 0) or 0,
        ])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=tripwire_alerts.csv"})

@app.route("/api/analytics")
@login_required
def api_analytics():
    """Performance scorecard: how the app's OWN signals actually played out (self-scoring),
    not alert-volume vanity metrics. Everything here is built from resolved outcomes
    (alerts scored ~5 trading days after firing by resolve_outcomes)."""
    with get_db() as conn:
        # Per-rule live outcomes vs backtested edge
        orows = conn.execute(
            "SELECT rule_type, COUNT(*) n, AVG(outcome_excess) avg_exc, "
            "AVG(CASE WHEN outcome_correct=1 THEN 1.0 ELSE 0.0 END) hit "
            "FROM alerts WHERE outcome_ts IS NOT NULL AND outcome_excess IS NOT NULL "
            "GROUP BY rule_type").fetchall()
        scored = [dict(r) for r in conn.execute(
            "SELECT id, symbol, rule_type, timestamp, detail, event_id, decision, outcome_ret, outcome_excess "
            "FROM alerts WHERE outcome_ts IS NOT NULL AND outcome_excess IS NOT NULL ORDER BY timestamp")]
        pending = conn.execute("SELECT COUNT(*) c FROM alerts WHERE outcome_ts IS NULL").fetchone()["c"]

    # Signals, not rule triggers: one move that tripped several rules (or several cycles of
    # the same day, for alerts that predate event grouping) counts once. Counting per rule
    # let a single bad day dominate the record.
    groups = {}
    for r in scored:
        key = f"e{r['event_id']}" if r["event_id"] else f"d{r['symbol']}:{datetime.fromtimestamp(r['timestamp']):%Y-%m-%d}"
        groups.setdefault(key, []).append(r)
    signals = []
    for rows in groups.values():
        r0 = rows[0]
        signals.append({
            "ids": [r["id"] for r in rows], "symbol": r0["symbol"], "ts": r0["timestamp"],
            "label": _event_label(rows), "strong": any(_detail_of(r).get("strong") for r in rows),
            "ret": r0["outcome_ret"], "excess": r0["outcome_excess"],
            "decision": next((r["decision"] for r in rows if r["decision"]), None),
        })
    signals.sort(key=lambda s: s["ts"])
    head = {"n": len(signals),
            "avg_exc": (sum(s["excess"] for s in signals) / len(signals)) if signals else None,
            "hit": (sum(1 for s in signals if s["excess"] > 0) / len(signals)) if signals else None}

    def _grp(items):
        if not items:
            return None
        return {"n": len(items), "avg_excess": round(sum(i["excess"] for i in items) / len(items), 2),
                "hit": round(100 * sum(1 for i in items if i["excess"] > 0) / len(items), 1)}
    strong_sigs = [s for s in signals if s["strong"]]
    money = None
    if strong_sigs:
        money = {"n": len(strong_sigs), "stake": 1000,
                 "vs_market": round(sum(s["excess"] for s in strong_sigs) * 10, 0),
                 "raw": round(sum(s["ret"] for s in strong_sigs) * 10, 0)}
    decisions = {"acted": _grp([s for s in strong_sigs if s["decision"] == "acted"]),
                 "passed": _grp([s for s in strong_sigs if s["decision"] == "passed"]),
                 "unmarked": sum(1 for s in strong_sigs if not s["decision"])}
    by_sym = {}
    for s in signals:
        by_sym.setdefault(s["symbol"], []).append(s)
    by_stock_list = sorted(({"symbol": k, **_grp(v)} for k, v in by_sym.items()),
                           key=lambda x: -x["avg_excess"])

    # Backtested baseline per rule = simple average of the per-symbol rule_stats entries.
    bt = {}
    for sym, rules in RULE_STATS.items():
        for rt, st in rules.items():
            bt.setdefault(rt, []).append(st)
    outcomes = []
    for r in orows:
        base = bt.get(r["rule_type"], [])
        outcomes.append({
            "rule_type": r["rule_type"], "n": r["n"],
            "live_excess": round(r["avg_exc"], 2) if r["avg_exc"] is not None else None,
            "live_hit": round(r["hit"] * 100, 1) if r["hit"] is not None else None,
            "bt_excess": round(sum(s.get("exc5", 0) for s in base) / len(base), 2) if base else None,
            "bt_hit": round(sum(s.get("hit5", 0) for s in base) / len(base), 1) if base else None,
        })
    outcomes.sort(key=lambda x: -(x["live_excess"] if x["live_excess"] is not None else -99))

    recent_list = [{
        "ids": s["ids"], "symbol": s["symbol"], "label": s["label"], "strong": s["strong"],
        "date": datetime.fromtimestamp(s["ts"]).strftime("%b %d"),
        "ret": s["ret"], "market": round(s["ret"] - s["excess"], 2), "excess": s["excess"],
        "decision": s["decision"],
    } for s in reversed(signals[-15:])]

    # Overall backtested hit rate across all rules that have live outcomes, for calibration drift.
    live_hits = [o["live_hit"] for o in outcomes if o["live_hit"] is not None]
    bt_hits   = [o["bt_hit"]   for o in outcomes if o["bt_hit"]   is not None]
    headline = {
        "n": head["n"] or 0,
        "pct_correct": round(head["hit"] * 100, 1) if head["hit"] is not None else None,
        "avg_excess": round(head["avg_exc"], 2) if head["avg_exc"] is not None else None,
    }
    calibration = None
    if headline["n"] >= 10 and live_hits and bt_hits:
        live_overall = sum(live_hits) / len(live_hits)
        bt_overall = sum(bt_hits) / len(bt_hits)
        calibration = {
            "live_hit": round(live_overall, 1), "bt_hit": round(bt_overall, 1),
            "drifting": live_overall < bt_overall - 8,  # meaningfully below backtest
        }
    return jsonify({
        "headline": headline,
        "outcomes": outcomes,
        "by_stock": by_stock_list,
        "recent": recent_list,
        "money": money,
        "decisions": decisions,
        "calibration": calibration,
        "outcomes_pending": pending,
    })

@app.route("/api/check", methods=["POST"])
@login_required
def api_check():
    with state_lock:
        if state["checking"]:
            return jsonify({"success": False, "error": "Check already in progress"})
    symbols = request.json.get("symbols") if request.json else None
    worker_pool.submit(run_check, symbols)
    return jsonify({"success": True})

@app.route("/api/stocks/add", methods=["POST"])
@login_required
def api_add_stock():
    data   = request.json
    symbol = data.get("symbol","").upper().strip()
    cat    = data.get("category","high_vol")
    if not symbol:
        return jsonify({"success": False, "error": "No symbol provided"})
    guest = is_guest()
    if guest:
        active = {s["symbol"]: s for s in get_stocks()}
        if symbol in active:
            return jsonify({"success": False, "error": f"{symbol} is already on the watchlist"})
        if sum(1 for s in active.values() if s.get("added_by") == "guest") >= GUEST_MAX_TICKERS:
            return jsonify({"success": False, "error": f"Guests can add up to {GUEST_MAX_TICKERS} tickers — remove one of the guest-added ones first"})
    try:
        quote = fetch_quote(symbol)
        store_price(symbol, quote)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not fetch {symbol}: {e}"})
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO stocks (symbol,category,active,added_at,added_by) VALUES (?,?,1,?,?)",
            (symbol, cat, int(time.time()), "guest" if guest else "owner")
        )
        conn.commit()
    worker_pool.submit(run_check, [symbol])
    worker_pool.submit(fetch_extended_data, symbol)
    worker_pool.submit(populate_history, symbol)
    return jsonify({"success": True, "symbol": symbol, "price": quote["close"]})

@app.route("/api/stocks/remove", methods=["POST"])
@login_required
def api_remove_stock():
    symbol = request.json.get("symbol","").upper().strip()
    if is_guest() and not is_guest_stock(symbol):
        return jsonify({"success": False, "guest": True,
                        "error": "Guests can only remove tickers that guests added."}), 403
    with get_db() as conn:
        conn.execute("UPDATE stocks SET active=0 WHERE symbol=?", (symbol,))
        conn.commit()
    with state_lock:
        state["results"].pop(symbol, None)
    return jsonify({"success": True})

@app.route("/api/stock/<symbol>/category", methods=["POST"])
@login_required
def api_set_category(symbol):
    cat = request.json.get("category","high_vol")
    with get_db() as conn:
        conn.execute("UPDATE stocks SET category=? WHERE symbol=?", (cat, symbol.upper()))
        conn.commit()
    return jsonify({"success": True})

@app.route("/api/stock/<symbol>/params", methods=["POST"])
@login_required
def api_set_params(symbol):
    sym    = symbol.upper()
    data   = request.json
    stocks = {s["symbol"]: s for s in get_stocks()}
    cat    = stocks.get(sym, {}).get("category","high_vol")
    current = get_params(sym, cat)
    current.update({k: v for k, v in data.items() if k in current})
    save_params(sym, current)
    worker_pool.submit(run_check, [sym])
    return jsonify({"success": True, "params": current})

@app.route("/api/stock/<symbol>/sensitivity", methods=["POST"])
@login_required
def api_set_sensitivity(symbol):
    """Set the high-level sensitivity dial. Authoritative: replaces any advanced per-rule
    overrides with a clean preset so switching the dial always fully takes effect."""
    sym = symbol.upper()
    level = (request.json or {}).get("level", "calibrated")
    if level not in SENSITIVITY_LEVELS:
        return jsonify({"success": False, "error": "invalid level"}), 400
    save_params(sym, {"_sensitivity": level})
    worker_pool.submit(run_check, [sym])
    return jsonify({"success": True, "level": level})

@app.route("/api/stock/<symbol>/params/reset", methods=["POST"])
@login_required
def api_reset_params(symbol):
    sym = symbol.upper()
    reset_params(sym)
    worker_pool.submit(run_check, [sym])
    return jsonify({"success": True})

# ── One-click recalibration (runs the standalone backtester as a subprocess) ──────
BACKTEST_PY = Path(__file__).parent / "backtest.py"
RECAL_STATE = {"running": False, "phase": "idle", "rc": None, "tail": "", "started": None}
_recal_lock = threading.Lock()

RULE_STATS_PENDING = RULE_STATS_PATH.with_name("rule_stats.pending.json")
RULE_STATS_PREV = RULE_STATS_PATH.with_name("rule_stats.prev.json")

def _reload_rule_stats():
    global RULE_STATS
    RULE_STATS = _load_rule_stats()

def _run_backtest(args, phase):
    """A backtest run rewrites rule_stats.json for its *suggested* thresholds. Until those are
    applied, the live file must keep describing the thresholds actually in use, so a run's new
    stats are parked as pending and only swapped in on apply (and swapped back on undo)."""
    RECAL_STATE.update({"running": True, "phase": phase, "rc": None, "started": int(time.time())})
    refresh = "--refresh" in args
    live_stats = RULE_STATS_PATH.read_bytes() if refresh and RULE_STATS_PATH.exists() else None
    try:
        proc = subprocess.run([sys.executable, str(BACKTEST_PY), *args],
                              cwd=str(BACKTEST_PY.parent), capture_output=True, text=True, timeout=1800)
        RECAL_STATE["rc"] = proc.returncode
        RECAL_STATE["tail"] = (proc.stdout or "")[-1500:] + (("\n[stderr]\n" + proc.stderr[-800:]) if proc.returncode else "")
        if refresh and RULE_STATS_PATH.exists():
            if proc.returncode == 0:
                RULE_STATS_PENDING.write_bytes(RULE_STATS_PATH.read_bytes())
            if live_stats is not None:
                RULE_STATS_PATH.write_bytes(live_stats)
        elif args == ["apply"] and proc.returncode == 0 and RULE_STATS_PENDING.exists():
            if RULE_STATS_PATH.exists():
                RULE_STATS_PREV.write_bytes(RULE_STATS_PATH.read_bytes())
            RULE_STATS_PENDING.replace(RULE_STATS_PATH)
            _reload_rule_stats()
        elif args == ["undo"] and proc.returncode == 0 and RULE_STATS_PREV.exists():
            RULE_STATS_PREV.replace(RULE_STATS_PATH)
            _reload_rule_stats()
    except Exception as e:
        RECAL_STATE["rc"] = -1
        RECAL_STATE["tail"] = f"error: {e}"
        if live_stats is not None:
            RULE_STATS_PATH.write_bytes(live_stats)
    finally:
        RECAL_STATE["running"] = False
        RECAL_STATE["phase"] = "done"
    if RECAL_STATE["rc"] == 0 and (refresh or args in (["apply"], ["undo"])):
        portfolio_build_async()   # new data or new live thresholds -> redo the backtest portfolio

PARAM_NAMES = {
    "volatility_multiplier": "unusual-move threshold (× normal daily move)",
    "volatility_lookback": "unusual-move lookback (days)",
    "support_resist_pct": "support/resistance buffer (%)", "support_resist_lookback": "support/resistance lookback (days)",
    "volume_multiplier": "volume-spike threshold (× normal volume)", "volume_lookback": "volume lookback (days)",
    "gap_pct": "opening-gap threshold (%)", "rsi_period": "RSI period", "rsi_overbought": "RSI overbought level",
    "rsi_oversold": "RSI oversold level", "enable_volatility": "unusual-move rule",
    "enable_support_resistance": "support/resistance rule", "enable_volume": "volume rule",
    "enable_gap": "opening-gap rule", "enable_rsi": "RSI rule", "enable_ma": "moving-average rule",
}

def auto_recal_loop():
    """Monthly, at night: re-run the backtest on fresh data and email the owner only if it
    suggests threshold changes. Nothing is applied without the owner pressing Apply."""
    time.sleep(300)
    while True:
        try:
            now = datetime.now()
            last = get_setting("auto_recal_last", "")
            due = not last or (now.date() - datetime.strptime(last, "%Y-%m-%d").date()).days >= 30
            if get_setting_bool("auto_recal_enabled") and due and now.hour == 3:
                with _recal_lock:
                    busy = RECAL_STATE["running"]
                    if not busy:
                        RECAL_STATE["running"] = True
                if not busy:
                    set_setting("auto_recal_last", now.strftime("%Y-%m-%d"))
                    _run_backtest(["--refresh"], "monthly check")
                    diff = _recal_diff() or [] if RECAL_STATE["rc"] == 0 else []
                    if diff:
                        worker_pool.submit(_send_recal_email, diff)
        except Exception as e:
            log.warning("auto_recal_loop error: %s", e)
        time.sleep(1800)

def _send_recal_email(diff):
    link = f"{PUBLIC_URL}/?tab=settings" if PUBLIC_URL else None
    fmt = lambda v: "on" if v is True else "off" if v is False else ("—" if v is None else str(v))
    lines = [f"{c['symbol']}: {PARAM_NAMES.get(c['key'], c['key'])} {fmt(c['from'])} → {fmt(c['to'])}" for c in diff[:20]]
    more = f"\n… and {len(diff) - 20} more" if len(diff) > 20 else ""
    subject = f"🔄 Tripwire monthly recalibration: {len(diff)} suggested change{'s' if len(diff) != 1 else ''}"
    intro = ("Tripwire re-ran its 5-year backtest on fresh prices. These thresholds would now work better "
             "than the ones in use. Nothing changes until you review them and press Apply.")
    text = intro + "\n\n" + "\n".join(lines) + more + (f"\n\nReview and apply: {link}" if link else "\n\nReview in Settings → Recalibration.")
    html = _email_shell(f'<h2 style="margin:0 0 8px">Monthly recalibration</h2><p>{h_esc(intro)}</p><ul>'
                        + "".join(f"<li>{h_esc(l)}</li>" for l in lines) + "</ul>" + (f"<p>{h_esc(more)}</p>" if more else "")
                        + (_btn(link, "Review and apply", "#2563EB") if link else "<p>Review in Settings → Recalibration.</p>"))
    _send_email(subject, text, html)

threading.Thread(target=auto_recal_loop, daemon=True).start()

# ── Portfolio test: $1,000 following the app's STRONG signals (rulebook in portfolio_sim.py) ──
PORTFOLIO_JSON = Path(__file__).parent / "backtest_results" / "portfolio_backtest.json"
LIVE_PARAMS_JSON = Path(__file__).parent / "backtest_results" / "live_params.json"
PF_STATE = {"building": False, "error": None}
_pf_lock = threading.Lock()
_live_pf_cache = {"ts": 0.0, "data": None}

def _owner_stocks():
    return [s for s in get_stocks() if s.get("added_by") != "guest"]

def _live_params_snapshot():
    return {s["symbol"]: {"category": s["category"], "params": get_params(s["symbol"], s["category"])}
            for s in _owner_stocks()}

def _fingerprint(snap):
    return hashlib.sha1(json.dumps(snap, sort_keys=True).encode()).hexdigest()[:16]

def backtest_portfolio_stale():
    """The backtest portfolio must reflect the thresholds live right now; any change to a stock's
    rules, sensitivity or the watchlist changes the fingerprint and triggers a rebuild."""
    return not PORTFOLIO_JSON.exists() or get_setting("sim_bt_fingerprint", "") != _fingerprint(_live_params_snapshot())

def build_backtest_portfolio():
    with _pf_lock:
        if PF_STATE["building"] or RECAL_STATE["running"]:
            return False
        PF_STATE["building"] = True
    try:
        snap = _live_params_snapshot()
        LIVE_PARAMS_JSON.parent.mkdir(parents=True, exist_ok=True)
        LIVE_PARAMS_JSON.write_text(json.dumps(snap), encoding="utf-8")
        proc = subprocess.run([sys.executable, str(BACKTEST_PY), "portfolio"], cwd=str(BACKTEST_PY.parent),
                              capture_output=True, text=True, timeout=900)
        if proc.returncode != 0:
            PF_STATE["error"] = ((proc.stderr or "") + (proc.stdout or ""))[-600:]
            log.warning("portfolio backtest failed: %s", PF_STATE["error"])
            return False
        PF_STATE["error"] = None
        fp = _fingerprint(snap)
        set_setting("sim_bt_fingerprint", fp)
        res = json.loads(PORTFOLIO_JSON.read_text(encoding="utf-8"))
        s, f = res["summary"], res.get("fair_summary") or {}
        with get_db() as conn:
            conn.execute(
                "INSERT INTO sim_backtest_runs (ts,start_date,end_date,strategy_end,untouched_end,spy_end,strategy_cagr,"
                "untouched_cagr,spy_cagr,fair_strategy_pct,fair_untouched_pct,fair_spy_pct,fingerprint) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (int(time.time()), res["start_date"], res["end_date"], s["strategy"]["end"], s["untouched"]["end"],
                 s["spy"]["end"], s["strategy"]["cagr_pct"], s["untouched"]["cagr_pct"], s["spy"]["cagr_pct"],
                 f.get("strategy", {}).get("total_pct"), f.get("untouched", {}).get("total_pct"),
                 f.get("spy", {}).get("total_pct"), fp))
            conn.commit()
        return True
    except Exception as e:
        PF_STATE["error"] = str(e)
        log.warning("portfolio backtest failed: %s", e)
        return False
    finally:
        PF_STATE["building"] = False

def portfolio_build_async():
    threading.Thread(target=build_backtest_portfolio, daemon=True).start()

def _sim_bars(sym, start):
    def fetch():
        h = yf.Ticker(sym).history(start=start, auto_adjust=True)
        if h is None or h.empty:
            return {}
        return {ts.strftime("%Y-%m-%d"): (float(r["Open"]), float(r["Close"]))
                for ts, r in h.iterrows() if r["Close"] == r["Close"] and r["Open"] == r["Open"]}
    try:
        return yf_cached(f"simbars:{sym}:{start}", 1800, fetch) or {}
    except Exception:
        return {}

def _live_sim_start():
    start = get_setting("sim_live_start", "")
    if not start:
        start = datetime.now().strftime("%Y-%m-%d")
        set_setting("sim_live_start", start)
        set_setting("sim_live_symbols", json.dumps([s["symbol"] for s in _owner_stocks()]))
    try:
        syms = json.loads(get_setting("sim_live_symbols", "[]") or "[]")
    except Exception:
        syms = []
    return start, syms

def _live_signals(start):
    """STRONG events the app actually logged after the start day, one per event, at the alert price."""
    ts0 = int(datetime.strptime(start, "%Y-%m-%d").timestamp()) + 86400
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM alerts WHERE timestamp >= ? ORDER BY timestamp, id", (ts0,))]
    groups = {}
    for r in rows:
        groups.setdefault(r.get("event_id") or r["id"], []).append(r)
    out = []
    for rs in groups.values():
        r0 = rs[0]
        if not any(_detail_of(r).get("strong") for r in rs) or is_guest_stock(r0["symbol"]) or not r0["price"]:
            continue
        out.append({"date": datetime.fromtimestamp(r0["timestamp"]).strftime("%Y-%m-%d"), "sym": r0["symbol"],
                    "label": _event_label(rs), "price": float(r0["price"]), "order": r0["timestamp"]})
    return out

def compute_live_portfolio(force=False):
    if not force and _live_pf_cache["data"] is not None and time.time() - _live_pf_cache["ts"] < 600:
        return _live_pf_cache["data"]
    start, syms = _live_sim_start()
    signals = _live_signals(start)
    spy = {d: c for d, (o, c) in _sim_bars("SPY", start).items()}
    calendar = sorted(d for d in spy if d >= start)
    data = {"start": start, "symbols": syms, "signals_seen": len(signals)}
    if calendar:
        # A signal logged on a weekend/holiday (e.g. a Saturday check of Friday's bars) belongs to
        # the last trading day before it; its price is no longer tradable, so it buys at the next open.
        days = set(calendar)
        bars = {s: _sim_bars(s, start) for s in sorted(set(syms) | {g["sym"] for g in signals})}
        # (A weekday after the last bar is just today before its bar exists — left as is.)
        for g in signals:
            d = g["date"]
            if d in days or (d > calendar[-1] and datetime.strptime(d, "%Y-%m-%d").weekday() < 5):
                continue
            prior = [x for x in calendar if x < d]
            if prior:
                g["date"], g["price"] = prior[-1], None
            else:   # logged before the test's first trading day: its first chance is that day's open
                first_open = (bars.get(g["sym"], {}).get(calendar[0]) or (None,))[0]
                g["date"], g["price"] = calendar[0], first_open
        res = portfolio_sim.simulate(bars, spy, calendar, syms, signals)
        if res:
            data.update(res)
            with get_db() as conn:
                conn.executemany("INSERT OR REPLACE INTO sim_daily (date,start,strategy,untouched,spy) VALUES (?,?,?,?,?)",
                                 [(e["date"], start, e["strategy"], e["untouched"], e["spy"]) for e in res["equity"]])
                conn.commit()
    _live_pf_cache.update(ts=time.time(), data=data)
    return data

def portfolio_loop():
    """Keeps both sides current: rebuilds the backtest portfolio whenever the live thresholds or
    watchlist change, and stores the live portfolio's values each evening after the US close."""
    time.sleep(240)
    _live_sim_start()   # the live test starts the first day this runs
    while True:
        try:
            if backtest_portfolio_stale():
                build_backtest_portfolio()
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            if (now.hour, now.minute) >= (22, 30) and get_setting("sim_live_snapshot", "") != today:
                compute_live_portfolio(force=True)
                set_setting("sim_live_snapshot", today)
                log.info("portfolio test: stored live snapshot for %s", today)
        except Exception as e:
            log.warning("portfolio_loop error: %s", e)
        time.sleep(900)

threading.Thread(target=portfolio_loop, daemon=True).start()

@app.route("/api/portfolio-sim")
@login_required
def api_portfolio_sim():
    bt = None
    if PORTFOLIO_JSON.exists():
        try:
            bt = json.loads(PORTFOLIO_JSON.read_text(encoding="utf-8"))
        except Exception:
            bt = None
    stale = backtest_portfolio_stale()
    if stale and not PF_STATE["building"]:
        portfolio_build_async()
    with get_db() as conn:
        runs = [dict(r) for r in conn.execute(
            "SELECT ts,strategy_cagr,untouched_cagr,spy_cagr FROM sim_backtest_runs ORDER BY id DESC LIMIT 8")]
    return jsonify({"backtest": bt, "backtest_updating": bool(stale or PF_STATE["building"]),
                    "backtest_error": PF_STATE["error"], "backtest_runs": runs,
                    "live": compute_live_portfolio()})

@app.route("/api/portfolio-sim/restart", methods=["POST"])
@login_required
def api_portfolio_sim_restart():
    today = datetime.now().strftime("%Y-%m-%d")
    set_setting("sim_live_start", today)
    set_setting("sim_live_symbols", json.dumps([s["symbol"] for s in _owner_stocks()]))
    with get_db() as conn:
        conn.execute("DELETE FROM sim_daily"); conn.commit()
    _live_pf_cache.update(ts=0.0, data=None)
    return jsonify({"success": True, "start": today})

def _recal_diff():
    """Preview: which live thresholds/enables would change if the latest recommendation applied."""
    recp = Path(__file__).parent / "backtest_results" / "recommended_params.json"
    if not recp.exists():
        return None
    try:
        recs = json.loads(recp.read_text())
    except Exception:
        return None
    changes = []
    for sym, rec in recs.items():
        cat = next((s["category"] for s in get_stocks() if s["symbol"] == sym), "high_vol")
        cur = get_params(sym, cat)
        for k, v in rec.items():
            if k == "_sensitivity":
                continue
            old = cur.get(k)
            try:
                differ = (old is None) or (round(float(old), 4) != round(float(v), 4)) if isinstance(v, (int, float)) else (old != v)
            except Exception:
                differ = old != v
            if differ:
                changes.append({"symbol": sym, "key": k, "from": old, "to": v})
    return changes

@app.route("/api/recalibrate/run", methods=["POST"])
@login_required
def api_recal_run():
    with _recal_lock:
        if RECAL_STATE["running"]:
            return jsonify({"success": False, "error": "already running", "phase": RECAL_STATE["phase"]})
        # Full pipeline: refresh data → grid → combo → select (writes recommended_params.json + rule_stats.json)
        threading.Thread(target=_run_backtest, args=(["--refresh"], "backtesting"), daemon=True).start()
    return jsonify({"success": True})

@app.route("/api/recalibrate/status")
@login_required
def api_recal_status():
    out = dict(RECAL_STATE)
    if not out["running"] and out["phase"] == "done":
        out["diff"] = _recal_diff()
    return jsonify(out)

@app.route("/api/recalibrate/apply", methods=["POST"])
@login_required
def api_recal_apply():
    with _recal_lock:
        if RECAL_STATE["running"]:
            return jsonify({"success": False, "error": "busy"})
        threading.Thread(target=_run_backtest, args=(["apply"], "applying"), daemon=True).start()
    return jsonify({"success": True})

@app.route("/api/recalibrate/undo", methods=["POST"])
@login_required
def api_recal_undo():
    with _recal_lock:
        if RECAL_STATE["running"]:
            return jsonify({"success": False, "error": "busy"})
        threading.Thread(target=_run_backtest, args=(["undo"], "undoing"), daemon=True).start()
    return jsonify({"success": True})

# ── Settings ──────────────────────────────────────────────────────────────────

@app.route("/api/track", methods=["POST"])
@login_required
def api_track():
    if is_guest():
        detail = str((request.get_json(silent=True) or {}).get("d", "")).strip()
        if detail:
            record_access("event", "guest", detail[:160])
    return jsonify({"success": True})

@app.route("/api/access-log")
@login_required
def api_access_log():
    if is_guest():
        return jsonify({"success": False, "error": "Owner only"}), 403
    with get_db() as conn:
        summary = {}
        for role in ("guest", "owner"):
            n, ips, last = conn.execute(
                "SELECT COUNT(*), COUNT(DISTINCT ip), MAX(ts) FROM access_log WHERE kind='login' AND role=?",
                (role,)).fetchone()
            summary[role] = {"logins": n, "ips": ips, "last": last}
        summary["failed"] = conn.execute(
            "SELECT COUNT(*) FROM access_log WHERE kind IN ('login_failed','login_expired')").fetchone()[0]
        logins = [dict(r) for r in conn.execute(
            "SELECT ts,kind,role,vid,ip,device FROM access_log WHERE kind LIKE 'login%' ORDER BY ts DESC LIMIT 150")]
        vids = [l["vid"] for l in logins if l["role"] == "guest" and l["kind"] == "login" and l["vid"]]
        events = {}
        if vids:
            q = ",".join("?" * len(vids))
            for r in conn.execute(f"SELECT vid,ts,detail FROM access_log WHERE kind='event' AND vid IN ({q}) ORDER BY ts",
                                  vids):
                events.setdefault(r["vid"], []).append({"ts": r["ts"], "detail": r["detail"]})
    return jsonify({"success": True, "summary": summary, "logins": logins, "events": events})

def build_guest_activity_email(since):
    with get_db() as conn:
        logins = [dict(r) for r in conn.execute(
            "SELECT ts,vid,ip,device FROM access_log WHERE kind='login' AND role='guest' AND ts>? ORDER BY ts", (since,))]
        events = {}
        for r in conn.execute("SELECT vid,ts,detail FROM access_log WHERE kind='event' AND role='guest' AND ts>? ORDER BY ts",
                              (since,)):
            events.setdefault(r["vid"], []).append(r)
    if not logins and not events:
        return None
    fmt = lambda t: datetime.fromtimestamp(t).strftime("%H:%M")
    lines = []
    by_vid = {l["vid"]: l for l in logins}
    for vid in list(by_vid) + [v for v in events if v not in by_vid]:
        l = by_vid.get(vid)
        head = (f"Visit {fmt(l['ts'])} — {l['ip']} — {l['device']}" if l
                else "Visit that started earlier (still active)")
        lines.append(head)
        for e in events.get(vid, [])[:80]:
            lines.append(f"   {fmt(e['ts'])}  {e['detail']}")
        if len(events.get(vid, [])) > 80:
            lines.append(f"   … {len(events[vid]) - 80} more")
        lines.append("")
    ips = {l["ip"] for l in logins}
    subject = f"👀 Tripwire guest activity today — {len(logins)} login{'s' if len(logins) != 1 else ''} from {len(ips)} address{'es' if len(ips) != 1 else ''}"
    body = "What guests did in the last 24 hours (times are server local time):\n\n" + "\n".join(lines) + \
           "\nFull history: Settings → Access log."
    return subject, body

GUEST_REPORT_HOUR = 21

def guest_report_loop():
    time.sleep(90)
    while True:
        try:
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            # Persisted so a restart after the report hour doesn't send the same day's report twice.
            if now.hour >= GUEST_REPORT_HOUR and get_setting("guest_report_last", "") != today:
                set_setting("guest_report_last", today)
                rep = build_guest_activity_email(int(time.time()) - 86400)
                if rep:
                    worker_pool.submit(_send_email, *rep)
        except Exception as e:
            log.warning("guest_report_loop error: %s", e)
        time.sleep(600)

threading.Thread(target=guest_report_loop, daemon=True).start()

@app.route("/api/settings", methods=["GET"])
@login_required
def api_get_settings():
    out = {}
    for k in SETTINGS_DEFAULTS:
        if k in SETTINGS_SECRET:
            out[k] = ""  # never echo secrets
            out[k + "_set"] = bool(get_setting(k, ""))  # tell UI whether one is stored
        else:
            out[k] = get_setting(k, SETTINGS_DEFAULTS[k])
    if is_guest():
        for k in ("smtp_user", "notify_email_to", "callmebot_phone"):
            out[k] = _mask_contact(out.get(k, ""))
    return jsonify(out)

def _mask_contact(v):
    if not v:
        return v
    if "@" in v:
        name, domain = v.split("@", 1)
        return name[:1] + "•••@" + domain
    return v[:3] + "•••" + v[-2:] if len(v) > 5 else "•••"

@app.route("/api/settings", methods=["POST"])
@login_required
def api_post_settings():
    data = request.json or {}
    for k, v in data.items():
        if k not in SETTINGS_DEFAULTS:
            continue
        if k in SETTINGS_SECRET and (v is None or v == ""):
            continue  # blank secret = leave unchanged
        set_setting(k, v)
    return jsonify({"success": True})

@app.route("/api/settings/test-notify", methods=["POST"])
@login_required
def api_test_notify():
    # A manual test click should wait for the real outcome (up to the 15s SMTP/HTTP
    # timeouts) rather than reporting fire-and-forget "sent" regardless of what happened —
    # that blind-success behavior was exactly why a misconfigured channel looked "on" while
    # silently never delivering anything.
    email_future = worker_pool.submit(_send_email, "⚡ Tripwire test", "This is a Tripwire test notification.")
    whatsapp_future = worker_pool.submit(_send_whatsapp, "⚡ Tripwire test notification")
    email_ok, email_msg = email_future.result(timeout=20)
    whatsapp_ok, whatsapp_msg = whatsapp_future.result(timeout=20)
    return jsonify({"success": True,
                    "email": {"ok": email_ok, "message": email_msg},
                    "whatsapp": {"ok": whatsapp_ok, "message": whatsapp_msg}})

# ─────────────────────────────────────────────────────────────────────────────
# AI ASSISTANT
# ─────────────────────────────────────────────────────────────────────────────

VALID_CATEGORIES = ("high_vol", "mod_vol", "low_vol")

def _chat_history(limit=30):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT role,content FROM chat_messages ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]

def _chat_save(role, content):
    with get_db() as conn:
        conn.execute("INSERT INTO chat_messages (role,content,ts) VALUES (?,?,?)",
                     (role, content, int(time.time())))
        conn.commit()

AI_TOOLS = [
    {"name": "get_watchlist", "description": "List all monitored stocks with current price, % change, category, triggered-rule count, and upcoming earnings.",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "get_stock_detail", "description": "Full detail for one stock: quote, every rule's state/params/signal, and earnings date.",
     "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}},
    {"name": "get_recent_alerts", "description": "Recent alerts across the watchlist (most recent first).",
     "input_schema": {"type": "object", "properties": {"limit": {"type": "integer", "description": "Max alerts (default 20)"}}}},
    {"name": "get_news", "description": "Recent news headlines for a ticker.",
     "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}},
    {"name": "get_earnings", "description": "Next upcoming earnings date for a ticker.",
     "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}},
    {"name": "get_app_settings", "description": "Current app settings (monitoring cadence, notification toggles, models). Secrets are redacted.",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "add_stock", "description": "Add a ticker to the watchlist.",
     "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}, "category": {"type": "string", "enum": list(VALID_CATEGORIES)}}, "required": ["symbol"]}},
    {"name": "remove_stock", "description": "Remove a ticker from the watchlist. Confirm with the user first.",
     "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}},
    {"name": "set_category", "description": "Change a stock's volatility category (high_vol/mod_vol/low_vol), which resets it to that category's default rule thresholds unless custom params exist.",
     "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}, "category": {"type": "string", "enum": list(VALID_CATEGORIES)}}, "required": ["symbol", "category"]}},
    {"name": "set_rule_params", "description": "Adjust one stock's rule parameters and/or enable/disable rules. Pass only the keys you want to change. Enable flags: enable_volatility, enable_support_resistance, use_consecutive, enable_volume, enable_gap, enable_rsi, enable_ma. Thresholds include volatility_multiplier, volatility_lookback, support_resist_pct, support_resist_lookback, consecutive_down_days, volume_multiplier, volume_lookback, gap_pct, rsi_period, rsi_overbought, rsi_oversold, ma_short, ma_long, ma_cross_lookback.",
     "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}, "params": {"type": "object", "description": "Key/value rule params to update"}}, "required": ["symbol", "params"]}},
    {"name": "reset_rule_params", "description": "Reset a stock's rules to its category defaults.",
     "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}},
    {"name": "update_settings", "description": "Update app settings. Keys: check_interval_seconds, check_interval_closed_seconds, market_hours_only (0/1), alert_cooldown_hours, show_info_tier (0/1, day-to-day activity awareness below signal threshold), notify_strong_only (0/1, only push/email/WhatsApp for multi-rule-confirmed STRONG signals — default on), notify_email_enabled (0/1), notify_whatsapp_enabled (0/1), browser_notifications_enabled (0/1), alert_sound_enabled (0/1), ai_synthesis_model, ai_assistant_model. Do not set secrets here.",
     "input_schema": {"type": "object", "properties": {"changes": {"type": "object"}}, "required": ["changes"]}},
    {"name": "run_check_now", "description": "Trigger an immediate re-check of the watchlist (or specific symbols).",
     "input_schema": {"type": "object", "properties": {"symbols": {"type": "array", "items": {"type": "string"}}}}},
]

WRITE_TOOLS = {"add_stock", "remove_stock", "set_category", "set_rule_params",
               "reset_rule_params", "update_settings", "run_check_now"}

def _tool_label(name, inp):
    sym = (inp.get("symbol") or "").upper() if isinstance(inp, dict) else ""
    return {
        "get_watchlist": "Reading your watchlist…",
        "get_stock_detail": f"Inspecting {sym}…",
        "get_recent_alerts": "Reviewing recent alerts…",
        "get_news": f"Fetching {sym} news…",
        "get_earnings": f"Checking {sym} earnings…",
        "get_app_settings": "Reading settings…",
        "add_stock": f"Adding {sym} to watchlist…",
        "remove_stock": f"Removing {sym}…",
        "set_category": f"Recategorizing {sym}…",
        "set_rule_params": f"Adjusting {sym} rules…",
        "reset_rule_params": f"Resetting {sym} rules…",
        "update_settings": "Updating settings…",
        "run_check_now": "Running a check…",
    }.get(name, f"Running {name}…")

def execute_ai_tool(name, inp):
    inp = inp or {}
    try:
        if name == "get_watchlist":
            out = []
            for s in get_stocks():
                sym = s["symbol"]
                with state_lock:
                    cached = dict(state["results"].get(sym, {}))
                with extended_lock:
                    ext = dict(extended_state.get(sym, {}))
                q = cached.get("quote", {})
                rules = cached.get("rules", [])
                prev = q.get("prev_close")
                out.append({
                    "symbol": sym, "category": s["category"],
                    "price": q.get("close"),
                    "pct_change_today": round((q["close"] - prev) / prev * 100, 2) if q.get("close") and prev else None,
                    "triggered_rules": [r["rule_type"] for r in rules if r.get("triggered") and not r.get("disabled")],
                    "earnings_date": ext.get("earnings_date"),
                    "earnings_in_days": _days_until(ext.get("earnings_date")),
                })
            return {"watchlist": out}
        if name == "get_stock_detail":
            sym = inp["symbol"].upper()
            with state_lock:
                cached = dict(state["results"].get(sym, {}))
            with extended_lock:
                ext = dict(extended_state.get(sym, {}))
            if not cached:
                return {"error": f"{sym} not in watchlist or not yet checked"}
            return {"symbol": sym, "quote": cached.get("quote"),
                    "rules": [{k: r.get(k) for k in ("rule_type","label","triggered","disabled","signal","actual_value","threshold","message")} for r in cached.get("rules", [])],
                    "params": cached.get("params"),
                    "earnings_date": ext.get("earnings_date")}
        if name == "get_recent_alerts":
            lim = int(inp.get("limit", 20))
            rows = get_alerts(lim)
            return {"alerts": [{"symbol": r["symbol"], "rule_type": r["rule_type"], "message": r["message"],
                                "price": r["price"], "time": datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M")} for r in rows]}
        if name == "get_news":
            return {"symbol": inp["symbol"].upper(), "news": fetch_news(inp["symbol"].upper())}
        if name == "get_earnings":
            sym = inp["symbol"].upper()
            ed = fetch_earnings_date(sym)
            return {"symbol": sym, "earnings_date": ed, "in_days": _days_until(ed)}
        if name == "get_app_settings":
            return {k: get_setting(k, v) for k, v in SETTINGS_DEFAULTS.items() if k not in SETTINGS_SECRET}
        if name == "add_stock":
            sym = inp["symbol"].upper().strip()
            cat = inp.get("category", "high_vol")
            if cat not in VALID_CATEGORIES: cat = "high_vol"
            try:
                q = fetch_quote(sym); store_price(sym, q)
            except Exception as e:
                return {"error": f"Could not fetch {sym}: {e}"}
            with get_db() as conn:
                conn.execute("INSERT OR REPLACE INTO stocks (symbol,category,active,added_at,added_by) VALUES (?,?,1,?,'owner')",
                             (sym, cat, int(time.time())))
                conn.commit()
            worker_pool.submit(run_check, [sym]); worker_pool.submit(fetch_extended_data, sym); worker_pool.submit(populate_history, sym)
            return {"success": True, "symbol": sym, "price": q["close"], "category": cat}
        if name == "remove_stock":
            sym = inp["symbol"].upper().strip()
            with get_db() as conn:
                conn.execute("UPDATE stocks SET active=0 WHERE symbol=?", (sym,)); conn.commit()
            with state_lock:
                state["results"].pop(sym, None)
            return {"success": True, "symbol": sym}
        if name == "set_category":
            sym = inp["symbol"].upper(); cat = inp["category"]
            if cat not in VALID_CATEGORIES:
                return {"error": "invalid category"}
            with get_db() as conn:
                conn.execute("UPDATE stocks SET category=? WHERE symbol=?", (cat, sym)); conn.commit()
            worker_pool.submit(run_check, [sym])
            return {"success": True, "symbol": sym, "category": cat}
        if name == "set_rule_params":
            sym = inp["symbol"].upper()
            stocks = {s["symbol"]: s for s in get_stocks()}
            if sym not in stocks:
                return {"error": f"{sym} not in watchlist"}
            cat = stocks[sym]["category"]
            current = get_params(sym, cat)
            changed = {k: v for k, v in (inp.get("params") or {}).items() if k in current}
            current.update(changed)
            save_params(sym, current)
            worker_pool.submit(run_check, [sym])
            return {"success": True, "symbol": sym, "updated": changed}
        if name == "reset_rule_params":
            sym = inp["symbol"].upper()
            reset_params(sym); worker_pool.submit(run_check, [sym])
            return {"success": True, "symbol": sym}
        if name == "update_settings":
            changes = inp.get("changes") or {}
            applied = {}
            for k, v in changes.items():
                if k in SETTINGS_DEFAULTS and k not in SETTINGS_SECRET:
                    set_setting(k, v); applied[k] = str(v)
            return {"success": True, "applied": applied}
        if name == "run_check_now":
            worker_pool.submit(run_check, inp.get("symbols"))
            return {"success": True}
        return {"error": f"unknown tool {name}"}
    except Exception as e:
        return {"error": str(e)}

def _ai_system_prompt():
    lines = []
    for s in get_stocks():
        sym = s["symbol"]
        with state_lock:
            cached = dict(state["results"].get(sym, {}))
        with extended_lock:
            ext = dict(extended_state.get(sym, {}))
        q = cached.get("quote", {})
        trig_rules = [r for r in cached.get("rules", []) if r.get("triggered") and not r.get("disabled")]
        trig = [r["rule_type"] for r in trig_rules]
        ed = ext.get("earnings_date"); din = _days_until(ed)
        earn = f", earnings {ed} ({din}d)" if ed else ""
        lines.append(f"  {sym} [{s['category']}] ${q.get('close','?')}"
                     + (f" — triggered: {', '.join(trig)}" if trig else "")
                     + earn)
        # Backtested evidence for each currently-triggered rule, so the assistant can cite the
        # actual edge instead of speaking generically (see backtest_results/rule_stats.json).
        for r in trig_rules:
            evline = _rule_stats_line(sym, r["rule_type"])
            if evline:
                lines.append(f"    {r['rule_type']}: {evline}")
    watch = "\n".join(lines) if lines else "  (empty)"
    regime = get_market_regime()
    regime_note = (
        "\n\nMarket regime note: SPY is currently below its 200-day SMA (bear regime). The "
        "backtest's calibration window was mostly a bull market, so bounce-watch (SELL-side) "
        "statistics above are less reliable right now — flag this if you cite them for a "
        "bounce-watch signal." if regime == "bear" else ""
    )
    return (
        "You are the built-in assistant for Tripwire, a personal stock-monitoring dashboard. "
        "You help the user understand market moves and manage the app.\n\n"
        f"Today is {datetime.now().strftime('%Y-%m-%d')}.\n\n"
        "You can: read the watchlist/alerts/news/settings; add, remove, and recategorize stocks; "
        "adjust or enable/disable any monitoring rule; change app settings; run checks; and use web search for research.\n\n"
        "Rule types: volatility (unusual daily move), support_resistance, consecutive_down, volume (volume spike), "
        "gap (opening gap), rsi (RSI overbought/oversold), ma_cross (moving-average crossover). Each has an enable_* flag "
        "(consecutive_down uses use_consecutive) and numeric thresholds — adjust via set_rule_params.\n\n"
        "Guidelines:\n"
        "- Confirm with the user BEFORE destructive actions (removing a stock, clearing alerts).\n"
        "- For 'why did X move' or narrative questions, use get_stock_detail + get_news + web_search, then synthesize concisely.\n"
        "- When changing rules or settings, state exactly what you changed.\n"
        "- When a triggered rule has backtested evidence listed below, cite it (the edge, hit rate, "
        "and sample size) rather than describing the rule generically. Mention low-confidence flags.\n"
        "- Be concise and specific. Use plain language a retail investor understands.\n"
        "- You are informational only — never give personalized financial advice or guarantees.\n\n"
        f"Current watchlist:\n{watch}{regime_note}"
    )

def _sse(obj):
    return f"data: {json.dumps(obj)}\n\n"

# Guest AI: read-only tools, a per-login question quota plus a global daily cap, and a private
# in-memory conversation per guest login that never touches the owner's stored chat history.
GUEST_AI_TOOLS = [t for t in AI_TOOLS if t["name"] not in WRITE_TOOLS]
_guest_ai_lock = threading.Lock()
_guest_ai_day = {"date": None, "count": 0}
_guest_convos = {}  # gid -> {"ts": last_activity, "messages": [{"role","content"}]}

def guest_ai_left():
    used = session.get("ai_used", 0)
    today = datetime.now().strftime("%Y-%m-%d")
    with _guest_ai_lock:
        day_used = _guest_ai_day["count"] if _guest_ai_day["date"] == today else 0
    return max(0, min(GUEST_AI_PER_LOGIN - used, GUEST_AI_DAILY_CAP - day_used))

def _guest_ai_consume():
    session["ai_used"] = session.get("ai_used", 0) + 1
    today = datetime.now().strftime("%Y-%m-%d")
    with _guest_ai_lock:
        if _guest_ai_day["date"] != today:
            _guest_ai_day.update(date=today, count=0)
        _guest_ai_day["count"] += 1

def _guest_gid():
    if "gid" not in session:
        session["gid"] = secrets.token_hex(8)
    return session["gid"]

def _guest_convo(gid):
    with _guest_ai_lock:
        return list(_guest_convos.get(gid, {}).get("messages", []))

def _guest_convo_add(gid, role, content):
    now = time.time()
    with _guest_ai_lock:
        for k in [k for k, v in _guest_convos.items() if now - v["ts"] > 86400]:
            del _guest_convos[k]
        c = _guest_convos.setdefault(gid, {"ts": now, "messages": []})
        c["ts"] = now
        c["messages"] = (c["messages"] + [{"role": role, "content": content}])[-10:]

GUEST_AI_NOTE = (
    "\n\nThe person chatting is a GUEST with view-only access, trying out the app. You can only "
    "read data and search the web — you cannot change stocks, rules or settings. If they ask for "
    "a change, explain it isn't available in guest mode (they can add or remove guest tickers "
    "themselves from the Stocks tab). Keep answers short."
)

@app.route("/api/ai/history")
@login_required
def api_ai_history():
    if is_guest():
        return jsonify({"messages": _guest_convo(session.get("gid", ""))})
    return jsonify({"messages": _chat_history(60)})

@app.route("/api/ai/clear", methods=["POST"])
@login_required
def api_ai_clear():
    if is_guest():
        with _guest_ai_lock:
            _guest_convos.pop(session.get("gid", ""), None)
        return jsonify({"success": True})
    with get_db() as conn:
        conn.execute("DELETE FROM chat_messages"); conn.commit()
    return jsonify({"success": True})

@app.route("/api/ai/chat", methods=["POST"])
@login_required
def api_ai_chat():
    if not get_anthropic_key():
        return jsonify({"error": "No Anthropic API key. Add one in the Settings tab (or set ANTHROPIC_API_KEY) to enable the assistant."}), 400
    user_msg = (request.json or {}).get("message", "").strip()
    if not user_msg:
        return jsonify({"error": "empty message"}), 400

    guest = is_guest()
    if guest:
        if guest_ai_left() <= 0:
            return jsonify({"error": f"You've used all {GUEST_AI_PER_LOGIN} guest questions — ask the owner if you'd like to try more.",
                            "guest_ai_left": 0}), 429
        _guest_ai_consume()
        gid = _guest_gid()
        left_after = guest_ai_left()
        history = _guest_convo(gid)
        _guest_convo_add(gid, "user", user_msg)
    else:
        history = _chat_history(30)
        _chat_save("user", user_msg)

    api_key = get_anthropic_key()
    def generate():
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        model = get_setting("ai_assistant_model", "claude-opus-4-8")
        system = _ai_system_prompt() + (GUEST_AI_NOTE if guest else "")
        tools = (GUEST_AI_TOOLS if guest else AI_TOOLS) + [{"type": "web_search_20260209", "name": "web_search"}]
        messages = history + [{"role": "user", "content": user_msg}]
        assistant_text_parts = []
        wrote = False
        try:
            for _ in range(6 if guest else 12):  # cap tool-use rounds
                with client.messages.stream(
                    model=model, max_tokens=4000, system=system,
                    tools=tools, messages=messages,
                    thinking={"type": "adaptive"},
                ) as stream:
                    for event in stream:
                        if event.type == "content_block_delta" and getattr(event.delta, "type", "") == "text_delta":
                            txt = event.delta.text
                            assistant_text_parts.append(txt)
                            yield _sse({"type": "text", "text": txt})
                    final = stream.get_final_message()
                messages.append({"role": "assistant", "content": final.content})
                if final.stop_reason == "tool_use":
                    tool_results = []
                    for block in final.content:
                        if block.type == "tool_use":
                            yield _sse({"type": "tool", "label": _tool_label(block.name, block.input)})
                            if guest and block.name in WRITE_TOOLS:
                                result = {"error": "Not available in guest mode"}
                            else:
                                result = execute_ai_tool(block.name, block.input)
                            if block.name in WRITE_TOOLS and not result.get("error"):
                                wrote = True
                            tool_results.append({"type": "tool_result", "tool_use_id": block.id,
                                                 "content": json.dumps(result)})
                    messages.append({"role": "user", "content": tool_results})
                    if assistant_text_parts and not assistant_text_parts[-1].endswith("\n"):
                        assistant_text_parts.append("\n\n")
                        yield _sse({"type": "text", "text": "\n\n"})
                    continue
                break
            full = "".join(assistant_text_parts).strip()
            if full:
                if guest:
                    _guest_convo_add(gid, "assistant", full)
                else:
                    _chat_save("assistant", full)
            done = {"type": "done", "refresh": wrote}
            if guest:
                done["guest_ai_left"] = left_after
            yield _sse(done)
        except Exception as e:
            log.warning("ai_chat failed: %s", e)
            yield _sse({"type": "error", "error": str(e)})

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tripwire</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0A0C12;color:#E4E0D8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh}
button{cursor:pointer;border:none;outline:none}
input,select{outline:none}

/* Top bar */
#topbar{background:#12151F;border-bottom:1px solid #1E2235;padding:0 20px;height:52px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;gap:12px;flex-wrap:wrap}
#logo{font-weight:800;font-size:17px;color:#F59E0B;letter-spacing:-0.5px}
#topbar-right{display:flex;align-items:center;gap:12px;font-size:13px;color:#6B7280}
#status-dot{width:8px;height:8px;border-radius:50%;background:#10B981;display:inline-block;margin-right:4px;transition:background .3s}
#status-dot.checking{background:#F59E0B}
#btn-check{background:#F59E0B;color:#000;border-radius:6px;padding:7px 16px;font-weight:700;font-size:13px}
#btn-check:disabled{opacity:0.5;cursor:not-allowed}

/* Action-window banner */
#window-banner{background:#F59E0B15;border-bottom:1px solid #F59E0B44;color:#F59E0B;
  padding:9px 20px;font-size:13px;font-weight:600;text-align:center;line-height:1.5}
#window-banner strong{font-weight:800}

/* Tabs */
/* Tabs — sticky under the (also sticky) topbar so the menu ribbon never scrolls away */
#tabs{display:flex;border-bottom:1px solid #1E2235;background:#12151F;padding:0 20px;position:sticky;top:52px;z-index:99}
.tab{padding:12px 18px;font-size:14px;color:#6B7280;border-bottom:2px solid transparent;cursor:pointer;background:none;border-left:none;border-right:none;border-top:none}
.tab.active{color:#F59E0B;border-bottom-color:#F59E0B;font-weight:700}

/* Content */
#content{padding:20px;max-width:1440px;margin:0 auto}

/* Add ticker */
/* Deliberately subtle "ghost" affordance — a dashed border and muted colors so it recedes
   next to the stock grid, but still a clearly-labeled, always-visible, one-click target
   (below the grid, not hidden behind an extra click) so it stays easy to find. */
#add-bar{background:transparent;border:1px dashed #2A2D3E;border-radius:8px;padding:7px 10px;margin-top:14px;display:flex;gap:7px;align-items:center;flex-wrap:wrap;opacity:.8;transition:opacity .15s,border-color .15s;max-width:fit-content}
#add-bar:hover,#add-bar:focus-within{opacity:1;border-color:#4B5563}
#add-bar-label{font-size:12px;color:#6B7280;font-weight:600}
#add-bar input{background:#0A0C12;border:1px solid #1E2235;color:#E4E0D8;border-radius:5px;padding:5px 9px;font-size:12px;width:90px}
#add-bar select{background:#0A0C12;border:1px solid #1E2235;color:#E4E0D8;border-radius:5px;padding:5px 9px;font-size:12px}
#btn-add{background:#10B981;color:#fff;border-radius:5px;padding:5px 12px;font-weight:700;font-size:12px}
#add-status{font-size:12px;color:#6B7280}
#add-status.err{color:#EF4444}
#add-status.ok{color:#10B981}

/* "Today" triage strip — what needs attention now, above the grid */
#triage-strip{margin-bottom:14px}
.triage-box{background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:12px 14px}
.today-head{font-size:17px;font-weight:800;margin-bottom:6px}
.today-head.calm{color:#10B981}
.today-head.needs{color:#F59E0B}
.today-item{display:flex;align-items:center;flex-wrap:wrap;gap:8px;padding:8px 0;border-top:1px solid #1E223599}
.today-sym{font-weight:800;font-size:15px;cursor:pointer;text-decoration:underline dotted #6B7280}
.today-meta{color:#9CA3AF;font-size:12px}
.today-actions{margin-left:auto;display:flex;gap:6px}
.dec-btn{border-radius:8px;padding:5px 10px;font-size:12px;font-weight:700;cursor:pointer;border:1px solid}
.dec-btn.acted{background:#05966922;color:#34D399;border-color:#05966966}
.dec-btn.passed{background:transparent;color:#9CA3AF;border-color:#374151}
.dec-tag{font-size:11px;font-weight:700;border-radius:6px;padding:1px 6px}
.dec-tag.acted{background:#05966922;color:#34D399}
.dec-tag.passed{background:#37415155;color:#9CA3AF}
.today-sub{color:#6B7280;font-size:12px;margin-top:6px}
.today-more{color:#93C5FD;text-decoration:none}
.today-detail{margin-top:10px;padding-top:10px;border-top:1px dashed #1E2235}
.perf-money{font-size:28px;font-weight:800;margin:2px 0 6px}
@media (max-width:600px){ .today-actions{margin-left:0;width:100%} .today-actions .dec-btn{flex:1;padding:8px} }
.triage-row{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px}
.triage-row:last-child{margin-bottom:0}
.triage-label{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#6B7280;min-width:96px}
.triage-chip{display:inline-flex;align-items:center;gap:6px;border-radius:8px;padding:4px 10px;cursor:pointer;font-size:13px;font-weight:700;border:1px solid transparent}
.triage-chip .tsym{font-weight:800}
.triage-chip.buy{color:#10B981;background:#10B98115;border-color:#10B98144}
.triage-chip.watch{color:#F59E0B;background:#F59E0B15;border-color:#F59E0B44}
.triage-chip.trend{color:#9CA3AF;background:#37415118;border-color:#37415144}
.triage-chip .tedge{font-weight:600;opacity:.85;font-size:12px}
.triage-empty{color:#6B7280;font-size:13px}
.triage-info{color:#6B7280;font-size:12px}

/* Stock grid — compact cards sized to fit several per row and each within a laptop viewport */
#stock-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:12px;align-items:start}
.stock-card{background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:12px 14px;cursor:pointer;transition:border-color .15s,box-shadow .15s;position:relative}
.stock-card:hover{border-color:#374151;box-shadow:0 4px 20px #00000044}
.stock-card.selected{border-color:#F59E0B;box-shadow:0 0 0 1px #F59E0B44}
.stock-card.alert{border-color:#F59E0B55}
.alert-dot{position:absolute;top:10px;right:10px;width:7px;height:7px;border-radius:50%;background:#F59E0B;box-shadow:0 0 6px #F59E0B}
.card-top{display:flex;justify-content:space-between;align-items:baseline;gap:8px}
.stock-symbol{font-weight:800;font-size:16px;letter-spacing:-.3px}
.stock-cat{font-size:11px;color:#6B7280;text-transform:uppercase;letter-spacing:.5px}
.stock-price{font-size:20px;font-weight:800;letter-spacing:-1px}
.stock-pct{font-size:13px;font-weight:600}
.card-priceline{display:flex;justify-content:space-between;align-items:baseline;gap:8px;margin-top:2px}
.stock-ext{font-size:11px;margin-top:3px;display:flex;gap:8px}
.stock-time{font-size:11px;color:#4B5563;margin-top:3px}
.stock-err{font-size:13px;color:#EF4444;margin-top:8px}
.up{color:#10B981}.dn{color:#EF4444}.muted{color:#6B7280}
.pre-clr{color:#A78BFA}.post-clr{color:#60A5FA}
.remove-btn{position:absolute;top:8px;left:8px;background:#1A1D27;border:1px solid #2A2D3E;color:#6B7280;border-radius:4px;font-size:12px;padding:1px 5px;display:none;z-index:2}
.stock-card:hover .remove-btn{display:block}

/* Compact per-card rule status: one small chip per rule instead of a tall labeled list. */
.card-signal-row{margin-top:8px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
/* A tile with an open signal wears the signal's colour, so it matches its banner row at a glance */
.stock-card.sig-open.buy{border:2px solid #10B98199;padding:11px 13px}
.stock-card.sig-open.watch{border:2px solid #F59E0B99;padding:11px 13px}
.stock-card.sig-open.decided{border-style:dashed}
.stock-card .sig-badge{font-size:12.5px;padding:2px 8px;letter-spacing:.3px}
.card-sig-meta{font-size:12px;color:#9CA3AF;font-weight:600}
.card-sig-meta.due{color:#FCD34D}
.lean-pill{display:inline-block;font-size:11px;font-weight:700;border-radius:6px;padding:1px 7px;margin-top:6px;border:1px dashed}
.lean-pill.buy{color:#6EE7B7;border-color:#10B98144}
.lean-pill.watch{color:#FCD34D;border-color:#F59E0B44}
.dec-state{font-size:12px;font-weight:700;border-radius:8px;padding:4px 9px}
.dec-state.acted{color:#34D399;background:#05966918}
.dec-state.passed{color:#9CA3AF;background:#37415133}
.alerts-sec{font-size:13px;font-weight:800;letter-spacing:.5px;text-transform:uppercase;color:#9CA3AF;margin:18px 0 8px}
.alerts-sec:first-child{margin-top:0}
.sig-card{border-width:2px;scroll-margin-top:130px}
.sig-card.buy{border-color:#10B98166}
.sig-card.watch{border-color:#F59E0B66}
.sig-card .today-item{border-top:none;padding:12px 18px}
.sig-card-why{padding:0 18px 12px;font-size:13px;color:#D1D5DB}
.sig-card.flash{box-shadow:0 0 0 3px #93C5FD88;transition:box-shadow .3s}
.alert-group-latest{font-size:12px;color:#6B7280}
.rule-chips{margin-top:8px;display:flex;flex-wrap:wrap;gap:4px}
.rule-chip{font-size:10px;font-weight:700;letter-spacing:.2px;border-radius:5px;padding:2px 6px;border:1px solid transparent;white-space:nowrap}
.rule-chip.ok{color:#10B981;background:#10B9810F;border-color:#10B98122}
.rule-chip.trig{color:#F59E0B;background:#F59E0B1A;border-color:#F59E0B44}
.rule-chip.off{color:#4B5563;background:#37415112;border-color:#37415133}
.card-status-hdr{font-size:11px;font-weight:700;letter-spacing:.4px;text-transform:uppercase;margin-top:9px}

/* Info-tier "activity" line — deliberately subordinate to .card-triggered (muted, small,
   no border/background emphasis) since it carries no BUY/SELL claim, just awareness. */
.info-tier-line{margin-top:8px;font-size:14px;color:#6B7280;line-height:1.4}

/* Detail panel */
#detail{background:#12151F;border:1px solid #1E2235;border-radius:14px;padding:22px;margin-bottom:16px}
#detail-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:18px}
#detail-title{font-size:24px;font-weight:800;letter-spacing:-1px}
#detail-meta{font-size:12px;color:#6B7280;margin-top:3px}
#btn-close{background:#1E2235;color:#E4E0D8;border-radius:6px;padding:7px 14px;font-size:13px}

/* Sensitivity dial */
.sens-wrap{background:#0A0C12;border:1px solid #1E2235;border-radius:10px;padding:12px 14px;margin-bottom:16px}
.sens-label{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#6B7280;margin-bottom:8px}
.sens-seg{display:inline-flex;border:1px solid #1E2235;border-radius:8px;overflow:hidden}
.sens-btn{background:#12151F;color:#9CA3AF;border:none;padding:7px 16px;font-size:13px;font-weight:700;cursor:pointer;border-right:1px solid #1E2235}
.sens-btn:last-child{border-right:none}
.sens-btn.active{background:#F59E0B;color:#000}
.sens-desc{font-size:12px;color:#6B7280;margin-top:7px}

/* Detail price chart (multi-timeframe) */
#detail-chart{margin-bottom:16px}
.chart-range-row{display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap}
.chart-range-btn{font-size:12px;font-weight:700;color:#9CA3AF;background:#0A0C12;border:1px solid #1E2235;border-radius:6px;padding:4px 12px;cursor:pointer}
.chart-range-btn.active{color:#F59E0B;border-color:#F59E0B66;background:#F59E0B12}
.chart-box{background:#0A0C12;border:1px solid #1E2235;border-radius:10px;padding:10px}
.chart-meta{display:flex;justify-content:space-between;font-size:12px;color:#6B7280;margin-bottom:4px}

/* Price grid */
#price-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:8px;margin-bottom:10px}
.price-cell{background:#0A0C12;border:1px solid #1E2235;border-radius:8px;padding:10px 12px}
.price-cell-label{font-size:14px;color:#6B7280;margin-bottom:4px;letter-spacing:.7px;text-transform:uppercase}
.price-cell-val{font-size:16px;font-weight:800;letter-spacing:-.5px}
.price-cell-sub{font-size:17px;color:#6B7280;margin-top:2px}

/* Range grid */
.ranges-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:18px}
.range-cell{background:#0A0C12;border:1px solid #1E2235;border-radius:8px;padding:10px 12px}
.range-cell-label{font-size:14px;color:#6B7280;letter-spacing:.7px;text-transform:uppercase;margin-bottom:6px}
.range-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:3px}
.range-row span:first-child{font-size:15px;color:#6B7280}
.range-row span:last-child{font-size:13px;font-weight:700}
.range-bar-wrap{height:4px;background:#1E2235;border-radius:2px;margin-top:6px;position:relative}
.range-bar-fill{height:100%;border-radius:2px}
.range-bar-dot{position:absolute;top:-3px;width:10px;height:10px;border-radius:50%;border:2px solid #0A0C12;transform:translateX(-50%)}

/* Rules */
.section-label{font-size:15px;color:#6B7280;letter-spacing:1px;margin-bottom:10px;font-weight:700;text-transform:uppercase}
.rule-card{background:#0A0C12;border:1px solid #1E2235;border-radius:10px;padding:16px;margin-bottom:10px;transition:border-color .15s}
.rule-card.alert-rule{border-color:#F59E0B55;background:#0D0F14}
.rule-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.rule-title{font-weight:700;font-size:14px}
.rule-header-right{display:flex;gap:8px;align-items:center}
.badge{border-radius:4px;padding:2px 9px;font-size:15px;font-weight:700;letter-spacing:.5px}
.badge-ok{background:#10B98115;color:#10B981;border:1px solid #10B98133}
.badge-alert{background:#F59E0B15;color:#F59E0B;border:1px solid #F59E0B33}
.badge-disabled{background:#6B728015;color:#6B7280;border:1px solid #6B728033}
.rule-desc{font-size:12px;color:#9CA3AF;margin-bottom:6px;line-height:1.5}
.rule-rationale{font-size:17px;color:#6B7280;background:#12151F;border-left:2px solid #374151;padding:7px 10px;border-radius:0 6px 6px 0;margin-bottom:10px;line-height:1.5;font-style:italic}
.rule-params-line{font-size:17px;color:#3B82F6;margin-bottom:10px}
.evidence-line{font-size:15px;color:#60A5FA;background:#3B82F60D;border:1px solid #3B82F62A;border-radius:6px;padding:6px 10px;margin-bottom:10px;line-height:1.5}
.low-conf-badge{display:inline-block;margin-left:8px;font-size:14px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;color:#F59E0B;background:#F59E0B15;border:1px solid #F59E0B44;border-radius:4px;padding:1px 6px}
.bear-caveat{font-size:15px;color:#F87171;background:#EF44440D;border:1px solid #EF44442A;border-radius:6px;padding:6px 10px;margin-top:6px;margin-bottom:10px;line-height:1.5}
.rule-vals{display:grid;grid-template-columns:repeat(auto-fill,minmax(100px,1fr));gap:6px;margin-bottom:10px}
.val-cell{background:#12151F;border:1px solid #1E2235;border-radius:6px;padding:7px 10px}
.val-cell-label{font-size:14px;color:#6B7280;margin-bottom:3px;letter-spacing:.5px;text-transform:uppercase}
.val-cell-val{font-size:14px;font-weight:700}
.rule-msg{font-size:12px;background:#12151F;border-radius:6px;padding:8px 11px;color:#9CA3AF;margin-top:6px}
.rule-msg.alert-msg{color:#F59E0B;background:#F59E0B0A;border:1px solid #F59E0B22}
.edit-toggle{background:#1E2235;color:#E4E0D8;border-radius:4px;padding:3px 10px;font-size:17px}
.edit-panel{background:#12151F;border:1px solid #1E2235;border-radius:8px;padding:14px;margin-top:10px}
.edit-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.edit-row label{font-size:12px;color:#9CA3AF}
.edit-row input{background:#0A0C12;border:1px solid #1E2235;color:#E4E0D8;border-radius:4px;padding:5px 8px;font-size:12px;width:90px}
.edit-actions{display:flex;gap:8px;margin-top:12px}
.btn-save{background:#F59E0B;color:#000;border-radius:6px;padding:6px 16px;font-size:12px;font-weight:700}
.btn-reset{background:#1E2235;color:#E4E0D8;border-radius:6px;padding:6px 14px;font-size:12px}

/* Alert log */
#alert-pane{padding-bottom:40px}
.alert-group{background:#12151F;border:1px solid #1E2235;border-radius:12px;margin-bottom:12px;overflow:hidden}
.alert-group-hdr{display:flex;align-items:center;gap:12px;padding:14px 18px;cursor:pointer;user-select:none}
.alert-group-hdr:hover{background:#1A1D27}
.alert-group-sym{font-size:16px;font-weight:800;letter-spacing:-.5px}
.alert-group-cnt{font-size:12px;color:#F59E0B;background:#F59E0B15;border:1px solid #F59E0B33;border-radius:10px;padding:1px 9px;font-weight:700}
.alert-group-cat{font-size:17px;color:#6B7280}
.alert-group-chevron{margin-left:auto;color:#6B7280;font-size:12px;transition:transform .2s}
.alert-group-chevron.open{transform:rotate(180deg)}
.alert-entries{display:none;border-top:1px solid #1E2235}
.alert-entries.open{display:block}
.alert-entry{padding:14px 18px;border-bottom:1px solid #0F1117}
.alert-entry:last-child{border-bottom:none}
.alert-entry-header{display:flex;align-items:center;gap:10px;margin-bottom:6px}
.alert-entry-time{font-size:17px;color:#4B5563;font-family:monospace}
.alert-entry-rule{font-size:17px;color:#F59E0B;background:#F59E0B0F;border-radius:4px;padding:1px 8px;font-weight:700;text-transform:uppercase;letter-spacing:.4px}
.alert-entry-price{font-size:17px;color:#9CA3AF;margin-left:auto}
.window-chip{font-size:14px;font-weight:700;text-transform:uppercase;letter-spacing:.3px;border-radius:4px;padding:1px 7px}
.window-chip.window-open{background:#3B82F615;color:#60A5FA;border:1px solid #3B82F633}
.window-chip.window-closed{background:#6B728015;color:#6B7280;border:1px solid #6B728033}
.alert-entry.window-expired{opacity:.62}
.alert-entry-msg{font-size:13px;color:#E4E0D8;margin-bottom:6px;font-weight:500}
.alert-entry-detail{font-size:17px;color:#6B7280;line-height:1.6}
.alert-detail-vals{display:flex;gap:12px;flex-wrap:wrap;margin-top:6px}
.alert-detail-val{background:#0A0C12;border-radius:4px;padding:3px 8px;font-size:17px}
.alert-detail-val span:first-child{color:#6B7280}
.alert-detail-val span:last-child{color:#E4E0D8;font-weight:600;margin-left:4px}
.alert-rationale{font-size:17px;color:#6B7280;background:#0A0C12;border-left:2px solid #374151;padding:6px 10px;border-radius:0 4px 4px 0;margin-top:6px;line-height:1.5;font-style:italic}
.news-synthesis{background:#130F1F;border-left:2px solid #7C3AED;padding:8px 12px;border-radius:0 6px 6px 0;margin-top:8px;line-height:1.6}
.news-synthesis-label{font-size:15px;font-weight:700;color:#7C3AED;letter-spacing:0.05em;text-transform:uppercase;margin-bottom:4px}
.news-synthesis-text{font-size:12px;color:#C4B5FD;font-style:italic}
.rule-news-synthesis{background:#130F1F;border-left:2px solid #7C3AED;padding:8px 12px;border-radius:0 6px 6px 0;margin-bottom:10px;line-height:1.6}
.rule-news-label{font-size:15px;font-weight:700;color:#7C3AED;letter-spacing:0.05em;text-transform:uppercase;margin-bottom:4px}
.rule-news-text{font-size:12px;color:#C4B5FD;font-style:italic}

.err-banner{background:#EF444415;border:1px solid #EF4444;color:#EF4444;padding:10px 16px;font-size:13px;margin-bottom:12px;border-radius:8px}
.no-alerts{color:#6B7280;font-size:14px;padding:30px;text-align:center}

/* Signal badges */
.sig-badge{display:inline-block;border-radius:6px;padding:3px 12px;font-size:17px;font-weight:800;letter-spacing:.6px;text-transform:uppercase}
.sig-strong-buy{background:#10B98125;color:#10B981;border:2px solid #10B98166}
/* "Bounce watch" (formerly SELL) is amber, not red — backtesting found downside triggers on
   this watchlist historically bounce within days rather than continue lower; see BACKTESTING.md */
.sig-strong-sell{background:#F59E0B25;color:#F59E0B;border:2px solid #F59E0B66}
.sig-trending-buy{background:#10B98112;color:#10B981;border:1px solid #10B98144}
.sig-trending-sell{background:#F59E0B12;color:#F59E0B;border:1px solid #F59E0B44}
.sig-pending{background:#6B728012;color:#6B7280;border:1px solid #6B728044}
.sig-buy{background:#10B98112;color:#10B981;border:1px solid #10B98133}
.sig-sell{background:#F59E0B12;color:#F59E0B;border:1px solid #F59E0B33}
.sig-neutral{background:#37415112;color:#6B7280;border:1px solid #37415144}
/* one rule's direction — deliberately smaller than a signal badge */
.sig-badge.rule-dir{font-size:12px;padding:1px 7px;letter-spacing:.3px}
.card-signal{margin:8px 0 4px 0}
.group-signal{margin-left:auto;margin-right:8px}

/* Market phase pill */
#market-phase{font-size:17px;font-weight:700;border-radius:10px;padding:2px 9px;letter-spacing:.3px}
.mp-regular{background:#10B98118;color:#10B981;border:1px solid #10B98140}
.mp-pre,.mp-post{background:#F59E0B15;color:#F59E0B;border:1px solid #F59E0B40}
.mp-closed{background:#6B728015;color:#9CA3AF;border:1px solid #6B728040}

/* Bear-regime pill + calibration staleness stamp */
.al-sum{font-size:13px;margin-bottom:4px}
.al-muted{color:#6B7280}
.al-wrap{overflow-x:auto;max-height:480px;overflow-y:auto;border:1px solid #1E2235;border-radius:8px}
.al-table{width:100%;border-collapse:collapse;font-size:12px}
.al-table th{position:sticky;top:0;background:#12151F;text-align:left;color:#9CA3AF;font-weight:600;padding:6px 8px;border-bottom:1px solid #1E2235}
.al-table td{padding:6px 8px;border-bottom:1px solid #1E223566;white-space:nowrap;vertical-align:top}
.al-table tr.al-events td{white-space:normal;background:#0A0C12;line-height:1.7}
.al-toggle{background:none;border:1px solid #3B82F655;color:#93C5FD;border-radius:6px;padding:2px 8px;font-size:11px;cursor:pointer}
.guest-settings-note{background:#3B82F612;border:1px solid #3B82F655;color:#BFDBFE;border-radius:10px;padding:10px 14px;font-size:13px;margin-bottom:14px;line-height:1.5}
button:disabled{opacity:.45;cursor:not-allowed}
.guest-tag{font-size:10px;font-weight:700;color:#93C5FD;background:#3B82F618;border:1px solid #3B82F655;border-radius:6px;padding:1px 5px;margin-left:6px;vertical-align:middle;cursor:help}
#chat-quota{font-size:12px;color:#93C5FD;margin-top:6px}
#guest-pill{font-size:12px;font-weight:700;border-radius:10px;padding:3px 9px;background:#3B82F618;color:#93C5FD;border:1px solid #3B82F655;cursor:help;white-space:nowrap}
#toast{position:fixed;left:50%;bottom:24px;transform:translate(-50%,20px);opacity:0;pointer-events:none;
  background:#1E2235;color:#E4E0D8;border:1px solid #3B82F655;border-radius:10px;padding:10px 16px;font-size:13px;
  box-shadow:0 8px 24px #0008;z-index:1000;transition:opacity .2s,transform .2s;max-width:calc(100vw - 32px);text-align:center}
#toast.show{opacity:1;transform:translate(-50%,0)}
#regime-pill{font-size:17px;font-weight:800;border-radius:10px;padding:2px 9px;letter-spacing:.3px;background:#EF444418;color:#F87171;border:1px solid #EF444444}
#calib-stamp.calib-stale{color:#F87171;font-weight:800}

/* Earnings chip on cards */
.earnings-chip{display:inline-block;font-size:15px;font-weight:700;border-radius:4px;padding:1px 7px;margin-top:5px;background:#3B82F615;color:#60A5FA;border:1px solid #3B82F633}
.earnings-chip.soon{background:#F59E0B15;color:#F59E0B;border-color:#F59E0B44}

/* Rule enable toggle in edit panel */
.rule-toggle-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;padding-bottom:10px;border-bottom:1px solid #1E2235}
.rule-toggle-row label{font-size:12px;color:#E4E0D8;font-weight:600}
.switch{position:relative;display:inline-block;width:38px;height:20px}
.switch input{opacity:0;width:0;height:0}
.slider{position:absolute;cursor:pointer;inset:0;background:#374151;border-radius:20px;transition:.2s}
.slider:before{content:"";position:absolute;height:14px;width:14px;left:3px;bottom:3px;background:#E4E0D8;border-radius:50%;transition:.2s}
.switch input:checked+.slider{background:#10B981}
.switch input:checked+.slider:before{transform:translateX(18px)}

/* Settings tab */
#pane-settings{max-width:640px;padding-bottom:60px}
.settings-group{background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:18px 20px;margin-bottom:16px}
.settings-group h3{font-size:13px;color:#F59E0B;margin-bottom:14px;letter-spacing:.5px;text-transform:uppercase}
.set-row{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px}
.set-row label{font-size:13px;color:#9CA3AF;flex:1}
/* .switch is also a <label> (wraps the hidden checkbox) — without this override, the
   ".set-row label{flex:1}" rule above stretches it full-width into a long bar instead of
   the compact pill it's styled to be. */
.set-row label.switch{flex:none}
.set-row input[type=text],.set-row input[type=number],.set-row input[type=password],.set-row select{background:#0A0C12;border:1px solid #1E2235;color:#E4E0D8;border-radius:6px;padding:7px 10px;font-size:13px;width:200px;max-width:55%}
.set-hint{font-size:17px;color:#6B7280;margin:-6px 0 12px 0;line-height:1.5}
.settings-actions{display:flex;gap:10px;align-items:center;margin-top:8px}
#btn-save-settings{background:#F59E0B;color:#000;border-radius:7px;padding:9px 20px;font-weight:700;font-size:13px}
#btn-test-notify{background:#1E2235;color:#E4E0D8;border-radius:7px;padding:9px 16px;font-size:13px}
#settings-status{font-size:12px;color:#10B981}

/* Analytics tab */
#pane-analytics{padding-bottom:60px}
#pane-portfolio{padding-bottom:60px}
.pf-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;align-items:start}
@media (max-width:900px){ .pf-grid{grid-template-columns:1fr} }
.pf-col{background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:18px 20px;min-width:0}
.pf-kind{font-size:12px;font-weight:800;letter-spacing:.5px;text-transform:uppercase;color:#9CA3AF}
.pf-period{font-size:13px;color:#6B7280;margin:2px 0 12px}
.pf-big{font-size:26px;font-weight:800;line-height:1.2}
.pf-sub{font-size:14px;margin:4px 0 14px}
.pf-rows{width:100%;border-collapse:collapse;font-size:13px;margin-bottom:10px}
.pf-rows td{padding:6px 4px;border-bottom:1px solid #1E223588}
.pf-rows td.r{text-align:right;white-space:nowrap}
.pf-dot{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:6px;vertical-align:middle}
.pf-verdict{border-radius:10px;padding:10px 12px;font-size:14px;font-weight:600;margin:6px 0 12px}
.pf-verdict.good{background:#05966918;color:#34D399;border:1px solid #05966955}
.pf-verdict.bad{background:#DC262618;color:#F87171;border:1px solid #DC262655}
.pf-verdict.flat{background:#37415133;color:#D1D5DB;border:1px solid #374151}
.pf-fair{font-size:12px;color:#BFDBFE;background:#3B82F612;border:1px solid #3B82F644;border-radius:8px;padding:8px 10px;margin-bottom:12px}
.pf-chart svg{width:100%;height:auto;display:block}
.pf-legend{font-size:11px;color:#9CA3AF;margin:4px 0 12px}
.pf-movers{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:8px 0}
.pf-movers h4{font-size:11px;color:#9CA3AF;text-transform:uppercase;letter-spacing:.4px;margin-bottom:4px}
.pf-movers div div{font-size:13px;padding:2px 0}
.pf-col details summary{cursor:pointer;color:#93C5FD;font-size:12px;margin:8px 0}
.pf-note{font-size:12px;color:#6B7280;margin-top:10px;line-height:1.5}

/* Glossary */
#pane-glossary{max-width:820px;padding-bottom:80px}
.gloss-link{color:#60A5FA;cursor:pointer;text-decoration:underline;text-decoration-style:dotted;text-underline-offset:2px}
.gloss-link:hover{color:#93C5FD}
.gloss-entry{background:#12151F;border:1px solid #1E2235;border-radius:10px;padding:14px 16px;margin-bottom:10px;scroll-margin-top:110px;transition:border-color .3s,background .3s}
.gloss-entry.gloss-flash{border-color:#F59E0B;background:#F59E0B10}
.gloss-term{font-size:16px;font-weight:800;color:#F59E0B;margin-bottom:5px}
.gloss-def{font-size:14px;color:#C9C6BE;line-height:1.6}
.gloss-def b{color:#E4E0D8}
.analytics-card{background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:18px 20px;margin-bottom:16px}
.analytics-card h3{font-size:13px;color:#F59E0B;margin-bottom:6px;letter-spacing:.5px;text-transform:uppercase}
/* Performance scorecard */
.perf-title{font-size:20px;font-weight:800;margin-bottom:2px}
.perf-headline{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:14px}
.perf-hero{flex:1;min-width:220px;background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:18px 20px}
.perf-hero-num{font-size:38px;font-weight:800;letter-spacing:-1.5px;line-height:1}
.perf-hero-lbl{font-size:13px;color:#9CA3AF;margin-top:6px;line-height:1.4}
.perf-drift{background:#F59E0B12;border:1px solid #F59E0B44;color:#F59E0B;border-radius:10px;padding:12px 16px;font-size:13px;margin-bottom:16px;line-height:1.5}
.perf-ok{background:#10B98110;border:1px solid #10B98133;color:#10B981;border-radius:10px;padding:10px 16px;font-size:13px;margin-bottom:16px}
.rt-buy{background:#10B98120;color:#10B981;border-radius:4px;padding:1px 6px;font-size:10px;font-weight:700}
.rt-watch{background:#F59E0B20;color:#F59E0B;border-radius:4px;padding:1px 6px;font-size:10px;font-weight:700}
.analytics-summary{display:flex;gap:20px;flex-wrap:wrap;margin-bottom:16px}
.an-stat{background:#12151F;border:1px solid #1E2235;border-radius:10px;padding:12px 18px}
.an-stat-num{font-size:22px;font-weight:800}
.outcome-table{width:100%;border-collapse:collapse;font-size:13px}
.outcome-table th{text-align:right;color:#9CA3AF;font-weight:700;font-size:11px;text-transform:uppercase;letter-spacing:.4px;padding:6px 8px;border-bottom:1px solid #1E2235}
.outcome-table td{text-align:right;padding:7px 8px;border-bottom:1px solid #12151F}
.an-stat-lbl{font-size:17px;color:#6B7280;text-transform:uppercase;letter-spacing:.5px}
.abar-row{display:flex;align-items:center;gap:10px;margin-bottom:7px}
.abar-label{font-size:12px;color:#9CA3AF;width:110px;text-align:right;flex-shrink:0}
.abar-track{flex:1;background:#0A0C12;border-radius:4px;height:18px;overflow:hidden}
.abar-fill{height:100%;background:#F59E0B;border-radius:4px}
.abar-val{font-size:12px;color:#E4E0D8;font-weight:700;width:34px}

/* Assistant tab */
#pane-assistant{display:flex;flex-direction:column;height:calc(100vh - 160px);max-width:820px}
#chat-scroll{flex:1;overflow-y:auto;padding:8px 2px 16px}
.chat-msg{margin-bottom:14px;display:flex}
.chat-msg.user{justify-content:flex-end}
.chat-bubble{max-width:82%;padding:11px 15px;border-radius:14px;font-size:14px;line-height:1.6;white-space:pre-wrap;word-wrap:break-word}
.chat-msg.user .chat-bubble{background:#F59E0B;color:#000;border-bottom-right-radius:4px}
.chat-msg.assistant .chat-bubble{background:#12151F;border:1px solid #1E2235;color:#E4E0D8;border-bottom-left-radius:4px}
.chat-bubble strong{color:#F59E0B;font-weight:700}
.chat-bubble code{background:#0A0C12;border:1px solid #1E2235;border-radius:4px;padding:1px 5px;font-size:12px}
.chat-tool{font-size:12px;color:#7C3AED;background:#130F1F;border:1px solid #7C3AED33;border-radius:8px;padding:5px 12px;margin-bottom:10px;display:inline-block}
.chat-suggestions{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px}
.chat-chip{background:#12151F;border:1px solid #1E2235;color:#9CA3AF;border-radius:16px;padding:7px 14px;font-size:12px;cursor:pointer}
.chat-chip:hover{border-color:#F59E0B55;color:#E4E0D8}
#chat-input-bar{display:flex;gap:8px;padding-top:10px;border-top:1px solid #1E2235}
#chat-input{flex:1;background:#0A0C12;border:1px solid #1E2235;color:#E4E0D8;border-radius:10px;padding:11px 14px;font-size:14px;resize:none;font-family:inherit}
#chat-send{background:#F59E0B;color:#000;border-radius:10px;padding:0 20px;font-weight:700;font-size:14px}
#chat-send:disabled{opacity:.5;cursor:not-allowed}
#chat-new{background:none;color:#6B7280;font-size:12px;text-decoration:underline;padding:4px}
.ai-disabled{color:#9CA3AF;font-size:14px;line-height:1.7;background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:24px}
.ai-disabled code{background:#0A0C12;border-radius:4px;padding:2px 6px;font-size:12px}

@media(max-width:600px){
  #topbar{flex-direction:column;height:auto;padding:10px 14px;gap:6px;align-items:stretch}
  #topbar-right{justify-content:space-between;width:100%;flex-wrap:wrap}
  #btn-check{padding:9px 16px;font-size:14px}
  #tabs{padding:0 10px;overflow-x:auto}
  .tab{padding:12px 14px;font-size:14px;white-space:nowrap}
  #content{padding:10px}
  #add-bar{flex-direction:column;align-items:stretch}
  #add-bar input,#add-bar select,#btn-add{width:100%}
  #stock-grid{grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:8px}
  .stock-card{padding:11px}
  #detail{padding:14px;border-radius:10px}
  #detail-header{flex-direction:column;gap:10px}
  #price-grid{grid-template-columns:repeat(2,1fr)}
  .ranges-grid{grid-template-columns:1fr}
  .rule-vals{grid-template-columns:repeat(2,1fr)}
  .edit-row input{width:110px}
  button,.tab,.remove-btn,.edit-toggle,.btn-save,.btn-reset{min-height:30px}
  .set-row{flex-direction:column;align-items:stretch;gap:5px}
  .set-row input[type=text],.set-row input[type=number],.set-row input[type=password],.set-row select{width:100%;max-width:100%}
  #pane-assistant{height:calc(100vh - 200px)}
  .abar-label{width:80px}
}
</style>
</head>
<body>

<div id="topbar">
  <div id="logo">⚡ Tripwire</div>
  <div id="topbar-right">
    <span id="market-phase" class="mp-closed">—</span>
    <span id="guest-pill" style="display:none">👀 Guest · view only</span>
    <span id="regime-pill" class="regime-bear" style="display:none">🐻 BEAR REGIME</span>
    <span><span id="status-dot"></span><span id="status-txt">Connecting...</span></span>
    <span id="last-check-txt"></span>
    <button id="btn-check" onclick="manualCheck()">▶ Check Now</button>
    <a href="/logout" style="color:#6B7280;font-size:13px;text-decoration:none">Logout</a>
  </div>
</div>

<div id="tabs">
  <button class="tab active" onclick="switchTab('stocks',this)">Stocks</button>
  <button class="tab" onclick="switchTab('alerts',this)" id="tab-alerts-btn">Alerts</button>
  <button class="tab" onclick="switchTab('assistant',this)">🤖 Assistant</button>
  <button class="tab" onclick="switchTab('analytics',this)">📊 Performance</button>
  <button class="tab" onclick="switchTab('portfolio',this)">💼 Portfolio test</button>
  <button class="tab" onclick="switchTab('settings',this)">Settings</button>
  <button class="tab" onclick="switchTab('glossary',this)" id="tab-glossary-btn">📖 Glossary</button>
</div>

<div id="toast" role="status" aria-live="polite"></div>

<div id="window-banner">⏱ <strong>Act within ~4 trading days.</strong> Rule thresholds are calibrated from a backtested 1–5 day edge — signals lose their statistical validity beyond that window.<span id="calib-stamp"></span></div>

<div id="content">
  <div id="err-banner" class="err-banner" style="display:none"></div>

  <div id="pane-stocks">
    <div id="triage-strip"></div>
    <div id="stock-grid"></div>
    <div id="add-bar">
      <span id="add-bar-label">+ Add ticker</span>
      <input id="inp-symbol" placeholder="e.g. TSLA" maxlength="10"
             onkeydown="if(event.key==='Enter')addStock()">
      <select id="inp-cat" title="Sets the alert-sensitivity profile for this stock — how big a move, gap, etc. has to be before a rule triggers. Volatile stocks (e.g. small caps, momentum names) need looser thresholds than a low-vol utility or blue chip, or they'd alert constantly. You can fine-tune this per stock later.">
        <option value="high_vol">High Vol</option>
        <option value="mod_vol">Moderate Vol</option>
        <option value="low_vol">Low Vol</option>
      </select>
      <button id="btn-add" onclick="addStock()">Add</button>
      <span id="add-status"></span>
    </div>
    <div id="detail" style="display:none"></div>
  </div>

  <div id="pane-alerts" style="display:none">
    <div id="alert-toolbar" style="display:flex;gap:10px;align-items:center;margin-bottom:12px">
      <button class="btn-reset" onclick="ackAllAlerts()">✓ Acknowledge all</button>
      <button class="btn-reset" onclick="clearAllAlerts()">🗑 Clear all</button>
      <a class="btn-reset" href="/api/alerts/export.csv" style="text-decoration:none">⬇ Export CSV</a>
    </div>
    <div id="alert-pane"></div>
  </div>

  <div id="pane-assistant" style="display:none">
    <div id="chat-scroll"></div>
    <div id="chat-input-bar">
      <textarea id="chat-input" rows="1" placeholder="Ask about a move, adjust a rule, change a setting…"
                onkeydown="chatKey(event)"></textarea>
      <button id="chat-send" onclick="sendChat()">Send</button>
    </div>
    <div style="text-align:right"><button id="chat-new" onclick="clearChat()">New conversation</button></div>
    <div id="chat-quota" style="display:none"></div>
  </div>

  <div id="pane-analytics" style="display:none"></div>

  <div id="pane-portfolio" style="display:none"></div>

  <div id="pane-settings" style="display:none"></div>

  <div id="pane-glossary" style="display:none"></div>
</div>

<script>
let stocks=[], alerts=[], selectedSym=null, checking=false;

// ── SVG Charts ────────────────────────────────────────────────────────────────

function chartVolatility(historyClosed, threshold, currentPct) {
  if (!historyClosed || historyClosed.length < 3) return '';
  const changes = [];
  for (let i = 1; i < historyClosed.length; i++) {
    const b = historyClosed[i-1], a = historyClosed[i];
    if (b > 0) changes.push({pct: Math.abs((a-b)/b)*100, up: a >= b});
  }
  const last = changes.slice(-20);
  if (last.length < 2) return '';
  const W=300, H=72, pL=6, pR=6, pT=8, pB=18;
  const plotW=W-pL-pR, plotH=H-pT-pB;
  const maxV = Math.max(...last.map(c=>c.pct), threshold||0, 0.01)*1.2;
  const bW = plotW/last.length - 1;
  const bars = last.map((c,i)=>{
    const x = pL + i*(plotW/last.length);
    const h = Math.max(2,(c.pct/maxV)*plotH);
    const y = pT+plotH-h;
    const isLast = i===last.length-1;
    const fill = isLast ? '#F59E0B' : (c.up ? '#10B98155' : '#EF444455');
    return `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${bW.toFixed(1)}" height="${h.toFixed(1)}" fill="${fill}" rx="1"/>`;
  }).join('');
  let tLine='';
  if (threshold) {
    const ty = pT+plotH-(threshold/maxV)*plotH;
    tLine=`<line x1="${pL}" y1="${ty.toFixed(1)}" x2="${W-pR}" y2="${ty.toFixed(1)}" stroke="#F59E0B" stroke-width="1.5" stroke-dasharray="4,3"/>
    <text x="${W-pR-2}" y="${Math.max(pT+8,ty-3).toFixed(1)}" fill="#F59E0B" font-size="8" text-anchor="end">threshold ${threshold.toFixed(1)}%</text>`;
  }
  const xL=`<text x="${pL}" y="${H-3}" fill="#374151" font-size="8">← ${last.length} days</text><text x="${W-pR}" y="${H-3}" fill="#F59E0B" font-size="8" text-anchor="end">today →</text>`;
  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:${H}px;display:block;margin-top:10px">${bars}${tLine}${xL}</svg>`;
}

function chartSR(support, resistance, current, pLow, pHigh) {
  if (!support || !resistance || !current) return '';
  const W=300, H=60;
  const lo = Math.min(pLow||support, support)*0.995;
  const hi = Math.max(pHigh||resistance, resistance)*1.005;
  const range = hi - lo || 1;
  const toX = v => 10 + ((v-lo)/range)*280;
  const supX=toX(support), resX=toX(resistance), curX=toX(current);
  const below=current<support, above=current>resistance;
  const zones=`
    <rect x="10" y="22" width="${Math.max(0,supX-10).toFixed(1)}" height="16" fill="#EF444420" rx="2"/>
    <rect x="${supX.toFixed(1)}" y="22" width="${Math.max(0,resX-supX).toFixed(1)}" height="16" fill="#10B98120" rx="2"/>
    <rect x="${resX.toFixed(1)}" y="22" width="${Math.max(0,290-resX).toFixed(1)}" height="16" fill="#EF444420" rx="2"/>
  `;
  const supLbl=`<text x="${supX.toFixed(1)}" y="16" fill="#EF444499" font-size="8" text-anchor="middle">SUP $${support}</text>`;
  const resLbl=`<text x="${resX.toFixed(1)}" y="16" fill="#EF444499" font-size="8" text-anchor="middle">RES $${resistance}</text>`;
  const curClr=below?'#EF4444':above?'#F59E0B':'#10B981';
  const curMk=`<line x1="${curX.toFixed(1)}" y1="18" x2="${curX.toFixed(1)}" y2="42" stroke="${curClr}" stroke-width="2"/>
  <circle cx="${curX.toFixed(1)}" cy="30" r="5" fill="${curClr}"/>
  <text x="${curX.toFixed(1)}" y="54" fill="${curClr}" font-size="8" text-anchor="middle">$${current}</text>`;
  return `<svg viewBox="0 0 300 60" style="width:100%;height:60px;display:block;margin-top:10px">${zones}${supLbl}${resLbl}${curMk}</svg>`;
}

function chartConsec(historyClosed) {
  if (!historyClosed || historyClosed.length < 3) return '';
  const cls = historyClosed.slice(-12);
  const W=300, H=64, pL=8, pR=8, pT=6, pB=18;
  const plotW=W-pL-pR, plotH=H-pT-pB;
  const lo=Math.min(...cls)*0.999, hi=Math.max(...cls)*1.001;
  const range=hi-lo||1;
  const pts=cls.map((v,i)=>({
    x: pL+(i/(cls.length-1))*plotW,
    y: pT+plotH-((v-lo)/range)*plotH
  }));
  const segs=[];
  for(let i=1;i<pts.length;i++){
    const dn=cls[i]<cls[i-1];
    segs.push(`<line x1="${pts[i-1].x.toFixed(1)}" y1="${pts[i-1].y.toFixed(1)}" x2="${pts[i].x.toFixed(1)}" y2="${pts[i].y.toFixed(1)}" stroke="${dn?'#EF4444':'#10B981'}" stroke-width="2"/>`);
  }
  const dots=pts.map((p,i)=>{
    const dn=i>0&&cls[i]<cls[i-1];
    const isLast=i===pts.length-1;
    return `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="${isLast?4.5:2.5}" fill="${dn?'#EF4444':'#10B981'}" ${isLast?'stroke="#0A0C12" stroke-width="1.5"':''}/>`;
  }).join('');
  const xL=`<text x="${pL}" y="${H-3}" fill="#374151" font-size="8">← ${cls.length} days</text><text x="${W-pR}" y="${H-3}" fill="#E4E0D8" font-size="8" text-anchor="end">today</text>`;
  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:${H}px;display:block;margin-top:10px">${segs.join('')}${dots}${xL}</svg>`;
}

function rangeBarHTML(lo, hi, current, label, loLabel, hiLabel, color) {
  if (!lo || !hi) return '';
  const pct = ((current-lo)/(hi-lo)*100).toFixed(1);
  const clampedPct = Math.max(2, Math.min(98, parseFloat(pct)));
  const curClr = parseFloat(pct) < 15 ? '#EF4444' : parseFloat(pct) > 85 ? '#F59E0B' : color||'#10B981';
  return `<div class="range-cell">
    <div class="range-cell-label">${label}</div>
    <div class="range-row"><span>${loLabel}</span><span class="dn">$${lo}</span></div>
    <div class="range-row"><span>${hiLabel}</span><span class="up">$${hi}</span></div>
    <div class="range-bar-wrap">
      <div class="range-bar-fill" style="width:${clampedPct}%;background:${curClr}22;position:relative;height:100%"></div>
      <div class="range-bar-dot" style="left:${clampedPct}%;background:${curClr}"></div>
    </div>
  </div>`;
}

// ── Data loop ─────────────────────────────────────────────────────────────────
async function fetchJSON(url){ const r=await fetch(url); if(r.status===401){location.href='/login';return{};} return r.json(); }
async function postJSON(url,body){
  const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(r.status===401){location.href='/login';return{};}
  if(r.status===403){
    const e=await r.json().catch(()=>({}));
    if(e.guest){
      showToast('👀 '+e.error);
      // Abort the caller so it can't show "saved"/"applying" for a change that never happened.
      throw new Error('guest-blocked');
    }
  }
  return r.json();
}

window.addEventListener('unhandledrejection',e=>{ if(e.reason&&e.reason.message==='guest-blocked') e.preventDefault(); });

let toastTimer=null;
function showToast(msg,ms){
  const t=document.getElementById('toast');
  t.textContent=msg; t.classList.add('show');
  clearTimeout(toastTimer); toastTimer=setTimeout(()=>t.classList.remove('show'),ms||3200);
}

// Guest activity tracking (guests only; the server ignores it for the owner).
function track(text){
  if(!guestMode) return;
  fetch('/api/track',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({d:text}),keepalive:true}).catch(()=>{});
}
const TAB_NAMES={stocks:'Stocks',alerts:'Alerts',assistant:'AI Assistant',analytics:'Performance',portfolio:'Portfolio test',settings:'Settings',glossary:'Glossary'};
function tabBtn(name){ return [...document.querySelectorAll('.tab')].find(b=>(b.getAttribute('onclick')||'').includes("'"+name+"'")); }

let aiEnabled=false, guestMode=false, guestAiLeft=null, guestAiLimit=5, maxAlertId=0, notifPrimed=false, appSettings={};
function renderChatQuota(){
  const q=document.getElementById('chat-quota');
  if(!q) return;
  if(!guestMode){ q.style.display='none'; return; }
  q.style.display='block';
  q.textContent=guestAiLeft>0
    ? `👀 Guest: ${guestAiLeft} of ${guestAiLimit} questions left · read-only (the assistant can look things up but not change anything)`
    : `👀 Guest: you've used all ${guestAiLimit} questions.`;
}

// ── Backtest evidence (fetched once; absent/empty = feature silently off) ──────
let ruleStats={}, ruleStatsLoaded=false;
async function loadRuleStats(){
  if(ruleStatsLoaded) return;
  ruleStatsLoaded=true;
  try{ ruleStats=await fetchJSON('/api/rule-stats')||{}; }catch(e){ ruleStats={}; }
}
function ruleStatsFor(sym,ruleType){
  return (ruleStats[sym]||{})[ruleType]||null;
}
// Compact evidence line, e.g. "Backtested: +3.2% avg vs SPY over 5d · 62% hit · n=41 · worst case -2.8%"
function ruleStatsLine(sym,ruleType){
  const st=ruleStatsFor(sym,ruleType);
  if(!st) return '';
  const parts=[`${st.exc5>=0?'+':''}${st.exc5.toFixed(1)}% avg vs SPY over 5d`];
  if(st.hit5!=null) parts.push(`${Math.round(st.hit5)}% hit`);
  parts.push(`n=${st.n}`);
  if(st.mae!=null) parts.push(`worst case ${st.mae>=0?'+':''}${st.mae.toFixed(1)}%`);
  return `Backtested: ${parts.join(' · ')}`;
}
function lowConfidenceBadge(sym,ruleType){
  const st=ruleStatsFor(sym,ruleType);
  if(!st) return '';
  if(st.short_history||st.n<30) return '<span class="low-conf-badge" title="Short trading history or small sample size">low confidence</span>';
  return '';
}
function evidenceLineHTML(sym,ruleType){
  const line=ruleStatsLine(sym,ruleType);
  if(!line) return '';
  return `<div class="evidence-line">📊 ${linkifyGlossary(line)}${lowConfidenceBadge(sym,ruleType)}</div>`;
}

// Deep links from emails: /?stock=NVDA opens that stock, /?tab=settings opens a tab.
let startParamsHandled=false;
function handleStartParams(){
  if(startParamsHandled) return;
  startParamsHandled=true;
  const p=new URLSearchParams(location.search);
  const tab=p.get('tab'), sym=(p.get('stock')||'').toUpperCase();
  if(tab&&TAB_NAMES[tab]) switchTab(tab,tabBtn(tab));
  if(sym&&stocks.some(s=>s.symbol===sym)&&selectedSym!==sym) selectStock(sym);
  if(tab||sym) history.replaceState(null,'','/');
}

async function loadAll(){
  try{
    const [s,a,st]=await Promise.all([fetchJSON('/api/stocks'),fetchJSON('/api/alerts'),fetchJSON('/api/status')]);
    await loadRuleStats();
    stocks=s; alerts=a.alerts||[];
    renderStatus(st);
    renderGrid();
    renderAlerts();
    checkNewAlertNotifications();
    handleStartParams();
    document.getElementById('err-banner').style.display='none';
  }catch(e){
    document.getElementById('err-banner').textContent='Cannot reach backend. Make sure app.py is running.';
    document.getElementById('err-banner').style.display='block';
  }
}
setInterval(loadAll,5000);
loadAll();

// ── Status ────────────────────────────────────────────────────────────────────
const PHASE_LABEL={regular:'● Market open',pre:'Pre-market',post:'After-hours',closed:'Market closed'};
let currentRegime='bull';
function renderStatus(st){
  checking=st.status==='checking';
  aiEnabled=!!st.ai_enabled;
  const firstGuestStatus=!guestMode&&!!st.guest;
  guestMode=!!st.guest;
  if(guestMode){ guestAiLeft=st.guest_ai_left; guestAiLimit=st.guest_ai_limit||5; renderChatQuota(); }
  if(firstGuestStatus){
    renderGrid();
    showToast('👀 Guest mode — your visit and clicks are recorded so the owner can see what testers try.',7000);
  }
  const gp=document.getElementById('guest-pill');
  if(gp){
    gp.style.display=guestMode?'inline-block':'none';
    gp.title=guestMode?('Guest access'+(st.guest_expires?' — valid until '+st.guest_expires:'')+'. You can open everything and add your own tickers; other changes are not saved. Visits and clicks are recorded for the owner.'):'';
  }
  currentRegime=st.regime||'bull';
  document.getElementById('status-dot').className=checking?'checking':'';
  document.getElementById('status-txt').textContent=checking?'Checking...':'Running';
  document.getElementById('btn-check').disabled=checking;
  const mp=document.getElementById('market-phase');
  if(mp&&st.market_phase){mp.textContent=PHASE_LABEL[st.market_phase]||st.market_phase;mp.className='mp-'+st.market_phase;}
  if(st.last_check){
    const d=new Date(st.last_check);
    document.getElementById('last-check-txt').textContent='Last: '+d.toLocaleTimeString();
  }
  const rp=document.getElementById('regime-pill');
  if(rp) rp.style.display=(currentRegime==='bear')?'inline-block':'none';
  renderCalibStamp(st.calibrated_at);
}
// Mirrors app.py's _bear_regime_caveat(signal) — SELL-side (bounce-watch) only, bear regime only.
function bearRegimeCaveatHTML(signal){
  if(signal!=='SELL'||currentRegime!=='bear') return '';
  return `<div class="bear-caveat">🐻 Caution: the market is currently in a bear regime, but this rule's bounce-watch statistics were calibrated on a mostly bull-market window — treat the backtested edge as less reliable right now.</div>`;
}

// Calibration-date stamp appended to the action-window banner. Turns amber/warning-emphasized
// once the calibration is stale (>90 days) so the user knows the backtest evidence may no
// longer reflect current thresholds/market conditions.
const CALIB_STALE_DAYS=90;
function renderCalibStamp(calibratedAt){
  const el=document.getElementById('calib-stamp');
  if(!el) return;
  if(!calibratedAt){ el.textContent=''; el.className=''; return; }
  const days=Math.floor((Date.now()-new Date(calibratedAt+'T00:00:00').getTime())/86400000);
  const stale=days>CALIB_STALE_DAYS;
  el.textContent=` · calibrated ${calibratedAt}${stale?' (stale — over 90 days old)':''}`;
  el.className=stale?'calib-stale':'';
}

async function manualCheck(){
  document.getElementById('btn-check').disabled=true;
  await postJSON('/api/check',{});
  setTimeout(loadAll,1500);
}

// ── Browser notifications ─────────────────────────────────────────────────────
function alertDetailOf(a){ try{ return a.detail?JSON.parse(a.detail):{}; }catch(e){ return {}; } }

function checkNewAlertNotifications(){
  const ids=alerts.map(a=>a.id);
  const newMax=ids.length?Math.max(...ids):0;
  if(!notifPrimed){ maxAlertId=newMax; notifPrimed=true; return; }  // don't fire on first load
  if(newMax>maxAlertId){
    let fresh=alerts.filter(a=>a.id>maxAlertId);
    // Respect notify_strong_only client-side too, so browser push matches the same STRONG-only
    // gate the server applies to email/WhatsApp — every alert still appears in the Alerts tab
    // regardless; this only filters which ones trigger a push/sound.
    if(appSettings.notify_strong_only!=='0'){
      fresh=fresh.filter(a=>alertDetailOf(a).strong===true);
    }
    if(appSettings.browser_notifications_enabled!=='0' && 'Notification' in window && Notification.permission==='granted'){
      fresh.slice(0,3).forEach(a=>{ try{ new Notification('⚡ '+a.symbol+' alert',{body:a.message}); }catch(e){} });
    }
    if(appSettings.alert_sound_enabled==='1' && fresh.length) beep();
    maxAlertId=newMax;
  }
}
function beep(){
  try{
    const ctx=new (window.AudioContext||window.webkitAudioContext)();
    const o=ctx.createOscillator(), g=ctx.createGain();
    o.connect(g); g.connect(ctx.destination);
    o.frequency.value=880; o.type='sine'; g.gain.value=0.08;
    o.start(); o.stop(ctx.currentTime+0.18);
  }catch(e){}
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function switchTab(name,btn){
  track('Opened tab: '+(TAB_NAMES[name]||name));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  if(btn) btn.classList.add('active');
  ['stocks','alerts','assistant','analytics','portfolio','settings','glossary'].forEach(n=>{
    const el=document.getElementById('pane-'+n);
    if(el) el.style.display=(n===name)?'':'none';
  });
  if(name==='settings') loadSettings();
  if(name==='analytics') loadAnalytics();
  if(name==='portfolio') loadPortfolio();
  if(name==='assistant') loadChatHistory();
  if(name==='glossary') renderGlossary();
}

// Jump to a specific glossary entry from an inline term link anywhere in the app.
function openGlossary(id){
  track('Looked up glossary term: '+id);
  const btn=document.getElementById('tab-glossary-btn');
  switchTab('glossary',btn);
  requestAnimationFrame(()=>{
    const el=document.getElementById('gloss-'+id);
    if(el){
      el.classList.add('gloss-flash');
      const y=el.getBoundingClientRect().top+window.pageYOffset-100;
      window.scrollTo({top:y,behavior:'smooth'});
      setTimeout(()=>el.classList.remove('gloss-flash'),1600);
    }
  });
  return false;
}

// Linkify a curated set of jargon terms in a plain-text string (first occurrence of each
// term only, to avoid clutter). Applied to backend-supplied rule descriptions and evidence
// lines, which are plain text — safe to inject as HTML after this pass.
const GLOSSARY_LINK_RULES=[
  [/\bbounce[- ]?watch\b/i,'bounce'],[/\bbounce\b/i,'bounce'],
  [/\bback-?tested\b/i,'backtested'],[/\bback-?test(?:ing)?\b/i,'backtested'],
  [/\bRSI\b/,'rsi'],
  [/\boverbought\b/i,'obos'],[/\boversold\b/i,'obos'],[/\bOB\/OS\b/i,'obos'],
  [/\bcross-?over\b/i,'crossover'],[/\bgolden cross\b/i,'crossover'],[/\bdeath cross\b/i,'crossover'],
  [/\bgap\b/i,'gap'],
  [/\bexcess return\b/i,'excess'],[/\bvs\.? SPY\b/i,'excess'],
  [/\bhit rate\b/i,'hitrate'],
  [/\bvolatility\b/i,'volatility'],
  [/\bsupport\b/i,'sr'],[/\bresistance\b/i,'sr'],
  [/\bSTRONG\b/,'strong'],
  [/\bvolume\b/i,'volume'],
  [/\balert\b/i,'alert'],
];
function linkifyGlossary(text){
  if(!text) return text;
  let out=String(text); const used=new Set();
  for(const [re,id] of GLOSSARY_LINK_RULES){
    if(used.has(id)) continue;
    let hit=false;
    out=out.replace(re,m=>{if(hit)return m;hit=true;used.add(id);
      return `<a class="gloss-link" href="javascript:void(0)" onclick="openGlossary('${id}');event.stopPropagation();return false;">${m}</a>`;});
  }
  return out;
}

const GLOSSARY=[
  ['alert','Alert','A rule "fires" (alerts) when its condition crosses the configured threshold — e.g. today\'s move exceeds the volatility threshold. Every alert is logged to the Alerts tab; whether it also sends a push/email/WhatsApp depends on your notification settings.'],
  ['strong','STRONG signal','When at least two independent rules agree on the same direction (a two-thirds majority of the voting rules), Tripwire escalates to a <b>STRONG BUY</b> or <b>STRONG BOUNCE WATCH</b>. STRONG signals carried the strongest backtested edge (~+1.9% over 5 days, 61% hit) and are what outbound notifications default to. Only the four rules with a proven edge vote (volatility, support/resistance, volume, RSI); gap, consecutive-down and MA-cross still alert individually but don\'t vote.'],
  ['bounce','Bounce watch','What used to be a "SELL" signal. Backtesting this watchlist found that downside triggers (a gap down, an oversold RSI, a support break) were historically followed by a <b>rebound within a few days</b>, not a continued decline — so a downside signal is framed as a "bounce watch" (a possible dip-buy setup) rather than a sell. This is calibrated on a bull-heavy period; in a sustained bear market the bounce tendency weakens.'],
  ['cagr','Per year (CAGR)','The steady yearly growth rate that would turn the starting value into the end value. +13%/yr for 5 years roughly doubles money. Shown only after 90 days, because annualising a few weeks exaggerates wildly.'],
  ['never-traded','Never traded','The Portfolio test\'s reference line: the same $1,000 split equally across the same stocks and simply held. The gap between "following the app" and this line is what the app\'s signals add or cost, separate from how good the stock picks themselves were.'],
  ['backtested','Backtested','Every threshold in Tripwire was chosen by replaying ~5 years of daily prices and measuring what actually happened after each trigger (see the Backtesting doc). The "Backtested: +x% over 5d" line on a rule is that rule\'s measured historical edge, not a guess.'],
  ['excess','Excess return (vs SPY)','A rule\'s forward return with the S&P 500 (SPY) subtracted over the same window, so the rule isn\'t credited for a move that was really just the whole market rising. +2% excess means the stock beat SPY by 2 points over the measured days.'],
  ['hitrate','Hit rate','The percent of a rule\'s historical triggers where the signal was "right" (a positive signal-direction excess return). 55–62% is typical for the strong rules — an edge, not a crystal ball.'],
  ['mae','Worst case (MAE)','Maximum Adverse Excursion — the average worst intraday drawdown within the window after a trigger. It answers "if I acted on this, how far underwater might I have gone before the edge played out?"'],
  ['window','4-day action window','The thresholds are tuned to a 1–5 trading-day edge, so a signal is only considered actionable for about four trading days. Older alerts are greyed as "window closed" and stop counting toward the combined signal.'],
  ['volatility','Volatility (unusual move)','Fires when today\'s percentage move is unusually large versus the stock\'s own recent average daily move (e.g. 3.75× the 10-day norm). Big outlier moves often mark a catalyst worth investigating.'],
  ['sr','Support / Resistance','<b>Support</b> is a price floor the stock has repeatedly bounced off; <b>resistance</b> is a ceiling it has repeatedly failed to break. A close decisively beyond the prior N-day high/low (plus a buffer) is a breakout/breakdown. Upside breakouts were the single strongest rule in backtesting.'],
  ['volume','Volume spike','Fires when today\'s share volume is a multiple (e.g. 3×) of the recent average — a surge signals unusual institutional interest and conviction behind a move.'],
  ['gap','Gap','The jump between yesterday\'s close and today\'s open, before regular trading. A large gap reflects overnight news (earnings, guidance, macro). Gap <i>up</i> carried a modest edge; gap <i>down</i> tends to fade (see bounce watch).'],
  ['rsi','RSI','Relative Strength Index — a 0–100 momentum gauge of how fast and far price has moved recently. High = overbought, low = oversold. Tripwire uses a longer 21-period RSI with 80/35 thresholds on most stocks, tuned from the backtest.'],
  ['obos','Overbought / Oversold (OB/OS)','<b>Overbought</b> (RSI ≥ the OB threshold, e.g. 80) means price has run up fast and may be due to cool off — a bounce-watch, though strong stocks can stay overbought for weeks. <b>Oversold</b> (RSI ≤ the OS threshold, e.g. 35) means it has fallen fast and may rebound — a possible buy setup.'],
  ['crossover','Moving-average cross-over','When a short moving average crosses a long one: a <b>golden cross</b> (short rises above long) is a bullish trend shift, a <b>death cross</b> (short falls below long) a bearish one. Backtesting showed little short-term edge here, so MA-cross is off by default and doesn\'t vote toward STRONG signals — it\'s kept as chart context.'],
  ['info','Info tier (activity)','A quieter second pass using the looser pre-tuning thresholds. It surfaces day-to-day "activity" on a card for awareness, without making a buy/sell claim, logging an alert, or sending a notification. Turn it off in Settings if you only want validated signals.'],
];
let glossaryRendered=false;
function renderGlossary(){
  if(glossaryRendered) return;
  glossaryRendered=true;
  const pane=document.getElementById('pane-glossary');
  pane.innerHTML=`<h2 style="font-size:20px;font-weight:800;margin-bottom:6px">📖 Glossary</h2>`+
    `<div style="color:#6B7280;font-size:13px;margin-bottom:18px">Plain-English definitions of the terms Tripwire uses. Dotted-underlined terms elsewhere in the app link here.</div>`+
    GLOSSARY.map(([id,term,def])=>`<div class="gloss-entry" id="gloss-${id}"><div class="gloss-term">${term}</div><div class="gloss-def">${def}</div></div>`).join('');
}

// ── Add / remove stock ────────────────────────────────────────────────────────
async function addStock(){
  const sym=document.getElementById('inp-symbol').value.trim().toUpperCase();
  const cat=document.getElementById('inp-cat').value;
  const st=document.getElementById('add-status');
  if(!sym){st.textContent='Enter a symbol';st.className='err';return;}
  st.textContent='Looking up '+sym+'...';st.className='';
  const r=await postJSON('/api/stocks/add',{symbol:sym,category:cat});
  if(r.success){
    st.textContent=sym+' added @ $'+r.price;st.className='ok';
    document.getElementById('inp-symbol').value='';
    await loadAll();
  }else{st.textContent=r.error;st.className='err';}
}

async function removeStock(sym,e){
  e.stopPropagation();
  if(!confirm('Remove '+sym+' from watchlist?')) return;
  await postJSON('/api/stocks/remove',{symbol:sym});
  if(selectedSym===sym){selectedSym=null;document.getElementById('detail').style.display='none';}
  await loadAll();
}

// ── Signal logic ─────────────────────────────────────────────────────────────
// Rule weights reflect backtest.py's `weight-check` comparison (see BACKTESTING.md): doubling
// the vote of the individually-strongest rules (support/resistance, volatility) was tested and
// measurably *hurt* the combined signal (pooled mean 5d excess -0.066pp vs equal-vote) — an
// ensemble's value comes from independent confirmation, which a dominant rule undermines. Only
// the ma_cross exclusion held up (roughly neutral, consistent with its ~zero solo edge), so it's
// the only non-1 weight kept here.
// Must stay in lock-step with Python's RULE_SIGNAL_WEIGHT (see compute_signal in the backend).
// Near-zero-edge rules are excluded from the ensemble vote (their individual alerts still show):
// backtest "weight-check" found removing these noise voters doubled STRONG-event quality, while
// up-weighting the strongest rules regressed it.
const RULE_SIGNAL_WEIGHT={consecutive_down:0,gap:0,ma_cross:0};
// Context-only rules (mirror of Python CONTEXT_ONLY_RULES): shown but never alert/notify.
const CONTEXT_ONLY_RULES={consecutive_down:1};
function ruleWeight(r){ return RULE_SIGNAL_WEIGHT[r.rule_type]??1; }

function computeSignal(rules){
  const triggered=(rules||[]).filter(r=>r.triggered&&!r.disabled&&ruleWeight(r)>0);
  if(!triggered.length) return null;
  const buys=triggered.filter(r=>r.signal==='BUY').reduce((s,r)=>s+ruleWeight(r),0);
  const sells=triggered.filter(r=>r.signal==='SELL').reduce((s,r)=>s+ruleWeight(r),0);
  const total=buys+sells;
  if(!total) return{label:'PENDING',cls:'sig-pending'};
  if(total>=2&&buys/total>=2/3) return{label:'STRONG BUY',cls:'sig-strong-buy'};
  if(total>=2&&sells/total>=2/3) return{label:'STRONG BOUNCE WATCH',cls:'sig-strong-sell'};
  if(buys>sells) return{label:'TRENDING BUY',cls:'sig-trending-buy'};
  if(sells>buys) return{label:'BOUNCE WATCH',cls:'sig-trending-sell'};
  return{label:'PENDING',cls:'sig-pending'};
}

function signalBadge(sig){
  if(!sig) return '';
  return `<span class="sig-badge ${sig.cls}">${sig.label}</span>`;
}

function ruleSigBadge(signal){
  if(!signal||signal==='NEUTRAL') return '<span class="sig-badge rule-dir sig-neutral">NEUTRAL</span>';
  if(signal==='BUY') return '<span class="sig-badge rule-dir sig-buy">▲ BUY</span>';
  if(signal==='SELL') return '<span class="sig-badge rule-dir sig-sell">⚠ BOUNCE WATCH</span>';
  return '';
}

// ── Stock grid ────────────────────────────────────────────────────────────────
function renderGrid(){
  const grid=document.getElementById('stock-grid');
  // The tab count is the banner's count: signals waiting for your call
  const needCount=needsCall().length;
  document.getElementById('tab-alerts-btn').textContent='Alerts'+(needCount>0?' ('+needCount+')':'');
  const openBySym=openSignalBySymbol();

  grid.innerHTML=stocks.map(s=>{
    const pctCls=s.pct>0?'up':s.pct<0?'dn':'muted';
    const sign=s.pct>0?'+':'';
    const selCls=selectedSym===s.symbol?' selected':'';
    const ev=openBySym[s.symbol];
    // signal colours are reserved for signals; a single rule triggering doesn't recolour the tile
    const alertCls=ev?` sig-open ${ev.buy?'buy':'watch'}${ev.decision?' decided':''}`:'';
    const triggered=(s.rules||[]).filter(r=>r.triggered&&!r.disabled&&!CONTEXT_ONLY_RULES[r.rule_type]);

    const sig=computeSignal(s.rules);
    const allRules=(s.rules||[]).filter(r=>!r.disabled);
    const hasRuleData=allRules.length>0&&allRules.some(r=>r.message&&!r.message.includes('Insufficient')&&!r.message.includes('Waiting'));

    // Compact status: a one-line header + a wrap of small chips (one per active rule),
    // amber if triggered, green if OK. Full detail is one click away in the panel below.
    let rulesHTML='';
    if(hasRuleData){
      const chips=allRules.filter(r=>detailMode||!CONTEXT_ONLY_RULES[r.rule_type]).map(r=>{
        const ctx=CONTEXT_ONLY_RULES[r.rule_type];
        const cls=ctx?'off':(r.triggered&&!r.disabled)?'trig':'ok';
        const tip=ctx?(r.label||r.rule_type)+' (context only — does not alert)':(r.label||r.rule_type);
        return `<span class="rule-chip ${cls}" title="${escAttr(tip)}">${shortRuleLabel(r.rule_type)}</span>`;
      }).join('');
      // "now" = the latest check; the open signal above it may be from an earlier day
      const hdr=triggered.length>0
        ? `<div class="card-status-hdr" style="color:#F59E0B">⚠ ${triggered.length} triggered now</div>`
        : `<div class="card-status-hdr" style="color:#10B981">✓ Quiet now</div>`;
      rulesHTML=`${hdr}${liveReadingHTML(sig,ev)}<div class="rule-chips">${chips}</div>`;
    }

    return `<div class="stock-card${selCls}${alertCls}" onclick="selectStock('${s.symbol}')">
      ${(!guestMode||s.guest_added)?`<button class="remove-btn" onclick="removeStock('${s.symbol}',event)">✕</button>`:''}
      ${ev&&!ev.decision?'<div class="alert-dot" title="Needs your call"></div>':''}
      <div class="card-top">
        <span class="stock-symbol">${s.symbol}${s.guest_added?'<span class="guest-tag" title="Added by a guest — shown here, but never emails or WhatsApps the owner">guest</span>':''}</span>
        <span class="stock-cat">${(s.category||'').replace('_vol',' vol')}</span>
      </div>
      ${s.price!=null?`
        <div class="card-priceline">
          <span class="stock-price">$${s.price}</span>
          <span class="stock-pct ${pctCls}">${sign}${s.pct}%</span>
        </div>
        <div class="stock-ext">
          ${s.pre_market?`<span class="pre-clr">Pre $${s.pre_market}</span>`:''}
          ${s.post_market?`<span class="post-clr">Post $${s.post_market}</span>`:''}
        </div>
        <div class="stock-time">${s.date} ${s.time}</div>
        ${earningsChip(s)}
        ${signalStripHTML(ev)}
        ${rulesHTML}
        ${infoEventsLineHTML(s)}
      `:`<div class="stock-err">${s.error||'Loading...'}</div>`}
    </div>`;
  }).join('');

  if(selectedSym){const s=stocks.find(x=>x.symbol===selectedSym);if(s)renderDetail(s);}
  renderTriage();
}

// ── Home: one answer — "Nothing needs you" or the STRONG signals still waiting for your call ──
let detailMode=false;
try{ detailMode=localStorage.getItem('tw_detail')==='1'; }catch(e){}
function toggleDetailMode(){
  detailMode=!detailMode;
  try{ localStorage.setItem('tw_detail',detailMode?'1':'0'); }catch(e){}
  track(detailMode?'Showed more detail':'Showed less detail');
  renderGrid();
}

// ── Signals: ONE definition shared by the banner, the stock tiles, the detail panel and the
// Alerts tab, so they can never disagree. A signal is a STRONG ensemble reading (2+ core rules
// agreeing) in the alert log. All STRONG alerts for one stock, one day and one direction are one
// signal (a rule firing later that day while still STRONG doesn't create a second one); the
// stock's other alerts that day are its supporting evidence. A signal stays open for
// ACTION_WINDOW_TRADING_DAYS trading days; it needs you until you mark "I acted" / "I passed".
const RULE_TITLE={volatility:'Unusual Move',support_resistance:'S/R Breach',consecutive_down:'Consec. Down',volume:'Volume Spike',gap:'Opening Gap',rsi:'RSI Extreme',ma_cross:'MA Crossover'};
function alertDay(a){ return String(a.time).slice(0,10); }
function signalEvents(){
  if(signalEvents._src===alerts) return signalEvents._cache;
  const map={}, byDay={};
  for(const a of alerts){
    const d=alertDetailOf(a);
    if(!d.strong) continue;
    const known=d.ensemble&&d.ensemble.startsWith('STRONG')?d.ensemble:null;
    const key=a.symbol+'|'+alertDay(a)+'|'+(known?(known.includes('BUY')?'buy':'watch'):'?');
    let ev=map[key];
    if(!ev) ev=map[key]={key:key.replace(/[^A-Za-z0-9]/g,'-'),symbol:a.symbol,day:alertDay(a),ts:a.ts,ids:[],
                         strongAlerts:[],alerts:[],label:known,decision:null,voteSet:{}};
    ev.ids.push(a.id); ev.strongAlerts.push(a); ev.alerts.push(a);
    ev.ts=Math.min(ev.ts,a.ts);
    if(a.decision) ev.decision=a.decision;
    for(const v of (d.votes||[])) ev.voteSet[v.rule_type]=v.signal;
  }
  const evs=Object.values(map);
  for(const ev of evs){
    if(!ev.label){   // alerts from before the label was stored: infer the direction
      const sigs=ev.strongAlerts.map(a=>alertDetailOf(a).signal);
      ev.label='STRONG '+(sigs.filter(s=>s==='BUY').length>=sigs.filter(s=>s==='SELL').length?'BUY':'BOUNCE WATCH');
    }
    ev.buy=ev.label.includes('BUY');
    (byDay[ev.symbol+'|'+ev.day]=byDay[ev.symbol+'|'+ev.day]||[]).push(ev);
  }
  // the stock's other alerts that day are the evidence (attached to the matching direction)
  for(const a of alerts){
    const d=alertDetailOf(a);
    if(d.strong) continue;
    const cands=byDay[a.symbol+'|'+alertDay(a)];
    if(!cands) continue;
    const ev=cands.find(e=>e.buy===(d.signal==='BUY'))||cands[0];
    ev.alerts.push(a);
  }
  for(const ev of evs){
    ev.alerts.sort((x,y)=>x.ts-y.ts);
    const want=ev.buy?'BUY':'SELL';
    if(!Object.keys(ev.voteSet).length)   // older alerts didn't store their votes
      for(const a of ev.alerts){ const s=alertDetailOf(a).signal; if(s===want&&ruleWeight(a)>0) ev.voteSet[a.rule_type]=s; }
    ev.votes=Object.keys(ev.voteSet).filter(rt=>ev.voteSet[rt]===want);
    ev.rules=ev.votes.length?ev.votes:ev.alerts.map(a=>a.rule_type);
  }
  signalEvents._src=alerts; signalEvents._cache=evs;
  return evs;
}
// open = still inside its action window; ordered like the banner: needing a call first (most urgent first)
function openSignals(){
  const open=signalEvents().filter(e=>isWithinActionWindow(e.ts));
  return open.filter(e=>!e.decision).sort((a,b)=>a.ts-b.ts).concat(open.filter(e=>e.decision).sort((a,b)=>b.ts-a.ts));
}
function needsCall(){ return openSignals().filter(e=>!e.decision); }
function openSignalBySymbol(){
  const m={};
  for(const e of openSignals()) if(!m[e.symbol]||e.ts>m[e.symbol].ts) m[e.symbol]=e;
  return m;
}
function strongBadgeHTML(ev){ return `<span class="sig-badge ${ev.buy?'sig-strong-buy':'sig-strong-sell'}">${ev.label}</span>`; }
function decisionHTML(ev){
  const ids=ev.ids.join(',');
  if(ev.decision){
    const st=ev.decision==='acted'?'<span class="dec-state acted">✓ You acted</span>':'<span class="dec-state passed">You passed</span>';
    return `<span class="today-actions">${st}${guestMode?'':` <a class="today-more" href="javascript:void(0)" onclick="markDecision('${ids}',null)">undo</a>`}</span>`;
  }
  if(guestMode) return '';
  return `<span class="today-actions"><button class="dec-btn acted" onclick="markDecision('${ids}','acted')">I acted</button><button class="dec-btn passed" onclick="markDecision('${ids}','passed')">I passed</button></span>`;
}
// The same row in the banner and on top of each Alerts-tab signal card
function signalRowHTML(ev,where){
  const why=where==='banner'?` · <a class="today-more" href="javascript:void(0)" onclick="showSignal('${ev.key}')">Why? ›</a>`:'';
  return `<div class="today-item">
    <span class="today-sym" onclick="goToStock('${ev.symbol}')">${ev.symbol}</span>
    ${strongBadgeHTML(ev)}
    <span class="today-meta">fired ${fmtFired(ev.ts)} · act by ${actByDate(ev.ts)}${eventEdge(ev.symbol,ev.rules)}${why}</span>
    ${decisionHTML(ev)}
  </div>`;
}
function goToStock(sym){
  if(document.getElementById('pane-stocks').style.display==='none') switchTab('stocks',tabBtn('stocks'));
  if(selectedSym!==sym) selectStock(sym);
  const card=[...document.querySelectorAll('.stock-card .stock-symbol')].find(x=>x.textContent.trim().startsWith(sym));
  if(card) card.closest('.stock-card').scrollIntoView({block:'center',behavior:'smooth'});
}
// The open signal on a tile / detail panel: the banner's badge, plus its deadline or your decision
function signalStripHTML(ev){
  if(!ev) return '';
  const meta=ev.decision==='acted'?'✓ you acted':ev.decision==='passed'?'you passed':`act by ${actByDate(ev.ts)}`;
  return `<div class="card-signal-row">${strongBadgeHTML(ev)}<span class="card-sig-meta${ev.decision?'':' due'}">${meta}</span></div>`;
}
// What the rules say at the latest check. Only a STRONG reading gets a full badge; a lean from a
// single rule is a small tag so it can't be mistaken for a signal.
function liveReadingHTML(sig,ev){
  if(!sig||sig.label==='PENDING') return '';
  if(sig.label.startsWith('STRONG')){
    if(ev&&ev.label===sig.label) return '';   // the open signal already says it
    return `<div class="card-signal-row">${signalBadge(sig)}<span class="card-sig-meta">at the latest check</span></div>`;
  }
  const buy=sig.label==='TRENDING BUY';
  return `<span class="lean-pill ${buy?'buy':'watch'}" title="Only one core rule points this way — not a signal">leaning ${buy?'buy':'bounce watch'} · not confirmed</span>`;
}
function showSignal(key){
  switchTab('alerts',tabBtn('alerts'));
  const el=document.getElementById('sig-'+key);
  if(!el) return;
  el.scrollIntoView({block:'start',behavior:'smooth'});
  el.classList.add('flash'); setTimeout(()=>el.classList.remove('flash'),1600);
}
function actByDate(ts){
  const d=new Date(ts*1000); let n=ACTION_WINDOW_TRADING_DAYS;
  while(n>0){ d.setDate(d.getDate()+1); const w=d.getDay(); if(w!==0&&w!==6) n--; }
  return d.toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short'});
}
function fmtFired(ts){ return new Date(ts*1000).toLocaleString('en-GB',{weekday:'short',hour:'2-digit',minute:'2-digit'}); }
function eventEdge(sym,rules){
  let best=null;
  for(const rt of rules){ const st=(ruleStats[sym]||{})[rt]; if(st&&(best==null||st.exc5>best)) best=st.exc5; }
  return best==null?'':` · backtested ${best>=0?'+':''}${best.toFixed(1)}% vs market over 5d`;
}
async function markDecision(ids,decision){
  await postJSON('/api/decision',{ids:String(ids).split(',').map(Number),decision});
  showToast(decision==='acted'?'Marked: you acted. The follow-up in ~5 trading days will show how it went.'
          :decision==='passed'?'Marked: you passed. The follow-up in ~5 trading days will show how it went.'
          :'Decision cleared — the signal needs your call again.',4500);
  await loadAll();
  if(document.getElementById('pane-analytics').style.display!=='none') loadAnalytics();
}

function renderTriage(){
  const el=document.getElementById('triage-strip');
  if(!el) return;
  const strong=[], trend=[]; let infoCount=0;
  for(const s of stocks){
    if(!s.rules||!s.rules.length) continue;
    const sig=computeSignal(s.rules);
    if(sig){
      if(sig.label.startsWith('STRONG')) strong.push({s,sig});
      else if(sig.label==='TRENDING BUY'||sig.label==='BOUNCE WATCH') trend.push({s,sig});
    }
    if((s.info_events||[]).length) infoCount++;
  }
  const needs=needsCall();
  let html;
  if(needs.length){
    html=`<div class="today-head needs">⚡ ${needs.length} signal${needs.length>1?'s need':' needs'} you — act by ${actByDate(needs[0].ts)}</div>`+
      needs.map(e=>signalRowHTML(e,'banner')).join('');
  }else{
    html=`<div class="today-head calm">✓ Nothing needs you today</div>`;
  }
  const bits=[];
  if(trend.length) bits.push(`${trend.length} trending (not yet confirmed)`);
  if(infoCount) bits.push(`${infoCount} with minor activity`);
  html+=`<div class="today-sub">${bits.join(' · ')}${bits.length?' · ':''}<a class="today-more" href="javascript:void(0)" onclick="toggleDetailMode()">${detailMode?'Less detail ▴':'More detail ▾'}</a></div>`;
  if(!detailMode){ el.innerHTML=`<div class="triage-box">${html}</div>`; return; }
  const bestEdge=(s)=>{
    // surface the strongest triggered rule's backtested 5d edge, if we have stats
    let best=null;
    for(const r of (s.rules||[])){
      if(!r.triggered||r.disabled) continue;
      const st=(ruleStats[s.symbol]||{})[r.rule_type];
      if(st&&(best==null||st.exc5>best)) best=st.exc5;
    }
    return best;
  };
  const chip=(s,sig)=>{
    const cls=sig.label.includes('BUY')?'buy':sig.label.includes('WATCH')?'watch':'trend';
    const e=bestEdge(s);
    const edge=e!=null?`<span class="tedge">${e>=0?'+':''}${e.toFixed(1)}% 5d</span>`:'';
    return `<span class="triage-chip ${cls}" onclick="selectStock('${s.symbol}')"><span class="tsym">${s.symbol}</span> ${sig.label} ${edge}</span>`;
  };
  let rows='';
  if(strong.length) rows+=`<div class="triage-row"><span class="triage-label">Strong right now</span>${strong.map(x=>chip(x.s,x.sig)).join('')}</div>`;
  if(trend.length)  rows+=`<div class="triage-row"><span class="triage-label">Trending</span>${trend.map(x=>chip(x.s,x.sig)).join('')}</div>`;
  if(infoCount)     rows+=`<div class="triage-row"><span class="triage-label">Activity</span><span class="triage-info">${infoCount} stock${infoCount>1?'s':''} with info-tier activity (below signal threshold)</span></div>`;
  el.innerHTML=`<div class="triage-box">${html}${rows?'<div class="today-detail">'+rows+'</div>':''}</div>`;
}

// Two-tier alerts: a single muted "activity" line per card for rules that crossed the looser
// pre-tuning (info) threshold but not the validated signal threshold — informational only,
// never a BUY/SELL claim, visually subordinate to real triggered rules. See INFO_RULES/
// compute_info_events in app.py and the show_info_tier setting.
function infoEventsLineHTML(s){
  const events=s.info_events||[];
  if(!events.length||!detailMode) return '';
  const text=events.map(e=>shortRuleLabel(e.rule_type)+': '+e.message.replace(/^activity:\s*/,'')).join('  ·  ');
  return `<div class="info-tier-line">⚡ activity: ${text}</div>`;
}

function earningsChip(s){
  if(s.earnings_in_days==null||s.earnings_in_days<0) return '';
  const d=s.earnings_in_days;
  const txt=d===0?'Earnings today':d===1?'Earnings tomorrow':'Earnings in '+d+'d';
  return `<div class="earnings-chip${d<=5?' soon':''}">📅 ${txt}</div>`;
}

function shortRuleLabel(rt){
  return {volatility:'Move',support_resistance:'S/R',consecutive_down:'Consec',
          volume:'Vol',gap:'Gap',rsi:'RSI',ma_cross:'MA'}[rt]||rt;
}

function cardRuleDetail(r){
  if(r.rule_type==='volatility'){
    if(r.actual_value==null) return r.message||'';
    const dir=r.direction==='UP'?'▲':'▼';
    return `${dir} ${r.actual_value}% (thresh ${r.threshold}%)`;
  }
  if(r.rule_type==='support_resistance'){
    if(r.actual_value==null) return r.message||'';
    if(r.triggered&&r.actual_value<r.support) return `$${r.actual_value} below support $${r.support}`;
    if(r.triggered&&r.actual_value>r.resistance) return `$${r.actual_value} above resistance $${r.resistance}`;
    return `$${r.actual_value} in range $${r.support}–$${r.resistance}`;
  }
  if(r.rule_type==='consecutive_down'){
    if(r.actual_value==null) return r.message||'';
    return r.triggered?`${r.actual_value} days down`:`max ${r.actual_value} days (OK)`;
  }
  return r.message||'';
}

// ── Select / detail ───────────────────────────────────────────────────────────
function selectStock(sym){
  if(selectedSym===sym){selectedSym=null;document.getElementById('detail').style.display='none';renderGrid();return;}
  track('Opened stock details: '+sym);
  selectedSym=sym;
  const s=stocks.find(x=>x.symbol===sym);
  if(s){
    renderDetail(s);renderGrid();
    // Detail renders below the grid — bring it into view, offset for the sticky topbar+tabs.
    requestAnimationFrame(()=>{
      const el=document.getElementById('detail');
      if(!el) return;
      const y=el.getBoundingClientRect().top+window.pageYOffset-96;
      window.scrollTo({top:y,behavior:'smooth'});
    });
  }
}

function pctStr(a,b){const p=((a-b)/b*100);return(p>0?'+':'')+p.toFixed(2)+'%';}

function renderDetail(s){
  const panel=document.getElementById('detail');
  panel.style.display='block';
  const pctCls=s.pct>0?'up':s.pct<0?'dn':'muted';
  const sign=s.pct>0?'+':'';

  // Compute 90d range from history
  const h=s.history_closes||[];
  const h90lo=h.length?Math.min(...h):null;
  const h90hi=h.length?Math.max(...h):null;

  const ranges=[
    rangeBarHTML(h90lo&&h90lo.toFixed(2),h90hi&&h90hi.toFixed(2),s.price,'30-day range','Low','High','#10B981'),
    rangeBarHTML(s.week52_low,s.week52_high,s.price,'52-week range','52W Low','52W High','#3B82F6'),
    rangeBarHTML(s.lifetime_low,s.lifetime_high,s.price,'All-time range','All-Time Low','All-Time High','#A78BFA'),
  ].join('');

  panel.innerHTML=`
    <div id="detail-header">
      <div>
        <div id="detail-title">${s.symbol} <span style="font-size:13px;color:#6B7280;font-weight:400">${(s.category||'').replace('_vol',' vol')}</span></div>
        <div id="detail-meta">Updated ${s.date} ${s.time}${s.alert?' · <span style="color:#F59E0B">⚠ Rule triggered now</span>':''}</div>
        ${signalStripHTML(openSignalBySymbol()[s.symbol])}
        ${liveReadingHTML(computeSignal(s.rules),openSignalBySymbol()[s.symbol])}
      </div>
      <button id="btn-close" onclick="selectStock('${s.symbol}')">✕ Close</button>
    </div>
    <div id="detail-chart">
      <div class="chart-range-row">
        ${['5D','1M','3M','YTD','12M'].map(r=>`<button class="chart-range-btn${r===chartRange?' active':''}" onclick="setChartRange('${s.symbol}','${r}')">${r}</button>`).join('')}
      </div>
      <div class="chart-box" id="chart-box"><div style="color:#6B7280;font-size:13px;padding:30px;text-align:center">Loading chart…</div></div>
    </div>
    <div id="price-grid">
      <div class="price-cell"><div class="price-cell-label">Price</div>
        <div class="price-cell-val">$${s.price||'—'}</div>
        <div class="price-cell-sub ${pctCls}">${sign}${s.pct}%</div></div>
      <div class="price-cell"><div class="price-cell-label">Prev Close</div>
        <div class="price-cell-val">$${s.prev_close||'—'}</div></div>
      <div class="price-cell"><div class="price-cell-label">Day High</div>
        <div class="price-cell-val up">$${s.high||'—'}</div></div>
      <div class="price-cell"><div class="price-cell-label">Day Low</div>
        <div class="price-cell-val dn">$${s.low||'—'}</div></div>
      <div class="price-cell" style="${s.pre_market?'border-color:#A78BFA44':''}">
        <div class="price-cell-label pre-clr">Pre-Market</div>
        <div class="price-cell-val">${s.pre_market?'$'+s.pre_market:'—'}</div>
        ${s.pre_market&&s.prev_close?`<div class="price-cell-sub">${pctStr(s.pre_market,s.prev_close)} vs close</div>`:''}
      </div>
      <div class="price-cell" style="${s.post_market?'border-color:#60A5FA44':''}">
        <div class="price-cell-label post-clr">Post-Market</div>
        <div class="price-cell-val">${s.post_market?'$'+s.post_market:'—'}</div>
        ${s.post_market&&s.price?`<div class="price-cell-sub">${pctStr(s.post_market,s.price)} vs close</div>`:''}
      </div>
    </div>
    <div class="ranges-grid">${ranges}</div>
    ${sensitivityControlHTML(s)}
    <div class="section-label">Rules <span style="color:#4B5563;font-weight:400;text-transform:none;letter-spacing:0">— advanced: fine-tune individual thresholds via each rule's Edit button</span></div>
    <div id="rules-container">
      ${(s.rules||[]).map((r,i)=>ruleHTML(s.symbol,r,i,s.history_closes,s.params)).join('')}
      ${(!s.rules||s.rules.length===0)?'<div style="color:#6B7280;font-size:13px">No rule data yet — click Check Now</div>':''}
    </div>`;
  loadDetailChart(s.symbol);
}

// ── Sensitivity dial ──────────────────────────────────────────────────────────
function sensitivityControlHTML(s){
  const cur=(s.params&&s.params._sensitivity)||'calibrated';
  const opts=[
    ['conservative','Conservative','Fewer, higher-conviction alerts'],
    ['calibrated','Calibrated','Backtest-tuned (recommended)'],
    ['sensitive','Sensitive','More alerts, catches smaller moves'],
  ];
  const btns=opts.map(([v,label,desc])=>
    `<button class="sens-btn${v===cur?' active':''}" title="${desc}" onclick="setSensitivity('${s.symbol}','${v}')">${label}</button>`).join('');
  return `<div class="sens-wrap">
    <div class="sens-label">Alert sensitivity</div>
    <div class="sens-seg">${btns}</div>
    <div class="sens-desc">${opts.find(o=>o[0]===cur)[2]}</div>
  </div>`;
}
async function setSensitivity(sym,level){
  const r=await postJSON('/api/stock/'+encodeURIComponent(sym)+'/sensitivity',{level});
  if(r&&r.success){ await loadAll(); }
}

// ── Detail multi-timeframe price chart ────────────────────────────────────────
let chartRange='3M';
const chartCache={};  // sym -> {daily:[{date,close}], intraday:[{t,close}]}
async function loadDetailChart(sym){
  if(chartCache[sym]){ renderDetailChart(sym); return; }
  try{
    const d=await fetchJSON('/api/history/'+encodeURIComponent(sym));
    chartCache[sym]=d;
  }catch(e){ chartCache[sym]={daily:[],intraday:[]}; }
  if(selectedSym===sym) renderDetailChart(sym);
}
function setChartRange(sym,r){
  track(`Changed chart range: ${sym} ${r}`);
  chartRange=r;
  document.querySelectorAll('.chart-range-btn').forEach(b=>b.classList.toggle('active',b.textContent===r));
  renderDetailChart(sym);
}
function pointsForRange(data,range){
  if(!data) return {pts:[],xlabel:''};
  if(range==='5D'){
    const iv=data.intraday||[];
    return {pts:iv.map(x=>({label:x.t,close:x.close})),xlabel:'last 5 market days'};
  }
  const daily=data.daily||[];
  if(!daily.length) return {pts:[],xlabel:''};
  let cut;
  const now=new Date();
  if(range==='1M') cut=new Date(now.getFullYear(),now.getMonth()-1,now.getDate());
  else if(range==='3M') cut=new Date(now.getFullYear(),now.getMonth()-3,now.getDate());
  else if(range==='YTD') cut=new Date(now.getFullYear(),0,1);
  else cut=new Date(now.getFullYear()-1,now.getMonth(),now.getDate()); // 12M
  const iso=cut.toISOString().slice(0,10);
  const f=daily.filter(x=>x.date>=iso);
  return {pts:(f.length?f:daily).map(x=>({label:x.date,close:x.close})),xlabel:range};
}
function renderDetailChart(sym){
  const box=document.getElementById('chart-box');
  if(!box) return;
  const {pts,xlabel}=pointsForRange(chartCache[sym],chartRange);
  if(!pts.length){ box.innerHTML='<div style="color:#6B7280;font-size:13px;padding:30px;text-align:center">No chart data for this range.</div>'; return; }
  box.innerHTML=priceLineSVG(pts,xlabel);
}
function priceLineSVG(pts,xlabel){
  const W=760,H=220,pT=14,pB=26,pL=52,pR=12;
  const plotW=W-pL-pR, plotH=H-pT-pB;
  const closes=pts.map(p=>p.close);
  const lo=Math.min(...closes), hi=Math.max(...closes);
  const span=(hi-lo)||1;
  const x=i=>pL+(pts.length<=1?plotW/2:(i/(pts.length-1))*plotW);
  const y=v=>pT+plotH-((v-lo)/span)*plotH;
  const first=closes[0], last=closes[closes.length-1];
  const up=last>=first;
  const stroke=up?'#10B981':'#EF4444';
  const line=pts.map((p,i)=>`${i?'L':'M'}${x(i).toFixed(1)},${y(p.close).toFixed(1)}`).join('');
  const area=`M${x(0).toFixed(1)},${(pT+plotH).toFixed(1)} `+pts.map((p,i)=>`L${x(i).toFixed(1)},${y(p.close).toFixed(1)}`).join(' ')+` L${x(pts.length-1).toFixed(1)},${(pT+plotH).toFixed(1)} Z`;
  // y-axis gridlines (lo, mid, hi)
  const grid=[lo,(lo+hi)/2,hi].map(v=>{
    const yy=y(v).toFixed(1);
    return `<line x1="${pL}" y1="${yy}" x2="${W-pR}" y2="${yy}" stroke="#1E2235" stroke-width="1"/>`+
           `<text x="${pL-6}" y="${(+yy+3).toFixed(1)}" fill="#4B5563" font-size="10" text-anchor="end">$${v.toFixed(2)}</text>`;
  }).join('');
  const pct=(((last-first)/first)*100);
  const pctTxt=(pct>=0?'+':'')+pct.toFixed(2)+'%';
  const gid='g'+Math.random().toString(36).slice(2,7);
  return `<div class="chart-meta"><span>${xlabel} · ${pts.length} pts</span><span style="color:${stroke};font-weight:700">${pctTxt} · $${last.toFixed(2)}</span></div>`+
    `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:${H}px" preserveAspectRatio="none">`+
    `<defs><linearGradient id="${gid}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${stroke}" stop-opacity="0.22"/><stop offset="100%" stop-color="${stroke}" stop-opacity="0"/></linearGradient></defs>`+
    `${grid}<path d="${area}" fill="url(#${gid})"/><path d="${line}" fill="none" stroke="${stroke}" stroke-width="1.6"/>`+
    `<text x="${pL}" y="${H-6}" fill="#4B5563" font-size="10">${pts[0].label}</text>`+
    `<text x="${W-pR}" y="${H-6}" fill="#4B5563" font-size="10" text-anchor="end">${pts[pts.length-1].label}</text>`+
    `</svg>`;
}

// ── Rule card ─────────────────────────────────────────────────────────────────
function ruleHTML(sym,r,idx,historyClosed,params){
  const badge=r.disabled?'<span class="badge badge-disabled">DISABLED</span>':
               r.triggered?'<span class="badge badge-alert">⚠ ALERT</span>':
               '<span class="badge badge-ok">✓ OK</span>';
  const rSig=r.triggered&&!r.disabled?ruleSigBadge(r.signal):'';
  const alertCls=r.triggered&&!r.disabled?' alert-rule':'';
  const msgCls=r.triggered&&!r.disabled?' alert-msg':'';

  const vals=[];
  if(r.actual_value!=null){
    const unit=r.rule_type==='volatility'?'%':r.rule_type==='consecutive_down'?' days':'';
    vals.push(`<div class="val-cell"><div class="val-cell-label">Actual</div><div class="val-cell-val" style="color:${r.triggered?'#F59E0B':'#E4E0D8'}">${r.actual_value}${unit}</div></div>`);
  }
  if(r.threshold!=null){
    const unit=r.rule_type==='volatility'?'%':r.rule_type==='consecutive_down'?' days':'';
    vals.push(`<div class="val-cell"><div class="val-cell-label">Threshold</div><div class="val-cell-val">${r.threshold}${unit}</div></div>`);
  }
  if(r.avg_daily_vol!=null) vals.push(`<div class="val-cell"><div class="val-cell-label">Avg Daily Vol</div><div class="val-cell-val">${r.avg_daily_vol}%</div></div>`);
  if(r.support!=null)       vals.push(`<div class="val-cell"><div class="val-cell-label">Support</div><div class="val-cell-val up">$${r.support}</div></div>`);
  if(r.resistance!=null)    vals.push(`<div class="val-cell"><div class="val-cell-label">Resistance</div><div class="val-cell-val dn">$${r.resistance}</div></div>`);
  if(r.period_low!=null)    vals.push(`<div class="val-cell"><div class="val-cell-label">Period Low</div><div class="val-cell-val">$${r.period_low}</div></div>`);
  if(r.period_high!=null)   vals.push(`<div class="val-cell"><div class="val-cell-label">Period High</div><div class="val-cell-val">$${r.period_high}</div></div>`);

  let chart='';
  if(r.rule_type==='volatility') chart=chartVolatility(historyClosed,r.threshold,r.actual_value);
  else if(r.rule_type==='support_resistance') chart=chartSR(r.support,r.resistance,r.actual_value,r.period_low,r.period_high);
  else if(r.rule_type==='consecutive_down') chart=chartConsec(historyClosed);

  const editFields=RULE_EDIT_FIELDS[r.rule_type]||[];
  const enableKey=RULE_ENABLE_KEY[r.rule_type];
  const enabled=!(params&&params[enableKey]===false);

  const editRows=editFields.map(f=>{
    const cur=(params&&params[f.key]!=null)?params[f.key]:'';
    return `<div class="edit-row">
      <label>${f.label}</label>
      <input type="number" id="ep_${sym}_${f.key}" step="${f.step}" min="${f.min}" max="${f.max}" value="${cur}">
    </div>`;
  }).join('');

  const toggleRow=enableKey?`<div class="rule-toggle-row">
    <label>Rule enabled</label>
    <label class="switch"><input type="checkbox" id="en_${sym}_${r.rule_type}" ${enabled?'checked':''}><span class="slider"></span></label>
  </div>`:'';

  return `<div class="rule-card${alertCls}">
    <div class="rule-header">
      <div class="rule-title">${r.label}</div>
      <div class="rule-header-right">
        ${badge}
        ${rSig}
        <button class="edit-toggle" onclick="toggleEdit('${sym}','${r.rule_type}')">Edit</button>
      </div>
    </div>
    <div class="rule-desc">${linkifyGlossary(r.description||'')}</div>
    ${r.rationale?`<div class="rule-rationale">${linkifyGlossary(r.rationale)}</div>`:''}
    ${evidenceLineHTML(sym,r.rule_type)}
    ${r.triggered&&!r.disabled?bearRegimeCaveatHTML(r.signal):''}
    ${r.news_synthesis?`<div class="rule-news-synthesis"><div class="rule-news-label">📰 News Synthesis</div><div class="rule-news-text">${r.news_synthesis}</div></div>`:''}
    ${r.param_summary?`<div class="rule-params-line">⚙ ${r.param_summary}</div>`:''}
    ${chart}
    ${vals.length>0?`<div class="rule-vals" style="margin-top:10px">${vals.join('')}</div>`:''}
    <div class="rule-msg${msgCls}">▶ ${r.message}</div>
    <div id="edit_${sym}_${r.rule_type}" style="display:none">
      <div class="edit-panel">
        <div style="font-size:12px;font-weight:700;color:#3B82F6;margin-bottom:10px">Edit Parameters</div>
        ${toggleRow}
        ${editRows}
        <div class="edit-actions">
          <button class="btn-save" onclick="saveParams('${sym}','${r.rule_type}')">Save</button>
          <button class="btn-reset" onclick="resetParams('${sym}')">Reset to defaults</button>
        </div>
      </div>
    </div>
  </div>`;
}

const RULE_EDIT_FIELDS={
  volatility:[{key:'volatility_multiplier',label:'Multiplier',step:0.1,min:0.5,max:10},{key:'volatility_lookback',label:'Lookback (days)',step:1,min:5,max:60}],
  support_resistance:[{key:'support_resist_pct',label:'Buffer (%)',step:0.5,min:0.5,max:20},{key:'support_resist_lookback',label:'Lookback (days)',step:1,min:5,max:180}],
  consecutive_down:[{key:'consecutive_down_days',label:'Min days',step:1,min:2,max:10}],
  volume:[{key:'volume_multiplier',label:'Multiplier (×avg)',step:0.1,min:1.2,max:10},{key:'volume_lookback',label:'Lookback (days)',step:1,min:5,max:60}],
  gap:[{key:'gap_pct',label:'Gap threshold (%)',step:0.1,min:0.2,max:20}],
  rsi:[{key:'rsi_period',label:'Period',step:1,min:2,max:40},{key:'rsi_overbought',label:'Overbought ≥',step:1,min:50,max:95},{key:'rsi_oversold',label:'Oversold ≤',step:1,min:5,max:50}],
  ma_cross:[{key:'ma_short',label:'Short MA (days)',step:1,min:2,max:100},{key:'ma_long',label:'Long MA (days)',step:1,min:5,max:250},{key:'ma_cross_lookback',label:'Detect window (days)',step:1,min:1,max:15}],
};
const RULE_ENABLE_KEY={
  volatility:'enable_volatility', support_resistance:'enable_support_resistance',
  consecutive_down:'use_consecutive', volume:'enable_volume', gap:'enable_gap',
  rsi:'enable_rsi', ma_cross:'enable_ma',
};

function toggleEdit(sym,ruleType){
  const el=document.getElementById('edit_'+sym+'_'+ruleType);
  if(el) el.style.display=el.style.display==='none'?'block':'none';
}

async function saveParams(sym,ruleType){
  const fields=(RULE_EDIT_FIELDS[ruleType]||[]).map(f=>f.key);
  const params={};
  fields.forEach(k=>{
    const el=document.getElementById('ep_'+sym+'_'+k);
    if(el&&el.value!=='') params[k]=parseFloat(el.value);
  });
  const enKey=RULE_ENABLE_KEY[ruleType];
  const enEl=document.getElementById('en_'+sym+'_'+ruleType);
  if(enKey&&enEl) params[enKey]=enEl.checked;
  await postJSON('/api/stock/'+sym+'/params',params);
  setTimeout(loadAll,1500);
}

async function resetParams(sym){
  await postJSON('/api/stock/'+sym+'/params/reset',{});
  setTimeout(loadAll,1500);
}

// ── Alert log (grouped by ticker) ─────────────────────────────────────────────
const alertGroupOpen={};
const ACTION_WINDOW_TRADING_DAYS=4;

// Simple Mon-Fri trading-day count between a unix timestamp (seconds) and now. A holiday
// calendar is deliberately not modeled (see market_phase() docstring for the same tradeoff) —
// this is an approximation for "is this alert still inside its calibrated action window."
function tradingDaysSince(ts){
  const start=new Date(ts*1000);
  const now=new Date();
  let d=new Date(start.getFullYear(),start.getMonth(),start.getDate());
  const end=new Date(now.getFullYear(),now.getMonth(),now.getDate());
  let days=0;
  while(d<end){
    d.setDate(d.getDate()+1);
    const dow=d.getDay();
    if(dow!==0&&dow!==6) days++;
  }
  return days;
}
function isWithinActionWindow(ts){ return tradingDaysSince(ts)<=ACTION_WINDOW_TRADING_DAYS; }
function actionWindowChipHTML(ts){
  const age=tradingDaysSince(ts);
  if(age>ACTION_WINDOW_TRADING_DAYS){
    return `<span class="window-chip window-closed" title="Beyond the ${ACTION_WINDOW_TRADING_DAYS}-trading-day calibration window">window closed</span>`;
  }
  const left=ACTION_WINDOW_TRADING_DAYS-age;
  return `<span class="window-chip window-open">window: ${left}d left</span>`;
}

function alertEntryHTML(a,inSignal,hideNews){
  let detail={};
  try{ if(a.detail) detail=JSON.parse(a.detail); }catch(e){}
  const unit=a.rule_type==='volatility'?'%':a.rule_type==='consecutive_down'?' days':'';
  const detailVals=[];
  if(detail.actual_value!=null) detailVals.push(`<div class="alert-detail-val"><span>Actual</span><span>${detail.actual_value}${unit}</span></div>`);
  if(detail.threshold!=null)    detailVals.push(`<div class="alert-detail-val"><span>Threshold</span><span>${detail.threshold}${unit}</span></div>`);
  if(detail.avg_daily_vol!=null) detailVals.push(`<div class="alert-detail-val"><span>Avg Daily Vol</span><span>${detail.avg_daily_vol}%</span></div>`);
  if(detail.support!=null)       detailVals.push(`<div class="alert-detail-val"><span>Support</span><span>$${detail.support}</span></div>`);
  if(detail.resistance!=null)    detailVals.push(`<div class="alert-detail-val"><span>Resistance</span><span>$${detail.resistance}</span></div>`);
  if(detail.direction)           detailVals.push(`<div class="alert-detail-val"><span>Direction</span><span>${detail.direction}</span></div>`);
  const ruleLabel=RULE_TITLE[a.rule_type]||a.rule_type.replace(/_/g,' ');
  const alertSig=detail.signal?ruleSigBadge(detail.signal):'';
  const ctx=(RULE_SIGNAL_WEIGHT[a.rule_type]??1)===0?'<span class="window-chip window-closed" title="Shown for context — this rule does not count toward signals">context only</span>':'';
  // inside a signal card the card header carries the deadline; in the history only past signals get a chip
  const windowChip=inSignal?'':(detail.strong&&!isWithinActionWindow(a.ts)?actionWindowChipHTML(a.ts):'');
  const dim=!inSignal&&a.ack;
  return `<div class="alert-entry${!inSignal&&!isWithinActionWindow(a.ts)?' window-expired':''}" style="${dim?'opacity:.5':''}">
    <div class="alert-entry-header">
      <span class="alert-entry-time">${a.time}</span>
      <span class="alert-entry-rule">${ruleLabel}</span>
      ${alertSig}${ctx}
      ${windowChip}
      ${detail.near_earnings?'<span class="earnings-chip soon" style="margin-top:0">📅 earnings-driven?</span>':''}
      ${dim?'<span style="font-size:15px;color:#10B981">✓ ack</span>':''}
      <span class="alert-entry-price">Price: $${a.price}</span>
    </div>
    <div class="alert-entry-msg">${a.message}</div>
    ${detail.description?`<div class="alert-entry-detail">${detail.description}</div>`:''}
    ${detailVals.length>0?`<div class="alert-detail-vals">${detailVals.join('')}</div>`:''}
    ${detail.rationale?`<div class="alert-rationale">${detail.rationale}</div>`:''}
    ${evidenceLineHTML(a.symbol,a.rule_type)}
    ${detail.bear_regime_caveat?`<div class="bear-caveat">🐻 ${detail.bear_regime_caveat}</div>`:''}
    ${detail.news_synthesis&&!hideNews?`<div class="news-synthesis"><div class="news-synthesis-label">📰 News Synthesis</div><div class="news-synthesis-text">${detail.news_synthesis}</div></div>`:''}
  </div>`;
}

// One card per open signal — the banner's row on top, the alerts that make it up below.
// The same news synthesis is shared by every alert of a check, so it is shown once.
function signalCardHTML(ev){
  const agree=ev.votes.length
    ? `${ev.votes.length} core rule${ev.votes.length>1?'s':''} agreed: ${ev.votes.map(rt=>RULE_TITLE[rt]||rt).join(' · ')}`
    : '';
  let newsShown=false;   // one news synthesis per signal (older alerts each had their own copy)
  const entries=ev.alerts.map(a=>{
    const hide=newsShown; if(alertDetailOf(a).news_synthesis) newsShown=true;
    return alertEntryHTML(a,true,hide);
  }).join('');
  return `<div class="alert-group sig-card ${ev.buy?'buy':'watch'}" id="sig-${ev.key}">
    ${signalRowHTML(ev,'alerts')}
    ${agree?`<div class="sig-card-why">${agree}</div>`:''}
    <div class="alert-entries open">${entries}</div>
  </div>`;
}

function renderAlerts(){
  const el=document.getElementById('alert-pane');
  if(!alerts||alerts.length===0){
    el.innerHTML='<div class="no-alerts">No alerts yet — run a check to evaluate your watchlist.</div>';
    return;
  }
  // 1. Open signals: the same list, in the same order, as the banner on the Stocks tab
  const open=openSignals();
  const inCards=new Set(open.flatMap(e=>e.alerts.map(a=>a.id)));
  let html=`<div class="alerts-sec">Signals · last ${ACTION_WINDOW_TRADING_DAYS} trading days</div>`;
  html+=open.length?open.map(signalCardHTML).join('')
    :'<div class="no-alerts" style="padding:14px">✓ No open signals — nothing needs your call.</div>';

  // 2. Everything else, by stock, newest first, collapsed: single-rule alerts and past signals
  const groups={}, order=[];
  for(const a of alerts){
    if(inCards.has(a.id)) continue;
    if(!groups[a.symbol]){groups[a.symbol]=[];order.push(a.symbol);}
    groups[a.symbol].push(a);
  }
  if(order.length){
    html+=`<div class="alerts-sec">Rule activity &amp; history</div>
      <div class="set-hint" style="margin:-4px 0 10px">Single-rule alerts are context, not signals — it takes 2+ core rules agreeing to make one. Past signals appear here once their ${ACTION_WINDOW_TRADING_DAYS}-day window closes.</div>`;
    html+=order.map(sym=>{
      const entries=groups[sym];
      const isOpen=alertGroupOpen[sym]===true;   // collapsed unless you opened it
      const unacked=entries.filter(a=>!a.ack).length;
      const last=entries[0];
      const s=stocks.find(x=>x.symbol===sym);
      return `<div class="alert-group">
        <div class="alert-group-hdr" onclick="toggleAlertGroup('${sym}')">
          <span class="alert-group-sym">${sym}</span>
          <span class="alert-group-cnt">${entries.length} alert${entries.length>1?'s':''}</span>
          <span class="alert-group-cat">${s?(s.category||'').replace('_vol',' vol'):''}</span>
          <span class="alert-group-latest">latest ${fmtFired(last.ts)} · ${RULE_TITLE[last.rule_type]||last.rule_type}</span>
          ${unacked>0?`<button class="btn-reset" style="padding:3px 10px;font-size:17px;margin-right:6px" onclick="ackSymbol(event,'${sym}')">✓ Ack</button>`:''}
          <span class="alert-group-chevron ${isOpen?'open':''}" id="ag-chev-${sym}">▼</span>
        </div>
        <div class="alert-entries ${isOpen?'open':''}" id="ag-entries-${sym}">
          ${isOpen?entries.map(a=>alertEntryHTML(a,false)).join(''):''}
        </div>
      </div>`;
    }).join('');
  }
  el.innerHTML=html;
}

function toggleAlertGroup(sym){
  alertGroupOpen[sym]=!(alertGroupOpen[sym]===true);
  renderAlerts();
}

async function ackSymbol(e,sym){ e.stopPropagation(); await postJSON('/api/alerts/ack',{symbol:sym}); loadAll(); }
async function ackAllAlerts(){ await postJSON('/api/alerts/ack',{}); loadAll(); }
async function clearAllAlerts(){ if(!confirm('Delete ALL alert history? This cannot be undone.'))return; await postJSON('/api/alerts/clear',{}); loadAll(); }

// ── Settings tab ──────────────────────────────────────────────────────────────
const SETTINGS_FORM=[
  {group:'Monitoring',rows:[
    {key:'check_interval_seconds',label:'Check interval when open (seconds)',type:'number'},
    {key:'check_interval_closed_seconds',label:'Check interval when closed (seconds)',type:'number'},
    {key:'market_hours_only',label:'Only check during market hours',type:'toggle'},
    {key:'alert_cooldown_hours',label:'Alert cooldown (hours)',type:'number'},
    {key:'show_info_tier',label:'Show day-to-day activity (info tier)',type:'toggle',hint:'Computes the muted "activity" lines (moves that cross the looser pre-2026 thresholds but not the validated signal thresholds) — informational only, never a BUY/SELL claim. They appear on the cards when "More detail" is on.'},
    {key:'auto_recal_enabled',label:'Monthly automatic recalibration check',type:'toggle',hint:'About every 30 days at 03:00, Tripwire re-runs its backtest on fresh prices and emails you only if it suggests better thresholds. Nothing changes until you press Apply under Recalibration.'},
  ]},
  {group:'Notifications',rows:[
    {key:'daily_digest_enabled',label:'Daily digest instead of instant pings',type:'toggle',hint:'When on, suppresses instant push/email/WhatsApp and sends one summary per day instead (email + WhatsApp) — a better fit for the 1–5 day signal horizon. Alerts still appear live in the app.'},
    {key:'digest_hour',label:'Digest hour (0–23, local time)',type:'number',hint:'Hour of day the daily digest is sent.'},
    {key:'notify_strong_only',label:'Only push/email/WhatsApp for STRONG signals',type:'toggle',hint:'When on (default), outbound notifications only fire when at least 2 independent rules agree (STRONG BUY / STRONG BOUNCE WATCH). Every alert still appears in the Alerts tab regardless. Ignored while daily digest is on.'},
    {key:'browser_notifications_enabled',label:'Browser notifications',type:'toggle'},
    {key:'alert_sound_enabled',label:'Alert sound',type:'toggle'},
    {key:'notify_email_enabled',label:'Email notifications',type:'toggle'},
    {key:'smtp_host',label:'SMTP host',type:'text',hint:'e.g. smtp.gmail.com'},
    {key:'smtp_port',label:'SMTP port',type:'number'},
    {key:'smtp_user',label:'SMTP username (from address)',type:'text'},
    {key:'smtp_password',label:'SMTP password',type:'password',hint:'For Gmail, use an App Password. Leave blank to keep the stored one.'},
    {key:'notify_email_to',label:'Send alerts to (email)',type:'text'},
    {key:'notify_whatsapp_enabled',label:'WhatsApp notifications',type:'toggle'},
    {key:'callmebot_phone',label:'WhatsApp phone (with country code)',type:'text',hint:'Requires one-time CallMeBot setup — message their bot to get an API key. See callmebot.com/whatsapp-api'},
    {key:'callmebot_apikey',label:'CallMeBot API key',type:'password',hint:'Leave blank to keep the stored one.'},
  ]},
  {group:'AI',rows:[
    {key:'anthropic_api_key',label:'Anthropic API key',type:'password',hint:'Powers the news synthesis and the Assistant tab. Stored locally in the app database (not in the code or git). Leave blank to keep the stored one. Get a key at console.anthropic.com.'},
    {key:'ai_synthesis_model',label:'News-synthesis model',type:'text'},
    {key:'ai_assistant_model',label:'Assistant model',type:'text'},
  ]},
];

async function loadSettings(){
  appSettings=await fetchJSON('/api/settings');
  const html=SETTINGS_FORM.map(g=>`<div class="settings-group"><h3>${g.group}</h3>${
    g.rows.map(r=>{
      const v=appSettings[r.key]!=null?appSettings[r.key]:'';
      let input;
      if(r.type==='toggle'){
        input=`<label class="switch"><input type="checkbox" id="s_${r.key}" ${v==='1'?'checked':''}><span class="slider"></span></label>`;
      }else if(r.type==='password'){
        const ph=appSettings[r.key+'_set']?'•••••• (stored)':'';
        input=`<input type="password" id="s_${r.key}" placeholder="${ph}">`;
      }else{
        input=`<input type="${r.type}" id="s_${r.key}" value="${String(v).replace(/"/g,'&quot;')}">`;
      }
      return `<div class="set-row"><label>${r.label}</label>${input}</div>`+
             (r.hint?`<div class="set-hint">${r.hint}</div>`:'');
    }).join('')
  }</div>`).join('');
  const ownerOnly=guestMode?' disabled title="Owner only — not available with the guest password"':'';
  const guestNote=guestMode?`<div class="guest-settings-note">👀 You're signed in with the <strong>guest password</strong>: settings are shown read-only, and saving, test notifications and recalibration are owner-only. Log out and sign in with the main password to change them.</div>`:'';
  document.getElementById('pane-settings').innerHTML=guestNote+html+
    `<div class="settings-actions">
       <button id="btn-save-settings" onclick="saveSettings()"${ownerOnly}>Save settings</button>
       <button id="btn-test-notify" onclick="testNotify()"${ownerOnly}>Send test notification</button>
       <button class="btn-reset" onclick="enableBrowserNotifs()">Enable browser notifications</button>
       <span id="settings-status"></span>
     </div>`+
    `<div class="settings-group"><h3>Recalibration</h3>
       <div class="set-hint" style="margin-bottom:10px">Re-run the 5-year backtest on fresh data to re-tune every rule threshold, then review and apply the changes. Takes about a minute. It also runs by itself monthly (see Monitoring) and emails you when there's something to review.</div>
       <div class="settings-actions">
         <button id="btn-recal-run" onclick="recalRun()"${ownerOnly}>🔄 Recalibrate now${guestMode?' (owner only)':''}</button>
         <span id="recal-status" class="set-hint"></span>
       </div>
       <div id="recal-result" style="margin-top:12px"></div>
     </div>`+
    (guestMode?'':`<div class="settings-group"><h3>Access log</h3><div id="access-log">Loading…</div></div>`);
  refreshRecalStatus();
  if(!guestMode) loadAccessLog();
}

// ── Access log (owner only) ──────────────────────────────────────────────────
function fmtStamp(ts){
  const d=new Date(ts*1000), p=n=>String(n).padStart(2,'0');
  return `${d.getFullYear()}-${p(d.getMonth()+1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
function fmtTime(ts){ return fmtStamp(ts).slice(11); }
async function loadAccessLog(){
  const box=document.getElementById('access-log');
  if(!box) return;
  const d=await fetchJSON('/api/access-log');
  if(!d.success){ box.textContent='Could not load the access log.'; return; }
  const s=d.summary;
  const sumLine=(label,x)=>`<div class="al-sum"><strong>${label}:</strong> ${x.logins} login${x.logins===1?'':'s'} from ${x.ips} different address${x.ips===1?'':'es'}${x.last?` · last ${fmtStamp(x.last)}`:''}</div>`;
  const WHO={login:{guest:'👀 Guest',owner:'🔑 Main password'},login_failed:'⛔ Wrong password',login_expired:'⌛ Expired guest password'};
  const rows=d.logins.map((l,i)=>{
    const who=l.kind==='login'?WHO.login[l.role]:WHO[l.kind];
    const evs=(l.kind==='login'&&l.role==='guest')?(d.events[l.vid]||[]):null;
    const evCell=evs==null?'':(evs.length?`<button class="al-toggle" onclick="toggleAccessEvents(${i})">${evs.length} action${evs.length===1?'':'s'} ▸</button>`:'<span class="al-muted">no actions yet</span>');
    const evRow=evs&&evs.length?`<tr class="al-events" id="al-ev-${i}" style="display:none"><td colspan="5">${
      evs.map(e=>`<div><span class="al-muted">${fmtTime(e.ts)}</span> ${escapeHTML(e.detail)}</div>`).join('')}</td></tr>`:'';
    return `<tr><td>${fmtStamp(l.ts)}</td><td>${who}</td><td>${escapeHTML(l.ip||'')}</td><td>${escapeHTML(l.device||'')}</td><td>${evCell}</td></tr>${evRow}`;
  }).join('');
  box.innerHTML=sumLine('Guest password',s.guest)+sumLine('Main password',s.owner)+
    `<div class="al-sum"><strong>Failed attempts:</strong> ${s.failed}</div>`+
    `<div class="set-hint" style="margin:6px 0 10px">You get an email for every guest login, for main-password logins from a new device/address, and a guest-activity summary each evening. One friend on mobile data can appear under several addresses; people sharing a Wi-Fi appear as one.</div>`+
    (rows?`<div class="al-wrap"><table class="al-table"><thead><tr><th>Time</th><th>Who</th><th>Address</th><th>Device</th><th>Activity</th></tr></thead><tbody>${rows}</tbody></table></div>`
         :'<div class="al-muted">No logins recorded yet.</div>');
}
function toggleAccessEvents(i){
  const r=document.getElementById('al-ev-'+i); if(!r) return;
  const open=r.style.display==='none'; r.style.display=open?'table-row':'none';
  const b=r.previousElementSibling.querySelector('.al-toggle'); if(b) b.textContent=b.textContent.replace(open?'▸':'▾',open?'▾':'▸');
}

// ── One-click recalibration ───────────────────────────────────────────────────
let recalPoll=null;
async function recalRun(){
  const btn=document.getElementById('btn-recal-run'); if(btn) btn.disabled=true;
  document.getElementById('recal-result').innerHTML='';
  try{ await postJSON('/api/recalibrate/run',{}); }
  catch(e){ if(btn) btn.disabled=guestMode; throw e; }
  pollRecal();
}
function pollRecal(){
  if(recalPoll) clearInterval(recalPoll);
  recalPoll=setInterval(refreshRecalStatus,3000);
  refreshRecalStatus();
}
async function refreshRecalStatus(){
  let st; try{ st=await fetchJSON('/api/recalibrate/status'); }catch(e){ return; }
  const status=document.getElementById('recal-status');
  const btn=document.getElementById('btn-recal-run');
  if(!status) return;
  if(st.running){
    status.textContent='⏳ '+(st.phase||'working')+'…';
    if(btn) btn.disabled=true;
    if(!recalPoll) pollRecal();
    return;
  }
  if(btn) btn.disabled=guestMode;
  if(recalPoll){ clearInterval(recalPoll); recalPoll=null; }
  if(st.phase==='done'){
    status.textContent=st.rc===0?'✓ done':'⚠ finished with issues (rc '+st.rc+')';
    renderRecalResult(st);
  }else{
    status.textContent='';
  }
}
function renderRecalResult(st){
  const box=document.getElementById('recal-result'); if(!box) return;
  const diff=st.diff||[];
  if(st.rc!==0){
    box.innerHTML=`<div class="err-banner" style="white-space:pre-wrap">${escapeHTML(st.tail||'Recalibration failed.')}</div>`;
    return;
  }
  if(!diff.length){
    box.innerHTML=`<div class="set-hint">✓ Recalibration complete — no threshold changes recommended (already up to date).</div>`;
    return;
  }
  const rows=diff.slice(0,60).map(c=>`<tr><td style="text-align:left">${c.symbol}</td><td style="text-align:left">${c.key}</td><td class="muted">${c.from==null?'—':c.from}</td><td>→</td><td class="up">${c.to}</td></tr>`).join('');
  box.innerHTML=`<div class="set-hint" style="margin-bottom:8px">${diff.length} proposed change${diff.length>1?'s':''}:</div>
    <table class="outcome-table"><tbody>${rows}</tbody></table>
    <div class="settings-actions" style="margin-top:12px">
      <button id="btn-save-settings" onclick="recalApply()">✓ Apply changes</button>
      <button class="btn-reset" onclick="recalUndo()">↩ Undo last apply</button>
    </div>`;
}
async function recalApply(){ await postJSON('/api/recalibrate/apply',{}); document.getElementById('recal-status').textContent='⏳ applying…'; pollRecal(); setTimeout(loadAll,4000); }
async function recalUndo(){ await postJSON('/api/recalibrate/undo',{}); document.getElementById('recal-status').textContent='⏳ undoing…'; pollRecal(); setTimeout(loadAll,4000); }

async function saveSettings(){
  const body={};
  SETTINGS_FORM.forEach(g=>g.rows.forEach(r=>{
    const el=document.getElementById('s_'+r.key);
    if(!el) return;
    if(r.type==='toggle') body[r.key]=el.checked?'1':'0';
    else if(r.type==='password'){ if(el.value!=='') body[r.key]=el.value; }
    else body[r.key]=el.value;
  }));
  await postJSON('/api/settings',body);
  appSettings=await fetchJSON('/api/settings');
  const st=document.getElementById('settings-status'); st.textContent='✓ Saved'; setTimeout(()=>st.textContent='',2500);
}

async function testNotify(){
  const st=document.getElementById('settings-status');
  st.textContent='Sending… (waiting for real delivery result, up to ~20s)';
  const r=await postJSON('/api/settings/test-notify',{});
  const line=(label,res)=>`${res.ok?'✓':'✗'} ${label}: ${res.message}`;
  st.innerHTML=`<div style="color:${r.email.ok?'#10B981':'#EF4444'}">${line('Email',r.email)}</div>`+
               `<div style="color:${r.whatsapp.ok?'#10B981':'#EF4444'}">${line('WhatsApp',r.whatsapp)}</div>`+
               `<div class="set-hint" style="margin-top:4px">Just edited credentials? Save settings first, then test again.</div>`;
}

function enableBrowserNotifs(){
  if(!('Notification' in window)){alert('This browser does not support notifications.');return;}
  Notification.requestPermission().then(p=>{
    const st=document.getElementById('settings-status');
    if(st) st.textContent=p==='granted'?'✓ Browser notifications enabled':'Permission '+p;
  });
}

// ── Analytics tab ─────────────────────────────────────────────────────────────
const RULE_DISPLAY_NAME={volatility:'Unusual Move',support_resistance:'S/R Breakout',consecutive_down:'Consec Down',volume:'Volume Spike',gap:'Gap',rsi:'RSI',ma_cross:'MA Cross'};

// Performance scorecard: does the app's own signal actually work? Everything here is built
// from resolved outcomes — each alert scored ~5 trading days after it fired, vs SPY.
async function loadAnalytics(){
  const d=await fetchJSON('/api/analytics');
  const pane=document.getElementById('pane-analytics');
  const h=d.headline||{};
  const pend=d.outcomes_pending?`<div class="set-hint" style="margin-top:4px">${d.outcomes_pending} signal${d.outcomes_pending>1?'s':''} still maturing (each is scored ~5 trading days after it fires).</div>`:'';

  if(!h.n){
    pane.innerHTML=`<h2 class="perf-title">📊 Performance</h2>
      <div class="analytics-card"><div class="set-hint">No signals have matured yet. Once an alert is ~5 trading days old, Tripwire scores what actually happened (the stock's move and its ${linkifyGlossary('excess return')} vs the S&P 500) and reports here whether its signals are working.</div>${pend}</div>`;
    return;
  }

  // Headline
  const hitClr=h.pct_correct>=55?'#10B981':h.pct_correct>=50?'#F59E0B':'#EF4444';
  const excClr=h.avg_excess>=0?'#10B981':'#EF4444';
  const headlineHTML=`<div class="perf-headline">
    <div class="perf-hero"><div class="perf-hero-num" style="color:${hitClr}">${h.pct_correct}%</div><div class="perf-hero-lbl">of ${h.n} signals beat the market over the next 5 days</div></div>
    <div class="perf-hero"><div class="perf-hero-num" style="color:${excClr}">${h.avg_excess>=0?'+':''}${h.avg_excess}%</div><div class="perf-hero-lbl">avg ${linkifyGlossary('excess return')} vs S&P 500 over 5 days</div></div>
  </div>`;

  // Calibration nudge
  let calibHTML='';
  if(d.calibration){
    const c=d.calibration;
    if(c.drifting){
      calibHTML=`<div class="perf-drift">⚠ Live hit rate (${c.live_hit}%) is tracking below the backtested ${c.bt_hit}% — the market may have shifted. Consider <a class="gloss-link" href="javascript:void(0)" onclick="switchTab('settings',tabBtn('settings'));setTimeout(()=>document.getElementById('btn-recal-run')&&document.getElementById('btn-recal-run').scrollIntoView({block:'center'}),300);return false;">recalibrating</a>.</div>`;
    }else{
      calibHTML=`<div class="perf-ok">✓ Live hit rate (${c.live_hit}%) is in line with the backtested ${c.bt_hit}% — calibration looks healthy.</div>`;
    }
  }

  // Per-rule live vs backtested
  const cell=(live,bt,suffix)=>{
    if(live==null) return '<td class="muted">—</td>';
    const cls=bt==null?'':(live>=bt-0.01?'up':'dn');
    return `<td class="${cls}">${live>=0?'+':''}${live}${suffix}<span class="muted" style="font-size:11px"> vs ${bt!=null?(bt>=0?'+':'')+bt+suffix:'—'}</span></td>`;
  };
  const ruleBody=(d.outcomes||[]).map(r=>`<tr>
    <td style="text-align:left">${RULE_DISPLAY_NAME[r.rule_type]||r.rule_type}</td>
    <td>${r.n}</td>${cell(r.live_hit,r.bt_hit,'%')}${cell(r.live_excess,r.bt_excess,'%')}
  </tr>`).join('');
  const ruleCard=`<div class="analytics-card"><h3>By rule — live vs backtested</h3>
    <table class="outcome-table"><thead><tr><th style="text-align:left">Rule</th><th>Scored</th><th>${linkifyGlossary('Hit rate')}</th><th>Avg ${linkifyGlossary('excess return')} 5d</th></tr></thead>
    <tbody>${ruleBody}</tbody></table>
    <div class="set-hint" style="margin-top:8px">Green = live is meeting or beating the backtest; red = underperforming. Small "Scored" counts are noisy.</div></div>`;

  // Per-stock realized edge
  const stockBody=(d.by_stock||[]).map(s=>{
    const cls=s.avg_excess>=0?'up':'dn';
    return `<tr><td style="text-align:left">${s.symbol}</td><td>${s.n}</td>
      <td class="${cls}">${s.avg_excess>=0?'+':''}${s.avg_excess}%</td><td>${s.hit}%</td></tr>`;
  }).join('');
  const stockCard=(d.by_stock||[]).length?`<div class="analytics-card"><h3>By stock — which signals paid off</h3>
    <table class="outcome-table"><thead><tr><th style="text-align:left">Stock</th><th>Scored</th><th>Avg ${linkifyGlossary('excess return')} 5d</th><th>${linkifyGlossary('Hit rate')}</th></tr></thead>
    <tbody>${stockBody}</tbody></table></div>`:'';

  // Money + your own decisions
  const eur=v=>(v>=0?'+':'−')+'€'+Math.abs(Math.round(v)).toLocaleString('en-GB');
  const m=d.money, dec=d.decisions||{};
  const moneyCard=m?`<div class="analytics-card"><h3>In money</h3>
    <div class="perf-money" style="color:${m.vs_market>=0?'#10B981':'#EF4444'}">${eur(m.vs_market)} vs the market</div>
    <div class="set-hint">Putting €1,000 into each of the ${m.n} STRONG signal${m.n>1?'s':''} for 5 trading days (BUY and BOUNCE WATCH alike — both are calls that the stock will do well) would have left you ${eur(m.vs_market)} compared with the same money in the S&P 500; ${eur(m.raw)} in plain gains/losses. Before fees and taxes; past signals only.</div></div>`:'';
  const decLine=(lbl,g)=>g?`<div class="al-sum"><strong>${lbl}:</strong> <span class="${g.avg_excess>=0?'up':'dn'}">${g.avg_excess>=0?'+':''}${g.avg_excess}%</span> vs the market on average · ${g.hit}% beat it · ${g.n} signal${g.n>1?'s':''}</div>`:'';
  let decBody;
  if(dec.acted||dec.passed){
    decBody=decLine('When you acted',dec.acted)+decLine('When you passed',dec.passed);
    if(dec.acted&&dec.passed){
      const gap=+(dec.acted.avg_excess-dec.passed.avg_excess).toFixed(2);
      decBody+=`<div class="set-hint" style="margin-top:6px">${gap>=0?`The signals you acted on did ${gap} pts better than the ones you skipped — your judgement is adding value.`:`The signals you skipped did ${Math.abs(gap)} pts better than the ones you acted on — following every signal would have done better.`} Small samples are noisy.</div>`;
    }
    if(dec.unmarked) decBody+=`<div class="set-hint">${dec.unmarked} STRONG signal${dec.unmarked>1?'s':''} not marked — mark them in the table below.</div>`;
  }else{
    decBody=`<div class="set-hint">Mark STRONG signals with <b>I acted</b> / <b>I passed</b> (in the email, on the Stocks page, or in the table below). This card then shows whether your own choices beat simply following every signal.</div>`;
  }
  const decisionsCard=m?`<div class="analytics-card"><h3>Your decisions</h3>${decBody}</div>`:'';

  // Recent resolved signals (the receipts) — one row per signal, not per rule
  const decCell=r=>{
    if(r.decision) return `<span class="dec-tag ${r.decision}">${r.decision}</span>`;
    if(!r.strong||guestMode) return '<span class="muted">—</span>';
    const ids=r.ids.join(',');
    return `<button class="dec-btn acted" onclick="markDecision('${ids}','acted')">acted</button> <button class="dec-btn passed" onclick="markDecision('${ids}','passed')">passed</button>`;
  };
  const recentBody=(d.recent||[]).map(r=>{
    const buy=r.label.includes('BUY');
    return `<tr><td style="text-align:left">${r.date}</td><td style="text-align:left">${r.symbol}</td>
      <td style="text-align:left"><span class="${buy?'rt-buy':'rt-watch'}">${r.label}</span></td>
      <td class="${r.ret>=0?'up':'dn'}">${r.ret>=0?'+':''}${r.ret}%</td>
      <td class="muted">${r.market>=0?'+':''}${r.market}%</td>
      <td class="${r.excess>=0?'up':'dn'}">${r.excess>=0?'✓ +':'✗ '}${r.excess}</td>
      <td>${decCell(r)}</td></tr>`;
  }).join('');
  const recentCard=(d.recent||[]).length?`<div class="analytics-card"><h3>Recent signals, 5 trading days later</h3>
    <div class="al-wrap" style="max-height:none"><table class="outcome-table"><thead><tr><th style="text-align:left">Date</th><th style="text-align:left">Stock</th><th style="text-align:left">Signal</th><th>Stock</th><th>Market</th><th>vs market</th><th>You</th></tr></thead>
    <tbody>${recentBody}</tbody></table></div>
    <div class="set-hint" style="margin-top:8px">✓ = the stock beat the S&P 500 over the next 5 trading days. A BOUNCE WATCH is a rebound call, so it counts as right when the stock recovers ahead of the market.</div></div>`:'';

  pane.innerHTML=`<h2 class="perf-title">📊 Performance</h2>
    <div class="set-hint" style="margin:-6px 0 14px">How Tripwire's own signals actually played out — the app scoring itself on forward data. One move that tripped several rules counts as one signal.${pend?' '+d.outcomes_pending+' alert'+(d.outcomes_pending>1?'s':'')+' still maturing.':''}</div>
    ${headlineHTML}${calibHTML}${moneyCard}${decisionsCard}${recentCard}${stockCard}${ruleCard}`;
}

// ── Portfolio test tab ────────────────────────────────────────────────────────
const PF_LINES=[['strategy','Following the app','#F59E0B',''],['untouched','Same stocks, never traded','#93C5FD','6 5'],['spy','S&P 500','#6B7280','2 4']];
let pfPoll=null;
const usd=v=>(v<0?'−':'')+'$'+Math.abs(Math.round(v)).toLocaleString('en-US');
const pctS=v=>v==null?'—':(v>=0?'+':'')+v.toFixed(1)+'%';
function fmtDay(d){ return new Date(d+'T12:00:00').toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'}); }

async function loadPortfolio(){
  let d;
  try{ d=await fetchJSON('/api/portfolio-sim'); }catch(e){ return; }
  const pane=document.getElementById('pane-portfolio');
  if(!pane||pane.style.display==='none') return;
  pane.innerHTML=`<h2 class="perf-title">💼 Portfolio test</h2>
    <div class="set-hint" style="margin:-6px 0 14px">What would $1,000 have done if you followed every STRONG signal? It starts split equally across your watchlist. Each signal moves 10% of the portfolio into that stock for 5 trading days, taken from the other stocks and returned to them afterwards. The comparison with <b>the same stocks, never traded</b> shows what the app's rules add on top of your stock picks; the S&P 500 shows the market.</div>
    <div class="pf-grid">${pfColumn('backtest',d)}${pfColumn('live',d)}</div>
    <div class="pf-note">Rules: at most 10 signal slices at once (more are skipped); a repeat signal on a boosted stock extends it; 0.1% trading cost on every buy and sell; no new money, no borrowing, no short selling; prices include dividends. Backtest buys at the next day's open, live at the alert price. Before taxes. Past results don't guarantee future ones.</div>`;
  clearTimeout(pfPoll);
  if(d.backtest_updating) pfPoll=setTimeout(loadPortfolio,8000);
}

function pfColumn(kind,d){
  const r=kind==='backtest'?d.backtest:d.live;
  const title=kind==='backtest'?'Backtest — last 5 years':'Live — from today on';
  if(kind==='backtest'&&(!r||d.backtest_updating)){
    const why=d.backtest_error?`Couldn't build it: ${escapeHTML(d.backtest_error.slice(-200))}`
      :(r?'Updating for your latest rule changes…':'Building the 5-year simulation with your current rules… (about 10 seconds)');
    if(!r||d.backtest_error) return `<div class="pf-col"><div class="pf-kind">${title}</div><div class="pf-period">${why}</div></div>`;
  }
  if(kind==='live'&&(!r||!r.summary)){
    return `<div class="pf-col"><div class="pf-kind">${title}</div>
      <div class="pf-period">Started ${r?fmtDay(r.start):'today'}</div>
      <div class="pf-big">$1,000</div>
      <div class="pf-sub muted">Values appear after the first market close. The first trade appears when the next STRONG signal fires.</div>
      ${pfRestartLink()}</div>`;
  }
  const s=r.summary, st=s.strategy, un=s.untouched, sp=s.spy;
  const clr=st.change>0.5?'#10B981':st.change<-0.5?'#EF4444':'#D1D5DB';
  const arrow=st.change>0.5?'▲':st.change<-0.5?'▼':'■';
  const perYear=st.cagr_pct!=null;
  const diff=perYear?+(st.cagr_pct-un.cagr_pct).toFixed(1):+(st.total_pct-un.total_pct).toFixed(1);
  const unit=perYear?' pts per year':' pts so far';
  const verdictCls=Math.abs(diff)<0.25?'flat':diff>0?'good':'bad';
  const verdict=Math.abs(diff)<0.25?`The signals made no real difference compared with never trading (${diff>=0?'+':''}${diff}${unit}).`
    :diff>0?`The signals added +${diff}${unit} compared with never trading these stocks.`
    :`The signals cost ${Math.abs(diff)}${unit} compared with never trading these stocks.`;
  const row=(key,label,color,x)=>`<tr><td><span class="pf-dot" style="background:${color}"></span>${label}</td>
    <td class="r"><b>${usd(x.end)}</b></td><td class="r ${x.total_pct>=0?'up':'dn'}">${pctS(x.total_pct)}</td>
    <td class="r muted">${x.cagr_pct!=null?pctS(x.cagr_pct)+'/yr':''}</td></tr>`;
  const rows=PF_LINES.map(([k,l,c])=>row(k,l,c,s[k])).join('');
  let fair='';
  if(kind==='backtest'&&r.fair_summary){
    const f=r.fair_summary;
    fair=`<div class="pf-fair">From ${fmtDay(r.fair_from)} (right of the dotted line) the rules were never tuned on the data — the fairest test: app ${pctS(f.strategy.total_pct)} · never traded ${pctS(f.untouched.total_pct)} · S&P 500 ${pctS(f.spy.total_pct)}.</div>`;
  }
  const nTrades=r.trades_total!=null?r.trades_total:(r.trades||[]).length;
  const counts=`<div class="set-hint">${nTrades} signal trade${nTrades===1?'':'s'} completed · ${(r.open_slices||[]).length} open now · ${(r.skipped||[]).length} skipped</div>`;
  const ps=(r.per_stock||[]).filter(p=>p.total_pnl!==0||p.slices);
  const mv=p=>`<div><b>${p.sym}</b> <span class="${p.total_pnl>=0?'up':'dn'}">${p.total_pnl>=0?'+':''}${usd(p.total_pnl)}</span></div>`;
  const movers=ps.length?`<div class="pf-movers"><div><h4>Biggest gainers</h4>${ps.slice(0,3).filter(p=>p.total_pnl>0).map(mv).join('')||'<div class="muted">—</div>'}</div>
    <div><h4>Biggest losers</h4>${ps.slice(-3).reverse().filter(p=>p.total_pnl<0).map(mv).join('')||'<div class="muted">none lost money</div>'}</div></div>`:'';
  const table=ps.length?`<details><summary>All stocks</summary><table class="outcome-table"><thead><tr><th style="text-align:left">Stock</th><th>Held, untouched</th><th>Signal trades</th><th>From signals</th><th>Total</th></tr></thead><tbody>${
    ps.map(p=>`<tr><td style="text-align:left">${p.sym}</td><td>${p.untouched_pct!=null?pctS(p.untouched_pct):'<span class="muted">not held</span>'}</td><td>${p.slices}</td>
      <td class="${p.slice_pnl>=0?'up':'dn'}">${p.slice_pnl>=0?'+':''}${usd(p.slice_pnl)}</td><td class="${p.total_pnl>=0?'up':'dn'}">${p.total_pnl>=0?'+':''}${usd(p.total_pnl)}</td></tr>`).join('')
    }</tbody></table><div class="set-hint" style="margin-top:6px">"From signals" = profit or loss of that stock's 10% signal slices. "Total" also includes the stock's starting holding.</div></details>`:'';
  let history='';
  if(kind==='backtest'&&(d.backtest_runs||[]).length>1){
    history=`<details><summary>Earlier backtest runs</summary>${d.backtest_runs.map(x=>`<div class="set-hint">${fmtStamp(x.ts).slice(0,10)}: app ${pctS(x.strategy_cagr)}/yr · never traded ${pctS(x.untouched_cagr)}/yr · S&P 500 ${pctS(x.spy_cagr)}/yr</div>`).join('')}</details>`;
  }
  const period=kind==='backtest'?`${fmtDay(r.start_date)} → ${fmtDay(r.end_date)} · same rules as today${d.backtest_updating?' · updating…':''}`
    :`Since ${fmtDay(r.start_date)} · ${s.strategy.cagr_pct==null?'per-year figures appear after 90 days':''}`;
  return `<div class="pf-col">
    <div class="pf-kind">${title}</div><div class="pf-period">${period}</div>
    <div class="pf-big" style="color:${clr}">$1,000 → ${usd(st.end)} ${arrow}</div>
    <div class="pf-sub"><span style="color:${clr}">${st.change>=0?'+':''}${usd(st.change)} · ${pctS(st.total_pct)}</span>${perYear?` · ${pctS(st.cagr_pct)} per year`:''} <span class="muted">· worst drop ${st.worst_drop_pct}%</span></div>
    <table class="pf-rows">${rows}</table>
    <div class="pf-verdict ${verdictCls}">${verdict}</div>
    ${fair}
    <div class="pf-chart">${multiLineSVG(r.equity||[],kind==='backtest'?r.fair_from:null)}</div>
    <div class="pf-legend">${PF_LINES.map(([k,l,c])=>`<span class="pf-dot" style="background:${c}"></span>${l}`).join(' &nbsp; ')}</div>
    ${counts}${movers}${table}${history}${kind==='live'?pfRestartLink():''}
  </div>`;
}

function pfRestartLink(){
  if(guestMode) return '';
  return `<div class="set-hint" style="margin-top:10px"><a href="javascript:void(0)" class="today-more" onclick="restartLiveTest()">Restart the live test from today</a> — e.g. after applying new thresholds.</div>`;
}
async function restartLiveTest(){
  if(!confirm('Restart the live portfolio test from today? Its history so far is cleared.')) return;
  await postJSON('/api/portfolio-sim/restart',{});
  showToast('Live test restarted from today.');
  loadPortfolio();
}

function multiLineSVG(eq,markerDate){
  if(eq.length<2) return '<div class="set-hint">Not enough days yet for a chart.</div>';
  const W=760,H=240,pT=12,pB=26,pL=58,pR=10, plotW=W-pL-pR, plotH=H-pT-pB;
  const vals=eq.flatMap(e=>[e.strategy,e.untouched,e.spy]);
  const lo=Math.min(...vals), hi=Math.max(...vals), span=(hi-lo)||1;
  const x=i=>pL+(i/(eq.length-1))*plotW, y=v=>pT+plotH-((v-lo)/span)*plotH;
  const grid=[0,0.5,1].map(f=>{const v=lo+span*f;return `<line x1="${pL}" x2="${W-pR}" y1="${y(v)}" y2="${y(v)}" stroke="#1E2235"/><text x="${pL-6}" y="${y(v)+4}" text-anchor="end" font-size="11" fill="#6B7280">${usd(v)}</text>`;}).join('');
  const base=lo<=1000&&hi>=1000?`<line x1="${pL}" x2="${W-pR}" y1="${y(1000)}" y2="${y(1000)}" stroke="#374151" stroke-dasharray="3 3"/>`:'';
  const lines=[...PF_LINES].reverse().map(([k,,c,dash])=>`<polyline fill="none" stroke="${c}" stroke-width="${k==='strategy'?2.4:1.6}" ${dash?`stroke-dasharray="${dash}"`:''} points="${eq.map((e,i)=>x(i).toFixed(1)+','+y(e[k]).toFixed(1)).join(' ')}"/>`).join('');
  let marker='';
  if(markerDate){
    const mi=eq.findIndex(e=>e.date>=markerDate);
    if(mi>0) marker=`<line x1="${x(mi)}" x2="${x(mi)}" y1="${pT}" y2="${pT+plotH}" stroke="#93C5FD" stroke-dasharray="2 3"/><text x="${x(mi)+4}" y="${pT+10}" font-size="10" fill="#93C5FD">not tuned on →</text>`;
  }
  const xl=[0,Math.floor((eq.length-1)/2),eq.length-1].map((i,j)=>`<text x="${x(i)}" y="${H-6}" font-size="11" fill="#6B7280" text-anchor="${['start','middle','end'][j]}">${fmtDay(eq[i].date)}</text>`).join('');
  return `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Portfolio value over time">${grid}${base}${marker}${lines}${xl}</svg>`;
}

// ── Assistant tab ─────────────────────────────────────────────────────────────
let chatBusy=false, chatHistoryLoaded=false;
const CHAT_SUGGESTIONS=[
  'Why did my most active stock move today?',
  'Give me a narrative summary of my whole watchlist',
  'Make NVDA volatility rule less sensitive',
  'Enable the RSI rule on all my high-vol stocks',
  'Set the check interval to 5 minutes',
  'Research upcoming earnings across my watchlist',
];

function escapeHTML(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function escAttr(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function mdLite(s){
  return escapeHTML(s)
    .replace(/\*\*([^*]+)\*\*/g,'<strong>$1</strong>')
    .replace(/`([^`]+)`/g,'<code>$1</code>')
    .replace(/\n/g,'<br>');
}

async function loadChatHistory(){
  renderChatQuota();
  if(guestMode) document.getElementById('chat-input').placeholder='Ask about a stock, a move, or what a signal means…';
  if(!aiEnabled){
    document.getElementById('chat-scroll').innerHTML=
      '<div class="ai-disabled">🤖 The AI assistant needs an Anthropic API key.<br><br>'+
      'Set <code>ANTHROPIC_API_KEY</code> in the environment and restart Tripwire to enable it. '+
      'The same key also powers the automatic news synthesis on alerts.</div>';
    document.getElementById('chat-input').disabled=true;
    document.getElementById('chat-send').disabled=true;
    return;
  }
  document.getElementById('chat-input').disabled=false;
  document.getElementById('chat-send').disabled=false;
  if(chatHistoryLoaded) return;
  chatHistoryLoaded=true;
  const d=await fetchJSON('/api/ai/history');
  const scroll=document.getElementById('chat-scroll');
  scroll.innerHTML='';
  if(!d.messages||!d.messages.length){ renderSuggestions(); }
  else d.messages.forEach(m=>addChatBubble(m.role,m.content));
  scrollChat();
}

const GUEST_CHAT_SUGGESTIONS=[
  'Why did the most active stock move today?',
  'Give me a short summary of the whole watchlist',
  'What does a STRONG BUY signal mean here?',
  'Any upcoming earnings on the watchlist?',
];
function renderSuggestions(){
  const scroll=document.getElementById('chat-scroll');
  const intro=guestMode?'Ask anything about the watchlist — the assistant can look things up and search the web.'
                       :'Ask me anything about your watchlist, or tell me to change a rule or setting.';
  const list=guestMode?GUEST_CHAT_SUGGESTIONS:CHAT_SUGGESTIONS;
  scroll.innerHTML=`<div style="color:#6B7280;font-size:13px;margin-bottom:14px">${intro}</div>`+
    '<div class="chat-suggestions">'+list.map(s=>`<div class="chat-chip" onclick="useSuggestion(this)">${s}</div>`).join('')+'</div>';
}
function useSuggestion(el){ document.getElementById('chat-input').value=el.textContent; sendChat(); }

function addChatBubble(role,content){
  const scroll=document.getElementById('chat-scroll');
  const div=document.createElement('div');
  div.className='chat-msg '+role;
  div.innerHTML=`<div class="chat-bubble">${role==='assistant'?mdLite(content):escapeHTML(content)}</div>`;
  scroll.appendChild(div);
  return div.querySelector('.chat-bubble');
}
function addToolChip(label){
  const scroll=document.getElementById('chat-scroll');
  const div=document.createElement('div');
  div.innerHTML=`<span class="chat-tool">⚙ ${escapeHTML(label)}</span>`;
  scroll.appendChild(div);
  scrollChat();
}
function scrollChat(){ const s=document.getElementById('chat-scroll'); s.scrollTop=s.scrollHeight; }

function chatKey(e){ if(e.key==='Enter'&&!e.shiftKey){ e.preventDefault(); sendChat(); } }

async function sendChat(){
  if(chatBusy||!aiEnabled) return;
  const inp=document.getElementById('chat-input');
  const msg=inp.value.trim();
  if(!msg) return;
  // clear suggestions on first message
  const sug=document.querySelector('.chat-suggestions'); if(sug) document.getElementById('chat-scroll').innerHTML='';
  inp.value=''; chatBusy=true;
  document.getElementById('chat-send').disabled=true;
  addChatBubble('user',msg);
  scrollChat();
  let bubble=null, acc='';
  try{
    const resp=await fetch('/api/ai/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:msg})});
    if(resp.status===401){location.href='/login';return;}
    if(!resp.ok){
      const e=await resp.json().catch(()=>({error:'request failed'}));
      if(e.guest_ai_left!=null){ guestAiLeft=e.guest_ai_left; renderChatQuota(); }
      addChatBubble('assistant','⚠ '+(e.error||'error')); throw new Error(e.error);
    }
    const reader=resp.body.getReader();
    const dec=new TextDecoder();
    let buf='';
    while(true){
      const {value,done}=await reader.read();
      if(done) break;
      buf+=dec.decode(value,{stream:true});
      let idx;
      while((idx=buf.indexOf('\n\n'))>=0){
        const chunk=buf.slice(0,idx); buf=buf.slice(idx+2);
        const line=chunk.split('\n').find(l=>l.startsWith('data:'));
        if(!line) continue;
        let ev; try{ ev=JSON.parse(line.slice(5).trim()); }catch(e){ continue; }
        if(ev.type==='text'){ if(!bubble) bubble=addChatBubble('assistant',''); acc+=ev.text; bubble.innerHTML=mdLite(acc); scrollChat(); }
        else if(ev.type==='tool'){ addToolChip(ev.label); }
        else if(ev.type==='error'){ if(!bubble) bubble=addChatBubble('assistant',''); acc+='\n⚠ '+ev.error; bubble.innerHTML=mdLite(acc); }
        else if(ev.type==='done'){
          if(ev.guest_ai_left!=null){ guestAiLeft=ev.guest_ai_left; renderChatQuota(); }
          if(ev.refresh){ chatHistoryLoaded=true; loadAll(); }
        }
      }
    }
  }catch(e){ /* already shown */ }
  finally{ chatBusy=false; document.getElementById('chat-send').disabled=false; scrollChat(); }
}

async function clearChat(){
  if(!confirm('Start a new conversation? This clears the history.')) return;
  await postJSON('/api/ai/clear',{});
  chatHistoryLoaded=false;
  document.getElementById('chat-scroll').innerHTML='';
  renderSuggestions();
}
</script>
</body>
</html>"""

@app.route("/")
@login_required
def dashboard():
    return Response(DASHBOARD, mimetype="text/html")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  Tripwire v4 starting...")
    print("  Dashboard: http://localhost:5000")
    print("  Press Ctrl+C to stop")
    print("="*60 + "\n")
    app.run(host="0.0.0.0", port=5000, use_reloader=False)
