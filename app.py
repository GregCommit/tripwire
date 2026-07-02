"""
Tripwire v4 - Single file, self-contained
==========================================
Run:  python app.py
Open: http://localhost:5000
"""

import sqlite3, threading, time, json, os, logging, csv, io, smtplib, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
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
            CREATE INDEX IF NOT EXISTS idx_prices_symbol_ts ON prices(symbol, timestamp);
            CREATE INDEX IF NOT EXISTS idx_alerts_symbol_rule_ts ON alerts(symbol, rule_type, timestamp);
        """)
        for tbl, col, coldef in [
            ("alerts", "detail", "TEXT"),
            ("alerts", "ack", "INTEGER DEFAULT 0"),
            ("prices", "volume", "REAL"),
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

# ─────────────────────────────────────────────────────────────────────────────
# RULE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# Params shared by every category (rule toggles + new-rule thresholds).
_COMMON_RULE_DEFAULTS = {
    "enable_volatility": True,
    "enable_support_resistance": True,
    # Volume spike
    "enable_volume": True, "volume_multiplier": 3.0, "volume_lookback": 20,
    # Gap at open
    "enable_gap": True, "gap_pct": 3.0,
    # RSI(14)
    "enable_rsi": True, "rsi_period": 14, "rsi_overbought": 70, "rsi_oversold": 30,
    # Moving-average crossover
    "enable_ma": True, "ma_short": 20, "ma_long": 50, "ma_cross_lookback": 3,
}

def _mk_rules(base):
    d = dict(_COMMON_RULE_DEFAULTS)
    d.update(base)
    return d

DEFAULT_RULES = {
    "high_vol": _mk_rules({"volatility_multiplier":2.5,"volatility_lookback":20,"support_resist_pct":7.0,"support_resist_lookback":90,"consecutive_down_days":3,"use_consecutive":True}),
    "mod_vol":  _mk_rules({"volatility_multiplier":1.5,"volatility_lookback":20,"support_resist_pct":5.0,"support_resist_lookback":30,"consecutive_down_days":3,"use_consecutive":False,"volume_multiplier":2.5,"gap_pct":2.0}),
    "low_vol":  _mk_rules({"volatility_multiplier":1.2,"volatility_lookback":20,"support_resist_pct":4.0,"support_resist_lookback":60,"consecutive_down_days":3,"use_consecutive":False,"volume_multiplier":2.0,"gap_pct":1.5}),
}

def get_stocks():
    with get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM stocks WHERE active=1 ORDER BY symbol")]

def get_params(symbol, category):
    base = DEFAULT_RULES.get(category, DEFAULT_RULES["high_vol"]).copy()
    with get_db() as conn:
        row = conn.execute("SELECT params FROM rule_params WHERE symbol=?", (symbol,)).fetchone()
    if row:
        saved = json.loads(row["params"])
        base.update(saved)  # saved overrides defaults; new keys fall back to defaults
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
                day_ts = int(ts.timestamp())
                prev_close = round(float(rows[i-1][1]["Close"]), 2) if i > 0 else round(float(row["Close"]), 2)
                vol = row.get("Volume")
                conn.execute("""
                    INSERT OR IGNORE INTO prices
                    (symbol,timestamp,open,high,low,close,prev_close,pre_market,post_market,volume)
                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (symbol, day_ts,
                     round(float(row["Open"]),2), round(float(row["High"]),2),
                     round(float(row["Low"]),2),  round(float(row["Close"]),2),
                     prev_close, None, None,
                     int(vol) if vol and vol == vol else None))
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
            "SELECT timestamp,close,high,low,volume FROM prices WHERE symbol=? AND timestamp>? ORDER BY timestamp ASC",
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

def log_alert(symbol, rule_type, message, price, detail=None):
    now = int(time.time())
    with get_db() as conn:
        recent = conn.execute(
            "SELECT id FROM alerts WHERE symbol=? AND rule_type=? AND timestamp>? LIMIT 1",
            (symbol, rule_type, now - _cooldown_seconds())
        ).fetchone()
        if recent:
            return
        conn.execute(
            "INSERT INTO alerts (symbol,timestamp,rule_type,message,price,detail,ack) VALUES (?,?,?,?,?,?,0)",
            (symbol, now, rule_type, message, price, detail)
        )
        conn.commit()
    # New alert actually inserted — fire outbound notifications off the check thread.
    worker_pool.submit(notify_alert, symbol, rule_type, message, price)

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

def synthesize_news(symbol, news_items, move_pct, direction, rule_label):
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
        prompt = (
            f"{symbol} triggered a {rule_label} alert. The stock moved {abs(move_pct):.1f}% {dir_word} today.\n\n"
            f"Recent news headlines:\n{headlines}\n\n"
            f"In 2-3 sentences, explain what may be driving this unusual move based on the news. "
            f"Be concise and specific to {symbol}.{_earnings_hint(symbol)}"
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

def _send_email(subject, body):
    if not get_setting_bool("notify_email_enabled"):
        return
    host = get_setting("smtp_host", ""); user = get_setting("smtp_user", "")
    pwd  = get_setting("smtp_password", ""); to = get_setting("notify_email_to", "") or user
    if not (host and user and to):
        log.warning("Email enabled but SMTP host/user/recipient incomplete")
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = user
        msg["To"] = to
        msg.set_content(body)
        with smtplib.SMTP(host, get_setting_int("smtp_port", 587), timeout=15) as s:
            s.starttls()
            if pwd:
                s.login(user, pwd)
            s.send_message(msg)
    except Exception as e:
        log.warning("Email send failed: %s", e)

def _send_whatsapp(text):
    if not get_setting_bool("notify_whatsapp_enabled"):
        return
    phone = get_setting("callmebot_phone", ""); key = get_setting("callmebot_apikey", "")
    if not (phone and key):
        log.warning("WhatsApp enabled but phone/apikey incomplete")
        return
    try:
        url = "https://api.callmebot.com/whatsapp.php?" + urllib.parse.urlencode(
            {"phone": phone, "text": text, "apikey": key})
        urllib.request.urlopen(url, timeout=15).read()
    except Exception as e:
        log.warning("WhatsApp send failed: %s", e)

def notify_alert(symbol, rule_type, message, price):
    label = {"volatility":"Unusual Move","support_resistance":"S/R Breach",
             "consecutive_down":"Consecutive Down","volume":"Volume Spike",
             "gap":"Gap Open","rsi":"RSI Extreme","ma_cross":"MA Crossover"}.get(rule_type, rule_type)
    subject = f"⚡ Tripwire: {symbol} {label}"
    body = f"{symbol} — {label}\n{message}\nPrice: ${price}\n\n(Tripwire alert)"
    _send_email(subject, body)
    _send_whatsapp(f"⚡ {symbol} {label}: {message} (${price})")

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
            triggered = streak_active or streak_resolved
            if streak_active:
                rationale = f"The stock has closed lower for {cur_run} consecutive days — selling pressure is ongoing. Watch for either a bounce from oversold conditions or continuation of the downtrend. Both outcomes are actionable."
                description = f"Active streak: {cur_run} consecutive down days (threshold {threshold})."
            elif streak_resolved:
                rationale = f"The stock had a streak of {max_downs} consecutive down days but has since reversed upward. The rule fired on the streak; today's move up may be the technical bounce that streak often precedes. Monitor whether the recovery holds."
                description = f"Streak of {max_downs} consecutive down days detected recently; stock has since bounced."
            else:
                rationale = "Sustained multi-day selling pressure signals trend momentum and potential exhaustion. After a run of consecutive down days, the stock approaches a decision point: either a technical bounce from oversold conditions, or continuation of the downtrend."
                description = f"Fires after {threshold}+ consecutive closing days in the red."
            signal3 = ("SELL" if streak_active else "BUY" if streak_resolved else "NEUTRAL") if triggered else "NEUTRAL"
            r3.update({
                "description": description,
                "rationale": rationale,
                "param_summary": f"Min consecutive days: {threshold}",
                "actual_value": cur_run if streak_active else max_downs,
                "current_run": cur_run, "max_run": max_downs,
                "streak_active": streak_active, "streak_resolved": streak_resolved,
                "threshold": threshold, "triggered": triggered, "signal": signal3,
                "message": (
                    f"Active streak: {cur_run} consecutive down days — ALERT" if streak_active else
                    f"Streak of {max_downs} down days detected; stock has since bounced — ALERT" if streak_resolved else
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
    for s in stocks:
        sym, cat = s["symbol"], s["category"]
        try:
            quote   = fetch_quote(sym)
            store_price(sym, quote)
            history = get_history(sym, days=400)
            params  = get_params(sym, cat)
            rules   = evaluate_rules(sym, quote, history, params)
            move_pct = ((quote["close"] - quote["prev_close"]) / quote["prev_close"] * 100) if quote.get("prev_close") else 0
            for rule in rules:
                if rule.get("triggered") and not rule.get("disabled"):
                    synthesis = None
                    if not is_in_cooldown(sym, rule["rule_type"]):
                        news_items = fetch_news(sym)
                        direction  = rule.get("direction") or ("UP" if move_pct >= 0 else "DOWN")
                        synthesis  = synthesize_news(sym, news_items, move_pct, direction, rule.get("label", rule["rule_type"]))
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
                    })
                    log_alert(sym, rule["rule_type"], rule["message"], quote["close"], detail)
            hist30 = get_history(sym, days=30)
            result = {
                "quote": quote, "rules": rules, "params": params, "error": None,
                "week52_high":   quote.get("week52_high"),
                "week52_low":    quote.get("week52_low"),
                "history_closes": [h["close"] for h in hist30],
            }
        except Exception as e:
            log.warning("run_check(%s) failed: %s", sym, e)
            result = {"error": str(e), "rules": [], "params": {}, "history_closes": []}
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

@app.route("/login", methods=["GET", "POST"])
def login():
    error_html = ""
    if request.method == "POST":
        if request.form.get("password") == AUTH_PASSWORD:
            session["authed"] = True
            return redirect(url_for("dashboard"))
        error_html = '<div class="err">Incorrect password</div>'
    return Response(LOGIN_PAGE.replace("__ERROR_HTML__", error_html), mimetype="text/html")

@app.route("/logout")
def logout():
    session.pop("authed", None)
    return redirect(url_for("login"))

def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get("authed"):
            if request.path.startswith("/api/"):
                return jsonify({"success": False, "error": "Not authenticated"}), 401
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
    })

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
        any_alert = any(r.get("triggered") and not r.get("disabled") for r in rules)
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
            })
        else:
            results.append({"symbol": sym, "category": cat, "price": None,
                            "error": cached.get("error", "Waiting for first check..."),
                            "rules": [], "history_closes": []})
    return jsonify(results)

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
    } for r in rows]})

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
    now = int(time.time())
    cutoff = now - 30 * 86400
    with get_db() as conn:
        by_symbol = conn.execute(
            "SELECT symbol, COUNT(*) c FROM alerts GROUP BY symbol ORDER BY c DESC").fetchall()
        by_rule = conn.execute(
            "SELECT rule_type, COUNT(*) c FROM alerts GROUP BY rule_type ORDER BY c DESC").fetchall()
        recent = conn.execute(
            "SELECT timestamp, detail FROM alerts WHERE timestamp>?", (cutoff,)).fetchall()
        total = conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"]
    # Daily counts (last 30 days) + BUY/SELL split from detail JSON
    daily = {}
    buys = sells = 0
    for r in recent:
        day = datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d")
        daily[day] = daily.get(day, 0) + 1
        try:
            sig = (json.loads(r["detail"]) or {}).get("signal") if r["detail"] else None
            if sig == "BUY": buys += 1
            elif sig == "SELL": sells += 1
        except Exception:
            pass
    daily_series = []
    for i in range(29, -1, -1):
        day = datetime.fromtimestamp(now - i*86400).strftime("%Y-%m-%d")
        daily_series.append({"date": day, "count": daily.get(day, 0)})
    return jsonify({
        "total": total,
        "by_symbol": [{"symbol": r["symbol"], "count": r["c"]} for r in by_symbol],
        "by_rule": [{"rule_type": r["rule_type"], "count": r["c"]} for r in by_rule],
        "signal_split": {"buy": buys, "sell": sells},
        "daily": daily_series,
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
    try:
        quote = fetch_quote(symbol)
        store_price(symbol, quote)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not fetch {symbol}: {e}"})
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO stocks (symbol,category,active,added_at) VALUES (?,?,1,?)",
            (symbol, cat, int(time.time()))
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

@app.route("/api/stock/<symbol>/params/reset", methods=["POST"])
@login_required
def api_reset_params(symbol):
    sym = symbol.upper()
    reset_params(sym)
    worker_pool.submit(run_check, [sym])
    return jsonify({"success": True})

# ── Settings ──────────────────────────────────────────────────────────────────

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
    return jsonify(out)

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
    worker_pool.submit(_send_email, "⚡ Tripwire test", "This is a Tripwire test notification.")
    worker_pool.submit(_send_whatsapp, "⚡ Tripwire test notification")
    return jsonify({"success": True,
                    "email": get_setting_bool("notify_email_enabled"),
                    "whatsapp": get_setting_bool("notify_whatsapp_enabled")})

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
    {"name": "update_settings", "description": "Update app settings. Keys: check_interval_seconds, check_interval_closed_seconds, market_hours_only (0/1), alert_cooldown_hours, notify_email_enabled (0/1), notify_whatsapp_enabled (0/1), browser_notifications_enabled (0/1), alert_sound_enabled (0/1), ai_synthesis_model, ai_assistant_model. Do not set secrets here.",
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
                out.append({
                    "symbol": sym, "category": s["category"],
                    "price": q.get("close"),
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
                conn.execute("INSERT OR REPLACE INTO stocks (symbol,category,active,added_at) VALUES (?,?,1,?)",
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
        trig = [r["rule_type"] for r in cached.get("rules", []) if r.get("triggered") and not r.get("disabled")]
        ed = ext.get("earnings_date"); din = _days_until(ed)
        earn = f", earnings {ed} ({din}d)" if ed else ""
        lines.append(f"  {sym} [{s['category']}] ${q.get('close','?')}"
                     + (f" — triggered: {', '.join(trig)}" if trig else "")
                     + earn)
    watch = "\n".join(lines) if lines else "  (empty)"
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
        "- Be concise and specific. Use plain language a retail investor understands.\n"
        "- You are informational only — never give personalized financial advice or guarantees.\n\n"
        f"Current watchlist:\n{watch}"
    )

def _sse(obj):
    return f"data: {json.dumps(obj)}\n\n"

@app.route("/api/ai/history")
@login_required
def api_ai_history():
    return jsonify({"messages": _chat_history(60)})

@app.route("/api/ai/clear", methods=["POST"])
@login_required
def api_ai_clear():
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

    history = _chat_history(30)
    _chat_save("user", user_msg)

    api_key = get_anthropic_key()
    def generate():
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        model = get_setting("ai_assistant_model", "claude-opus-4-8")
        system = _ai_system_prompt()
        tools = AI_TOOLS + [{"type": "web_search_20260209", "name": "web_search"}]
        messages = history + [{"role": "user", "content": user_msg}]
        assistant_text_parts = []
        wrote = False
        try:
            for _ in range(12):  # cap tool-use rounds
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
                            result = execute_ai_tool(block.name, block.input)
                            if block.name in WRITE_TOOLS and not result.get("error"):
                                wrote = True
                            tool_results.append({"type": "tool_result", "tool_use_id": block.id,
                                                 "content": json.dumps(result)})
                    messages.append({"role": "user", "content": tool_results})
                    continue
                break
            full = "".join(assistant_text_parts).strip()
            if full:
                _chat_save("assistant", full)
            yield _sse({"type": "done", "refresh": wrote})
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
#topbar{background:#12151F;border-bottom:1px solid #1E2235;padding:0 20px;height:52px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
#logo{font-weight:800;font-size:17px;color:#F59E0B;letter-spacing:-0.5px}
#topbar-right{display:flex;align-items:center;gap:12px;font-size:13px;color:#6B7280}
#status-dot{width:8px;height:8px;border-radius:50%;background:#10B981;display:inline-block;margin-right:4px;transition:background .3s}
#status-dot.checking{background:#F59E0B}
#btn-check{background:#F59E0B;color:#000;border-radius:6px;padding:7px 16px;font-weight:700;font-size:13px}
#btn-check:disabled{opacity:0.5;cursor:not-allowed}

/* Tabs */
#tabs{display:flex;border-bottom:1px solid #1E2235;background:#12151F;padding:0 20px}
.tab{padding:12px 18px;font-size:14px;color:#6B7280;border-bottom:2px solid transparent;cursor:pointer;background:none;border-left:none;border-right:none;border-top:none}
.tab.active{color:#F59E0B;border-bottom-color:#F59E0B;font-weight:700}

/* Content */
#content{padding:20px;max-width:1440px;margin:0 auto}

/* Add ticker */
#add-bar{background:#12151F;border:1px solid #1E2235;border-radius:10px;padding:14px 16px;margin-bottom:16px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}
#add-bar input{background:#0A0C12;border:1px solid #1E2235;color:#E4E0D8;border-radius:6px;padding:8px 12px;font-size:13px;width:120px}
#add-bar select{background:#0A0C12;border:1px solid #1E2235;color:#E4E0D8;border-radius:6px;padding:8px 12px;font-size:13px}
#btn-add{background:#10B981;color:#fff;border-radius:6px;padding:8px 16px;font-weight:700;font-size:13px}
#add-status{font-size:12px;color:#6B7280}
#add-status.err{color:#EF4444}
#add-status.ok{color:#10B981}

/* Stock grid */
#stock-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:10px}
.stock-card{background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:14px;cursor:pointer;transition:border-color .15s,box-shadow .15s;position:relative}
.stock-card:hover{border-color:#374151;box-shadow:0 4px 20px #00000044}
.stock-card.selected{border-color:#F59E0B;box-shadow:0 0 0 1px #F59E0B44}
.stock-card.alert{border-color:#F59E0B55}
.alert-dot{position:absolute;top:10px;right:10px;width:7px;height:7px;border-radius:50%;background:#F59E0B;box-shadow:0 0 6px #F59E0B}
.stock-symbol{font-weight:800;font-size:15px;margin-bottom:2px;letter-spacing:-.3px}
.stock-cat{font-size:10px;color:#6B7280;margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px}
.stock-price{font-size:22px;font-weight:800;margin-bottom:2px;letter-spacing:-1px}
.stock-pct{font-size:13px;font-weight:600;margin-bottom:6px}
.stock-ext{font-size:10px;margin-top:4px;display:flex;gap:8px}
.stock-time{font-size:10px;color:#4B5563;margin-top:4px}
.stock-err{font-size:11px;color:#EF4444;margin-top:8px}
.up{color:#10B981}.dn{color:#EF4444}.muted{color:#6B7280}
.pre-clr{color:#A78BFA}.post-clr{color:#60A5FA}
.remove-btn{position:absolute;top:8px;left:8px;background:#1A1D27;border:1px solid #2A2D3E;color:#6B7280;border-radius:4px;font-size:10px;padding:1px 5px;display:none;z-index:2}
.stock-card:hover .remove-btn{display:block}

/* Alert summary on card */
.card-triggered{margin-top:10px;border-top:1px solid #1E2235;padding-top:8px}
.card-triggered-hdr{font-size:10px;color:#F59E0B;font-weight:700;letter-spacing:.5px;margin-bottom:5px;text-transform:uppercase}
.card-triggered-item{font-size:11px;color:#D1D5DB;padding:3px 0;display:flex;align-items:flex-start;gap:5px;line-height:1.4}
.card-triggered-item .ti-dot{color:#F59E0B;flex-shrink:0;margin-top:1px}
.card-triggered-item .ti-label{color:#F59E0B;font-weight:700;flex-shrink:0}

/* Detail panel */
#detail{background:#12151F;border:1px solid #1E2235;border-radius:14px;padding:22px;margin-bottom:16px}
#detail-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:18px}
#detail-title{font-size:24px;font-weight:800;letter-spacing:-1px}
#detail-meta{font-size:12px;color:#6B7280;margin-top:3px}
#btn-close{background:#1E2235;color:#E4E0D8;border-radius:6px;padding:7px 14px;font-size:13px}

/* Price grid */
#price-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:8px;margin-bottom:10px}
.price-cell{background:#0A0C12;border:1px solid #1E2235;border-radius:8px;padding:10px 12px}
.price-cell-label{font-size:9px;color:#6B7280;margin-bottom:4px;letter-spacing:.7px;text-transform:uppercase}
.price-cell-val{font-size:16px;font-weight:800;letter-spacing:-.5px}
.price-cell-sub{font-size:11px;color:#6B7280;margin-top:2px}

/* Range grid */
.ranges-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:18px}
.range-cell{background:#0A0C12;border:1px solid #1E2235;border-radius:8px;padding:10px 12px}
.range-cell-label{font-size:9px;color:#6B7280;letter-spacing:.7px;text-transform:uppercase;margin-bottom:6px}
.range-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:3px}
.range-row span:first-child{font-size:10px;color:#6B7280}
.range-row span:last-child{font-size:13px;font-weight:700}
.range-bar-wrap{height:4px;background:#1E2235;border-radius:2px;margin-top:6px;position:relative}
.range-bar-fill{height:100%;border-radius:2px}
.range-bar-dot{position:absolute;top:-3px;width:10px;height:10px;border-radius:50%;border:2px solid #0A0C12;transform:translateX(-50%)}

/* Rules */
.section-label{font-size:10px;color:#6B7280;letter-spacing:1px;margin-bottom:10px;font-weight:700;text-transform:uppercase}
.rule-card{background:#0A0C12;border:1px solid #1E2235;border-radius:10px;padding:16px;margin-bottom:10px;transition:border-color .15s}
.rule-card.alert-rule{border-color:#F59E0B55;background:#0D0F14}
.rule-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.rule-title{font-weight:700;font-size:14px}
.rule-header-right{display:flex;gap:8px;align-items:center}
.badge{border-radius:4px;padding:2px 9px;font-size:10px;font-weight:700;letter-spacing:.5px}
.badge-ok{background:#10B98115;color:#10B981;border:1px solid #10B98133}
.badge-alert{background:#F59E0B15;color:#F59E0B;border:1px solid #F59E0B33}
.badge-disabled{background:#6B728015;color:#6B7280;border:1px solid #6B728033}
.rule-desc{font-size:12px;color:#9CA3AF;margin-bottom:6px;line-height:1.5}
.rule-rationale{font-size:11px;color:#6B7280;background:#12151F;border-left:2px solid #374151;padding:7px 10px;border-radius:0 6px 6px 0;margin-bottom:10px;line-height:1.5;font-style:italic}
.rule-params-line{font-size:11px;color:#3B82F6;margin-bottom:10px}
.rule-vals{display:grid;grid-template-columns:repeat(auto-fill,minmax(100px,1fr));gap:6px;margin-bottom:10px}
.val-cell{background:#12151F;border:1px solid #1E2235;border-radius:6px;padding:7px 10px}
.val-cell-label{font-size:9px;color:#6B7280;margin-bottom:3px;letter-spacing:.5px;text-transform:uppercase}
.val-cell-val{font-size:14px;font-weight:700}
.rule-msg{font-size:12px;background:#12151F;border-radius:6px;padding:8px 11px;color:#9CA3AF;margin-top:6px}
.rule-msg.alert-msg{color:#F59E0B;background:#F59E0B0A;border:1px solid #F59E0B22}
.edit-toggle{background:#1E2235;color:#E4E0D8;border-radius:4px;padding:3px 10px;font-size:11px}
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
.alert-group-cat{font-size:11px;color:#6B7280}
.alert-group-chevron{margin-left:auto;color:#6B7280;font-size:12px;transition:transform .2s}
.alert-group-chevron.open{transform:rotate(180deg)}
.alert-entries{display:none;border-top:1px solid #1E2235}
.alert-entries.open{display:block}
.alert-entry{padding:14px 18px;border-bottom:1px solid #0F1117}
.alert-entry:last-child{border-bottom:none}
.alert-entry-header{display:flex;align-items:center;gap:10px;margin-bottom:6px}
.alert-entry-time{font-size:11px;color:#4B5563;font-family:monospace}
.alert-entry-rule{font-size:11px;color:#F59E0B;background:#F59E0B0F;border-radius:4px;padding:1px 8px;font-weight:700;text-transform:uppercase;letter-spacing:.4px}
.alert-entry-price{font-size:11px;color:#9CA3AF;margin-left:auto}
.alert-entry-msg{font-size:13px;color:#E4E0D8;margin-bottom:6px;font-weight:500}
.alert-entry-detail{font-size:11px;color:#6B7280;line-height:1.6}
.alert-detail-vals{display:flex;gap:12px;flex-wrap:wrap;margin-top:6px}
.alert-detail-val{background:#0A0C12;border-radius:4px;padding:3px 8px;font-size:11px}
.alert-detail-val span:first-child{color:#6B7280}
.alert-detail-val span:last-child{color:#E4E0D8;font-weight:600;margin-left:4px}
.alert-rationale{font-size:11px;color:#6B7280;background:#0A0C12;border-left:2px solid #374151;padding:6px 10px;border-radius:0 4px 4px 0;margin-top:6px;line-height:1.5;font-style:italic}
.news-synthesis{background:#130F1F;border-left:2px solid #7C3AED;padding:8px 12px;border-radius:0 6px 6px 0;margin-top:8px;line-height:1.6}
.news-synthesis-label{font-size:10px;font-weight:700;color:#7C3AED;letter-spacing:0.05em;text-transform:uppercase;margin-bottom:4px}
.news-synthesis-text{font-size:12px;color:#C4B5FD;font-style:italic}
.rule-news-synthesis{background:#130F1F;border-left:2px solid #7C3AED;padding:8px 12px;border-radius:0 6px 6px 0;margin-bottom:10px;line-height:1.6}
.rule-news-label{font-size:10px;font-weight:700;color:#7C3AED;letter-spacing:0.05em;text-transform:uppercase;margin-bottom:4px}
.rule-news-text{font-size:12px;color:#C4B5FD;font-style:italic}

.err-banner{background:#EF444415;border:1px solid #EF4444;color:#EF4444;padding:10px 16px;font-size:13px;margin-bottom:12px;border-radius:8px}
.no-alerts{color:#6B7280;font-size:14px;padding:30px;text-align:center}

/* Signal badges */
.sig-badge{display:inline-block;border-radius:6px;padding:3px 12px;font-size:11px;font-weight:800;letter-spacing:.6px;text-transform:uppercase}
.sig-strong-buy{background:#10B98125;color:#10B981;border:2px solid #10B98166}
.sig-strong-sell{background:#EF444425;color:#EF4444;border:2px solid #EF444466}
.sig-trending-buy{background:#10B98112;color:#10B981;border:1px solid #10B98144}
.sig-trending-sell{background:#EF444412;color:#EF4444;border:1px solid #EF444444}
.sig-pending{background:#6B728012;color:#6B7280;border:1px solid #6B728044}
.sig-buy{background:#10B98112;color:#10B981;border:1px solid #10B98133}
.sig-sell{background:#EF444412;color:#EF4444;border:1px solid #EF444433}
.sig-neutral{background:#37415112;color:#6B7280;border:1px solid #37415144}
.card-signal{margin:8px 0 4px 0}
.group-signal{margin-left:auto;margin-right:8px}

/* Market phase pill */
#market-phase{font-size:11px;font-weight:700;border-radius:10px;padding:2px 9px;letter-spacing:.3px}
.mp-regular{background:#10B98118;color:#10B981;border:1px solid #10B98140}
.mp-pre,.mp-post{background:#F59E0B15;color:#F59E0B;border:1px solid #F59E0B40}
.mp-closed{background:#6B728015;color:#9CA3AF;border:1px solid #6B728040}

/* Earnings chip on cards */
.earnings-chip{display:inline-block;font-size:10px;font-weight:700;border-radius:4px;padding:1px 7px;margin-top:5px;background:#3B82F615;color:#60A5FA;border:1px solid #3B82F633}
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
.set-row input[type=text],.set-row input[type=number],.set-row input[type=password],.set-row select{background:#0A0C12;border:1px solid #1E2235;color:#E4E0D8;border-radius:6px;padding:7px 10px;font-size:13px;width:200px;max-width:55%}
.set-hint{font-size:11px;color:#6B7280;margin:-6px 0 12px 0;line-height:1.5}
.settings-actions{display:flex;gap:10px;align-items:center;margin-top:8px}
#btn-save-settings{background:#F59E0B;color:#000;border-radius:7px;padding:9px 20px;font-weight:700;font-size:13px}
#btn-test-notify{background:#1E2235;color:#E4E0D8;border-radius:7px;padding:9px 16px;font-size:13px}
#settings-status{font-size:12px;color:#10B981}

/* Analytics tab */
#pane-analytics{padding-bottom:60px}
.analytics-card{background:#12151F;border:1px solid #1E2235;border-radius:12px;padding:18px 20px;margin-bottom:16px}
.analytics-card h3{font-size:13px;color:#F59E0B;margin-bottom:6px;letter-spacing:.5px;text-transform:uppercase}
.analytics-summary{display:flex;gap:20px;flex-wrap:wrap;margin-bottom:16px}
.an-stat{background:#12151F;border:1px solid #1E2235;border-radius:10px;padding:12px 18px}
.an-stat-num{font-size:22px;font-weight:800}
.an-stat-lbl{font-size:11px;color:#6B7280;text-transform:uppercase;letter-spacing:.5px}
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
  <button class="tab" onclick="switchTab('analytics',this)">Analytics</button>
  <button class="tab" onclick="switchTab('settings',this)">Settings</button>
</div>

<div id="content">
  <div id="err-banner" class="err-banner" style="display:none"></div>

  <div id="pane-stocks">
    <div id="add-bar">
      <strong style="font-size:13px">Add ticker:</strong>
      <input id="inp-symbol" placeholder="e.g. TSLA" maxlength="10"
             onkeydown="if(event.key==='Enter')addStock()">
      <select id="inp-cat">
        <option value="high_vol">High Vol</option>
        <option value="mod_vol">Moderate Vol</option>
        <option value="low_vol">Low Vol</option>
      </select>
      <button id="btn-add" onclick="addStock()">+ Add</button>
      <span id="add-status"></span>
    </div>
    <div id="detail" style="display:none"></div>
    <div id="stock-grid"></div>
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
  </div>

  <div id="pane-analytics" style="display:none"></div>

  <div id="pane-settings" style="display:none"></div>
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
async function postJSON(url,body){ const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); if(r.status===401){location.href='/login';return{};} return r.json(); }

let aiEnabled=false, maxAlertId=0, notifPrimed=false, appSettings={};

async function loadAll(){
  try{
    const [s,a,st]=await Promise.all([fetchJSON('/api/stocks'),fetchJSON('/api/alerts'),fetchJSON('/api/status')]);
    stocks=s; alerts=a.alerts||[];
    renderStatus(st);
    renderGrid();
    renderAlerts();
    checkNewAlertNotifications();
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
function renderStatus(st){
  checking=st.status==='checking';
  aiEnabled=!!st.ai_enabled;
  document.getElementById('status-dot').className=checking?'checking':'';
  document.getElementById('status-txt').textContent=checking?'Checking...':'Running';
  document.getElementById('btn-check').disabled=checking;
  const mp=document.getElementById('market-phase');
  if(mp&&st.market_phase){mp.textContent=PHASE_LABEL[st.market_phase]||st.market_phase;mp.className='mp-'+st.market_phase;}
  if(st.last_check){
    const d=new Date(st.last_check);
    document.getElementById('last-check-txt').textContent='Last: '+d.toLocaleTimeString();
  }
}

async function manualCheck(){
  document.getElementById('btn-check').disabled=true;
  await postJSON('/api/check',{});
  setTimeout(loadAll,1500);
}

// ── Browser notifications ─────────────────────────────────────────────────────
function checkNewAlertNotifications(){
  const ids=alerts.map(a=>a.id);
  const newMax=ids.length?Math.max(...ids):0;
  if(!notifPrimed){ maxAlertId=newMax; notifPrimed=true; return; }  // don't fire on first load
  if(newMax>maxAlertId){
    const fresh=alerts.filter(a=>a.id>maxAlertId);
    if(appSettings.browser_notifications_enabled!=='0' && 'Notification' in window && Notification.permission==='granted'){
      fresh.slice(0,3).forEach(a=>{ try{ new Notification('⚡ '+a.symbol+' alert',{body:a.message}); }catch(e){} });
    }
    if(appSettings.alert_sound_enabled==='1') beep();
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
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  btn.classList.add('active');
  ['stocks','alerts','assistant','analytics','settings'].forEach(n=>{
    const el=document.getElementById('pane-'+n);
    if(el) el.style.display=(n===name)?'':'none';
  });
  if(name==='settings') loadSettings();
  if(name==='analytics') loadAnalytics();
  if(name==='assistant') loadChatHistory();
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
function computeSignal(rules){
  const triggered=(rules||[]).filter(r=>r.triggered&&!r.disabled);
  if(!triggered.length) return null;
  const buys=triggered.filter(r=>r.signal==='BUY').length;
  const sells=triggered.filter(r=>r.signal==='SELL').length;
  const total=triggered.length;
  if(total>=2&&buys/total>=2/3) return{label:'STRONG BUY',cls:'sig-strong-buy'};
  if(total>=2&&sells/total>=2/3) return{label:'STRONG SELL',cls:'sig-strong-sell'};
  if(buys>sells) return{label:'TRENDING BUY',cls:'sig-trending-buy'};
  if(sells>buys) return{label:'TRENDING SELL',cls:'sig-trending-sell'};
  return{label:'PENDING',cls:'sig-pending'};
}

function signalBadge(sig){
  if(!sig) return '';
  return `<span class="sig-badge ${sig.cls}">${sig.label}</span>`;
}

function ruleSigBadge(signal){
  if(!signal||signal==='NEUTRAL') return '<span class="sig-badge sig-neutral">NEUTRAL</span>';
  if(signal==='BUY') return '<span class="sig-badge sig-buy">▲ BUY</span>';
  if(signal==='SELL') return '<span class="sig-badge sig-sell">▼ SELL</span>';
  return '';
}

// ── Stock grid ────────────────────────────────────────────────────────────────
function renderGrid(){
  const grid=document.getElementById('stock-grid');
  const alertCount=stocks.filter(s=>s.alert).length;
  document.getElementById('tab-alerts-btn').textContent='Alerts'+(alertCount>0?' ('+alertCount+')':'');

  grid.innerHTML=stocks.map(s=>{
    const pctCls=s.pct>0?'up':s.pct<0?'dn':'muted';
    const sign=s.pct>0?'+':'';
    const selCls=selectedSym===s.symbol?' selected':'';
    const alertCls=s.alert?' alert':'';
    const triggered=(s.rules||[]).filter(r=>r.triggered&&!r.disabled);

    const sig=computeSignal(s.rules);
    const allRules=(s.rules||[]).filter(r=>!r.disabled);
    const hasRuleData=allRules.length>0&&allRules.some(r=>r.message&&!r.message.includes('Insufficient')&&!r.message.includes('Waiting'));

    let rulesHTML='';
    if(hasRuleData){
      if(triggered.length>0){
        rulesHTML=`
          <div class="card-triggered">
            <div class="card-triggered-hdr">⚠ ${triggered.length} rule${triggered.length>1?'s':''} triggered</div>
            ${triggered.map(r=>`<div class="card-triggered-item">
              <span class="ti-dot">▸</span>
              <span><span class="ti-label">${shortRuleLabel(r.rule_type)}:</span> ${cardRuleDetail(r)} ${ruleSigBadge(r.signal)}</span>
            </div>`).join('')}
          </div>
          <div class="card-signal">${signalBadge(sig)}</div>`;
      } else {
        // All rules evaluated and all OK — show compact per-rule status
        rulesHTML=`
          <div class="card-triggered">
            <div class="card-triggered-hdr" style="color:#10B981">✓ All rules OK</div>
            ${allRules.map(r=>`<div class="card-triggered-item">
              <span class="ti-dot" style="color:#10B981">✓</span>
              <span><span class="ti-label" style="color:#9CA3AF">${shortRuleLabel(r.rule_type)}:</span> ${cardRuleDetail(r)}</span>
            </div>`).join('')}
          </div>`;
      }
    }

    return `<div class="stock-card${selCls}${alertCls}" onclick="selectStock('${s.symbol}')">
      <button class="remove-btn" onclick="removeStock('${s.symbol}',event)">✕</button>
      ${s.alert?'<div class="alert-dot"></div>':''}
      <div class="stock-symbol">${s.symbol}</div>
      <div class="stock-cat">${(s.category||'').replace('_vol',' vol')}</div>
      ${s.price!=null?`
        <div class="stock-price">$${s.price}</div>
        <div class="stock-pct ${pctCls}">${sign}${s.pct}%</div>
        <div class="stock-ext">
          ${s.pre_market?`<span class="pre-clr">Pre $${s.pre_market}</span>`:''}
          ${s.post_market?`<span class="post-clr">Post $${s.post_market}</span>`:''}
        </div>
        <div class="stock-time">${s.date} ${s.time}</div>
        ${earningsChip(s)}
        ${rulesHTML}
      `:`<div class="stock-err">${s.error||'Loading...'}</div>`}
    </div>`;
  }).join('');

  if(selectedSym){const s=stocks.find(x=>x.symbol===selectedSym);if(s)renderDetail(s);}
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
  selectedSym=sym;
  const s=stocks.find(x=>x.symbol===sym);
  if(s){renderDetail(s);renderGrid();}
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
        <div id="detail-meta">Updated ${s.date} ${s.time}${s.alert?' · <span style="color:#F59E0B">⚠ Alert active</span>':''}</div>
        ${signalBadge(computeSignal(s.rules))}
      </div>
      <button id="btn-close" onclick="selectStock('${s.symbol}')">✕ Close</button>
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
    <div class="section-label">Rules</div>
    <div id="rules-container">
      ${(s.rules||[]).map((r,i)=>ruleHTML(s.symbol,r,i,s.history_closes,s.params)).join('')}
      ${(!s.rules||s.rules.length===0)?'<div style="color:#6B7280;font-size:13px">No rule data yet — click Check Now</div>':''}
    </div>`;
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
    <div class="rule-desc">${r.description||''}</div>
    ${r.rationale?`<div class="rule-rationale">${r.rationale}</div>`:''}
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

function renderAlerts(){
  const el=document.getElementById('alert-pane');
  if(!alerts||alerts.length===0){
    el.innerHTML='<div class="no-alerts">No alerts yet — run a check to evaluate your watchlist.</div>';
    return;
  }

  // Group by symbol, preserve insertion order (already sorted by time DESC)
  const groups={};
  const order=[];
  alerts.forEach(a=>{
    if(!groups[a.symbol]){groups[a.symbol]=[];order.push(a.symbol);}
    groups[a.symbol].push(a);
  });

  el.innerHTML=order.map(sym=>{
    const entries=groups[sym];
    const isOpen=alertGroupOpen[sym]!==false;  // default open
    const html=entries.map(a=>{
      let detail={};
      try{ if(a.detail) detail=JSON.parse(a.detail); }catch(e){}

      const detailVals=[];
      if(detail.actual_value!=null){
        const unit=a.rule_type==='volatility'?'%':a.rule_type==='consecutive_down'?' days':'';
        detailVals.push(`<div class="alert-detail-val"><span>Actual</span><span>${detail.actual_value}${unit}</span></div>`);
      }
      if(detail.threshold!=null){
        const unit=a.rule_type==='volatility'?'%':a.rule_type==='consecutive_down'?' days':'';
        detailVals.push(`<div class="alert-detail-val"><span>Threshold</span><span>${detail.threshold}${unit}</span></div>`);
      }
      if(detail.avg_daily_vol!=null) detailVals.push(`<div class="alert-detail-val"><span>Avg Daily Vol</span><span>${detail.avg_daily_vol}%</span></div>`);
      if(detail.support!=null)       detailVals.push(`<div class="alert-detail-val"><span>Support</span><span>$${detail.support}</span></div>`);
      if(detail.resistance!=null)    detailVals.push(`<div class="alert-detail-val"><span>Resistance</span><span>$${detail.resistance}</span></div>`);
      if(detail.direction)           detailVals.push(`<div class="alert-detail-val"><span>Direction</span><span>${detail.direction}</span></div>`);

      const ruleLabel={volatility:'Unusual Move',support_resistance:'S/R Breach',consecutive_down:'Consec. Down',volume:'Volume Spike',gap:'Opening Gap',rsi:'RSI Extreme',ma_cross:'MA Crossover'}[a.rule_type]||a.rule_type.replace(/_/g,' ');
      const alertSig=detail.signal?ruleSigBadge(detail.signal):'';

      return `<div class="alert-entry" style="${a.ack?'opacity:.5':''}">
        <div class="alert-entry-header">
          <span class="alert-entry-time">${a.time}</span>
          <span class="alert-entry-rule">${ruleLabel}</span>
          ${alertSig}
          ${a.ack?'<span style="font-size:10px;color:#10B981">✓ ack</span>':''}
          <span class="alert-entry-price">Price: $${a.price}</span>
        </div>
        <div class="alert-entry-msg">${a.message}</div>
        ${detail.description?`<div class="alert-entry-detail">${detail.description}</div>`:''}
        ${detailVals.length>0?`<div class="alert-detail-vals">${detailVals.join('')}</div>`:''}
        ${detail.rationale?`<div class="alert-rationale">${detail.rationale}</div>`:''}
        ${detail.news_synthesis?`<div class="news-synthesis"><div class="news-synthesis-label">📰 News Synthesis</div><div class="news-synthesis-text">${detail.news_synthesis}</div></div>`:''}
      </div>`;
    }).join('');

    // Compute group signal from all alerts in this group
    const groupSigs=entries.map(a=>{let d={};try{if(a.detail)d=JSON.parse(a.detail);}catch(e){}return d.signal;}).filter(Boolean);
    const gBuys=groupSigs.filter(s=>s==='BUY').length;
    const gSells=groupSigs.filter(s=>s==='SELL').length;
    const gTotal=groupSigs.length;
    let groupSig=null;
    if(gTotal>=2&&gBuys/gTotal>=2/3) groupSig={label:'STRONG BUY',cls:'sig-strong-buy'};
    else if(gTotal>=2&&gSells/gTotal>=2/3) groupSig={label:'STRONG SELL',cls:'sig-strong-sell'};
    else if(gBuys>gSells) groupSig={label:'TRENDING BUY',cls:'sig-trending-buy'};
    else if(gSells>gBuys) groupSig={label:'TRENDING SELL',cls:'sig-trending-sell'};
    else if(gTotal>0) groupSig={label:'PENDING',cls:'sig-pending'};

    const unacked=entries.filter(a=>!a.ack).length;
    return `<div class="alert-group">
      <div class="alert-group-hdr" onclick="toggleAlertGroup('${sym}')">
        <span class="alert-group-sym">${sym}</span>
        <span class="alert-group-cnt">${entries.length} alert${entries.length>1?'s':''}</span>
        <span class="alert-group-cat" id="ag-cat-${sym}"></span>
        ${groupSig?`<span class="sig-badge ${groupSig.cls} group-signal">${groupSig.label}</span>`:''}
        ${unacked>0?`<button class="btn-reset" style="padding:3px 10px;font-size:11px;margin-right:6px" onclick="ackSymbol(event,'${sym}')">✓ Ack</button>`:''}
        <span class="alert-group-chevron ${isOpen?'open':''}" id="ag-chev-${sym}">▼</span>
      </div>
      <div class="alert-entries ${isOpen?'open':''}" id="ag-entries-${sym}">
        ${html}
      </div>
    </div>`;
  }).join('');

  // Fill in category labels from stocks data
  order.forEach(sym=>{
    const s=stocks.find(x=>x.symbol===sym);
    const el=document.getElementById('ag-cat-'+sym);
    if(el&&s) el.textContent=(s.category||'').replace('_vol',' vol');
  });
}

function toggleAlertGroup(sym){
  alertGroupOpen[sym] = !(alertGroupOpen[sym]!==false);
  const entries=document.getElementById('ag-entries-'+sym);
  const chev=document.getElementById('ag-chev-'+sym);
  if(entries) entries.className='alert-entries'+(alertGroupOpen[sym]?' open':'');
  if(chev) chev.className='alert-group-chevron'+(alertGroupOpen[sym]?' open':'');
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
  ]},
  {group:'Notifications',rows:[
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
  document.getElementById('pane-settings').innerHTML=html+
    `<div class="settings-actions">
       <button id="btn-save-settings" onclick="saveSettings()">Save settings</button>
       <button id="btn-test-notify" onclick="testNotify()">Send test notification</button>
       <button class="btn-reset" onclick="enableBrowserNotifs()">Enable browser notifications</button>
       <span id="settings-status"></span>
     </div>`;
}

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
  st.textContent='Sending…';
  const r=await postJSON('/api/settings/test-notify',{});
  st.textContent=`Sent (email:${r.email?'on':'off'}, whatsapp:${r.whatsapp?'on':'off'}) — save first if you just edited creds`;
  setTimeout(()=>st.textContent='',5000);
}

function enableBrowserNotifs(){
  if(!('Notification' in window)){alert('This browser does not support notifications.');return;}
  Notification.requestPermission().then(p=>{
    const st=document.getElementById('settings-status');
    if(st) st.textContent=p==='granted'?'✓ Browser notifications enabled':'Permission '+p;
  });
}

// ── Analytics tab ─────────────────────────────────────────────────────────────
async function loadAnalytics(){
  const d=await fetchJSON('/api/analytics');
  const pane=document.getElementById('pane-analytics');
  if(!d.total){ pane.innerHTML='<div class="no-alerts">No alert history yet.</div>'; return; }
  const bars=(items,keyName,max)=>items.map(it=>{
    const w=max?Math.max(3,it.count/max*100):0;
    return `<div class="abar-row"><span class="abar-label">${it[keyName]}</span>
      <div class="abar-track"><div class="abar-fill" style="width:${w}%"></div></div>
      <span class="abar-val">${it.count}</span></div>`;
  }).join('');
  const symMax=Math.max(...d.by_symbol.map(x=>x.count),1);
  const ruleMax=Math.max(...d.by_rule.map(x=>x.count),1);
  const ruleName={volatility:'Unusual Move',support_resistance:'S/R Breach',consecutive_down:'Consec Down',volume:'Volume',gap:'Gap',rsi:'RSI',ma_cross:'MA Cross'};
  const byRule=d.by_rule.map(x=>({count:x.count,rule_type:ruleName[x.rule_type]||x.rule_type}));
  pane.innerHTML=`
    <div class="analytics-summary">
      <div class="an-stat"><div class="an-stat-num">${d.total}</div><div class="an-stat-lbl">Total alerts</div></div>
      <div class="an-stat"><div class="an-stat-num up">${d.signal_split.buy}</div><div class="an-stat-lbl">Buy signals (30d)</div></div>
      <div class="an-stat"><div class="an-stat-num dn">${d.signal_split.sell}</div><div class="an-stat-lbl">Sell signals (30d)</div></div>
    </div>
    <div class="analytics-card"><h3>Alerts per stock</h3>${bars(d.by_symbol,'symbol',symMax)||'<div class="set-hint">No data</div>'}</div>
    <div class="analytics-card"><h3>Alerts by rule type</h3>${bars(byRule,'rule_type',ruleMax)||'<div class="set-hint">No data</div>'}</div>
    <div class="analytics-card"><h3>Activity — last 30 days</h3>${sparkline(d.daily)}</div>`;
}

function sparkline(daily){
  if(!daily||!daily.length) return '';
  const W=700,H=90,pT=8,pB=20,pL=8,pR=8;
  const plotW=W-pL-pR, plotH=H-pT-pB;
  const max=Math.max(...daily.map(x=>x.count),1);
  const bw=plotW/daily.length-2;
  const bars=daily.map((x,i)=>{
    const bh=Math.max(0,(x.count/max)*plotH);
    const bx=pL+i*(plotW/daily.length);
    const by=pT+plotH-bh;
    return `<rect x="${bx.toFixed(1)}" y="${by.toFixed(1)}" width="${Math.max(2,bw).toFixed(1)}" height="${bh.toFixed(1)}" fill="#F59E0B" rx="1"><title>${x.date}: ${x.count}</title></rect>`;
  }).join('');
  const lbl=`<text x="${pL}" y="${H-4}" fill="#6B7280" font-size="9">${daily[0].date}</text>
    <text x="${W-pR}" y="${H-4}" fill="#6B7280" font-size="9" text-anchor="end">${daily[daily.length-1].date}</text>`;
  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:${H}px">${bars}${lbl}</svg>`;
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
function mdLite(s){
  return escapeHTML(s)
    .replace(/\*\*([^*]+)\*\*/g,'<strong>$1</strong>')
    .replace(/`([^`]+)`/g,'<code>$1</code>')
    .replace(/\n/g,'<br>');
}

async function loadChatHistory(){
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

function renderSuggestions(){
  const scroll=document.getElementById('chat-scroll');
  scroll.innerHTML='<div style="color:#6B7280;font-size:13px;margin-bottom:14px">Ask me anything about your watchlist, or tell me to change a rule or setting.</div>'+
    '<div class="chat-suggestions">'+CHAT_SUGGESTIONS.map(s=>`<div class="chat-chip" onclick="useSuggestion(this)">${s}</div>`).join('')+'</div>';
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
    if(!resp.ok){ const e=await resp.json().catch(()=>({error:'request failed'})); addChatBubble('assistant','⚠ '+(e.error||'error')); throw new Error(e.error); }
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
        else if(ev.type==='done'){ if(ev.refresh){ chatHistoryLoaded=true; loadAll(); } }
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
    import webbrowser
    print("\n" + "="*60)
    print("  Tripwire v4 starting...")
    print("  Dashboard: http://localhost:5000")
    print("  Press Ctrl+C to stop")
    print("="*60 + "\n")
    threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5000")).start()
    app.run(port=5000, use_reloader=False)
