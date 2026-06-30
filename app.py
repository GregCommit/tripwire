"""
Tripwire v3 - Single file, self-contained
==========================================
Run:  python app.py
Open: http://localhost:5000
"""

import sqlite3, threading, time, json, os
from datetime import datetime
from pathlib import Path
import statistics
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import yfinance as yf

app = Flask(__name__)
CORS(app)

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
        """)
        try:
            conn.execute("ALTER TABLE alerts ADD COLUMN detail TEXT")
            conn.commit()
        except: pass
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
# RULE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_RULES = {
    "high_vol": {"volatility_multiplier":2.5,"volatility_lookback":20,"support_resist_pct":7.0,"support_resist_lookback":90,"consecutive_down_days":3,"use_consecutive":True},
    "mod_vol":  {"volatility_multiplier":1.5,"volatility_lookback":20,"support_resist_pct":5.0,"support_resist_lookback":30,"consecutive_down_days":3,"use_consecutive":False},
    "low_vol":  {"volatility_multiplier":1.2,"volatility_lookback":20,"support_resist_pct":4.0,"support_resist_lookback":60,"consecutive_down_days":3,"use_consecutive":False},
}

def get_stocks():
    with get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM stocks WHERE active=1 ORDER BY symbol")]

def get_params(symbol, category):
    with get_db() as conn:
        row = conn.execute("SELECT params FROM rule_params WHERE symbol=?", (symbol,)).fetchone()
        if row:
            return json.loads(row["params"])
    return DEFAULT_RULES.get(category, DEFAULT_RULES["high_vol"]).copy()

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

extended_state = {}  # symbol -> {lifetime_high, lifetime_low, updated}

def fetch_quote(symbol):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="5d")
    if hist.empty or len(hist) < 1:
        raise ValueError(f"No data for {symbol}")
    current = hist.iloc[-1]
    prev    = hist.iloc[-2] if len(hist) > 1 else current
    result = {
        "close":      round(float(current["Close"]), 2),
        "prev_close": round(float(prev["Close"]), 2),
        "open":       round(float(current["Open"]), 2),
        "high":       round(float(current["High"]), 2),
        "low":        round(float(current["Low"]), 2),
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

def fetch_extended_data(symbol):
    try:
        hist = yf.Ticker(symbol).history(period="max")
        if not hist.empty:
            extended_state[symbol] = {
                "lifetime_high": round(float(hist["High"].max()), 2),
                "lifetime_low":  round(float(hist["Low"].min()),  2),
                "updated": int(time.time()),
            }
    except: pass

def refresh_extended_loop():
    stocks = get_stocks()
    for s in stocks:
        fetch_extended_data(s["symbol"])
    while True:
        time.sleep(86400)
        for s in get_stocks():
            fetch_extended_data(s["symbol"])

threading.Thread(target=refresh_extended_loop, daemon=True).start()

def store_price(symbol, q):
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO prices
            (symbol,timestamp,open,high,low,close,prev_close,pre_market,post_market)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (symbol,q["timestamp"],q["open"],q["high"],q["low"],
             q["close"],q["prev_close"],q.get("pre_market"),q.get("post_market")))
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
            "SELECT timestamp,close,high,low FROM prices WHERE symbol=? AND timestamp>? ORDER BY timestamp ASC",
            (symbol, cutoff)
        ).fetchall()
        return [dict(r) for r in rows]

def log_alert(symbol, rule_type, message, price, detail=None):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol,timestamp,rule_type,message,price,detail) VALUES (?,?,?,?,?,?)",
            (symbol, int(time.time()), rule_type, message, price, detail)
        )
        conn.commit()

def get_alerts(limit=200):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

# ─────────────────────────────────────────────────────────────────────────────
# RULE EVALUATION
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_rules(symbol, quote, history, params):
    results = []
    close, prev = quote["close"], quote["prev_close"]

    # Rule 1: Volatility
    r1 = {"rule_type":"volatility","label":"Unusual Daily Move"}
    if prev and prev > 0:
        daily_pct = abs((close - prev) / prev) * 100
        closes = [h["close"] for h in history[-params["volatility_lookback"]:]]
        if len(closes) >= 2:
            changes = [abs((closes[i]-closes[i-1])/closes[i-1])*100 for i in range(1,len(closes))]
            avg_vol   = round(statistics.mean(changes), 2)
            threshold = round(avg_vol * params["volatility_multiplier"], 2)
            triggered = daily_pct > threshold
            r1.update({
                "description": f"Fires when today's move exceeds {params['volatility_multiplier']}× the {params['volatility_lookback']}-day average daily move.",
                "rationale": "Large single-day moves relative to a stock's own history often precede or follow major catalysts — earnings, institutional repositioning, or macro events. When a stock moves far beyond its typical daily range, the cause warrants investigation before acting.",
                "param_summary": f"Multiplier: {params['volatility_multiplier']}×  |  Lookback: {params['volatility_lookback']} days",
                "actual_value": round(daily_pct,2), "threshold": threshold,
                "avg_daily_vol": avg_vol, "triggered": triggered,
                "direction": "UP" if close > prev else "DOWN",
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
    closes = [h["close"] for h in history[-params["support_resist_lookback"]:]]
    if len(closes) >= 5:
        lo, hi = min(closes), max(closes)
        support    = round(lo * (1 - sr_pct), 2)
        resistance = round(hi * (1 + sr_pct), 2)
        below = close < support
        above = close > resistance
        triggered = below or above
        r2.update({
            "description": f"Fires when price breaks {params['support_resist_pct']}% beyond the {params['support_resist_lookback']}-day high or low.",
            "rationale": "Price levels where a stock has repeatedly found buying (support) or selling (resistance) act as psychological anchors. A decisive break through these zones signals a potential regime shift — either a breakdown or a breakout — and often leads to accelerated price movement in the breakout direction.",
            "param_summary": f"Buffer: {params['support_resist_pct']}%  |  Lookback: {params['support_resist_lookback']} days",
            "support": support, "resistance": resistance,
            "period_low": round(lo,2), "period_high": round(hi,2),
            "actual_value": close, "triggered": triggered,
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
            r3.update({
                "description": description,
                "rationale": rationale,
                "param_summary": f"Min consecutive days: {threshold}",
                "actual_value": cur_run if streak_active else max_downs,
                "current_run": cur_run, "max_run": max_downs,
                "streak_active": streak_active, "streak_resolved": streak_resolved,
                "threshold": threshold, "triggered": triggered,
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

    return results

# ─────────────────────────────────────────────────────────────────────────────
# MONITOR LOOP
# ─────────────────────────────────────────────────────────────────────────────

state = {"last_check": None, "results": {}, "checking": False}

def run_check(symbols=None):
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
            for rule in rules:
                if rule.get("triggered") and not rule.get("disabled"):
                    detail = json.dumps({
                        "actual_value": rule.get("actual_value"),
                        "threshold":    rule.get("threshold"),
                        "direction":    rule.get("direction"),
                        "support":      rule.get("support"),
                        "resistance":   rule.get("resistance"),
                        "avg_daily_vol":rule.get("avg_daily_vol"),
                        "period_low":   rule.get("period_low"),
                        "period_high":  rule.get("period_high"),
                        "description":  rule.get("description"),
                        "rationale":    rule.get("rationale"),
                    })
                    log_alert(sym, rule["rule_type"], rule["message"], quote["close"], detail)
            hist30 = get_history(sym, days=30)
            state["results"][sym] = {
                "quote": quote, "rules": rules, "params": params, "error": None,
                "week52_high":   quote.get("week52_high"),
                "week52_low":    quote.get("week52_low"),
                "history_closes": [h["close"] for h in hist30],
            }
        except Exception as e:
            state["results"][sym] = {"error": str(e), "rules": [], "params": {}, "history_closes": []}
    state["last_check"] = datetime.now().isoformat()
    state["checking"] = False

def monitor_loop():
    run_check()
    while True:
        time.sleep(60)
        run_check()

threading.Thread(target=monitor_loop, daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    stocks = get_stocks()
    return jsonify({
        "status": "checking" if state["checking"] else "running",
        "stocks": len(stocks),
        "last_check": state["last_check"],
    })

@app.route("/api/stocks")
def api_stocks():
    results = []
    for s in get_stocks():
        sym, cat = s["symbol"], s["category"]
        row    = get_latest(sym)
        cached = state["results"].get(sym, {})
        rules  = cached.get("rules", [])
        any_alert = any(r.get("triggered") and not r.get("disabled") for r in rules)
        ext = extended_state.get(sym, {})
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
                "history_closes": cached.get("history_closes", []),
            })
        else:
            results.append({"symbol": sym, "category": cat, "price": None,
                            "error": cached.get("error", "Waiting for first check..."),
                            "rules": [], "history_closes": []})
    return jsonify(results)

@app.route("/api/alerts")
def api_alerts():
    rows = get_alerts(200)
    return jsonify({"alerts": [{
        "id": r["id"], "symbol": r["symbol"], "rule_type": r["rule_type"],
        "message": r["message"], "price": r["price"],
        "detail": r["detail"],
        "time": datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M"),
        "ts": r["timestamp"],
    } for r in rows]})

@app.route("/api/check", methods=["POST"])
def api_check():
    if state["checking"]:
        return jsonify({"success": False, "error": "Check already in progress"})
    symbols = request.json.get("symbols") if request.json else None
    threading.Thread(target=run_check, args=(symbols,), daemon=True).start()
    return jsonify({"success": True})

@app.route("/api/stocks/add", methods=["POST"])
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
    threading.Thread(target=run_check, args=([symbol],), daemon=True).start()
    threading.Thread(target=fetch_extended_data, args=(symbol,), daemon=True).start()
    return jsonify({"success": True, "symbol": symbol, "price": quote["close"]})

@app.route("/api/stocks/remove", methods=["POST"])
def api_remove_stock():
    symbol = request.json.get("symbol","").upper().strip()
    with get_db() as conn:
        conn.execute("UPDATE stocks SET active=0 WHERE symbol=?", (symbol,))
        conn.commit()
    state["results"].pop(symbol, None)
    return jsonify({"success": True})

@app.route("/api/stock/<symbol>/category", methods=["POST"])
def api_set_category(symbol):
    cat = request.json.get("category","high_vol")
    with get_db() as conn:
        conn.execute("UPDATE stocks SET category=? WHERE symbol=?", (cat, symbol.upper()))
        conn.commit()
    return jsonify({"success": True})

@app.route("/api/stock/<symbol>/params", methods=["POST"])
def api_set_params(symbol):
    sym    = symbol.upper()
    data   = request.json
    stocks = {s["symbol"]: s for s in get_stocks()}
    cat    = stocks.get(sym, {}).get("category","high_vol")
    current = get_params(sym, cat)
    current.update({k: v for k, v in data.items() if k in current})
    save_params(sym, current)
    threading.Thread(target=run_check, args=([sym],), daemon=True).start()
    return jsonify({"success": True, "params": current})

@app.route("/api/stock/<symbol>/params/reset", methods=["POST"])
def api_reset_params(symbol):
    sym = symbol.upper()
    reset_params(sym)
    threading.Thread(target=run_check, args=([sym],), daemon=True).start()
    return jsonify({"success": True})

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
#stock-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px}
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

.err-banner{background:#EF444415;border:1px solid #EF4444;color:#EF4444;padding:10px 16px;font-size:13px;margin-bottom:12px;border-radius:8px}
.no-alerts{color:#6B7280;font-size:14px;padding:30px;text-align:center}
</style>
</head>
<body>

<div id="topbar">
  <div id="logo">⚡ Tripwire</div>
  <div id="topbar-right">
    <span><span id="status-dot"></span><span id="status-txt">Connecting...</span></span>
    <span id="last-check-txt"></span>
    <button id="btn-check" onclick="manualCheck()">▶ Check Now</button>
  </div>
</div>

<div id="tabs">
  <button class="tab active" onclick="switchTab('stocks',this)">Stocks</button>
  <button class="tab" onclick="switchTab('alerts',this)" id="tab-alerts-btn">Alerts</button>
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
    <div id="alert-pane"></div>
  </div>
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
async function fetchJSON(url){ const r=await fetch(url); return r.json(); }
async function postJSON(url,body){ const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); return r.json(); }

async function loadAll(){
  try{
    const [s,a,st]=await Promise.all([fetchJSON('/api/stocks'),fetchJSON('/api/alerts'),fetchJSON('/api/status')]);
    stocks=s; alerts=a.alerts||[];
    renderStatus(st);
    renderGrid();
    renderAlerts();
    document.getElementById('err-banner').style.display='none';
  }catch(e){
    document.getElementById('err-banner').textContent='Cannot reach backend. Make sure app.py is running.';
    document.getElementById('err-banner').style.display='block';
  }
}
setInterval(loadAll,5000);
loadAll();

// ── Status ────────────────────────────────────────────────────────────────────
function renderStatus(st){
  checking=st.status==='checking';
  document.getElementById('status-dot').className=checking?'checking':'';
  document.getElementById('status-txt').textContent=checking?'Checking...':'Running';
  document.getElementById('btn-check').disabled=checking;
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

// ── Tabs ──────────────────────────────────────────────────────────────────────
function switchTab(name,btn){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('pane-stocks').style.display=name==='stocks'?'':'none';
  document.getElementById('pane-alerts').style.display=name==='alerts'?'':'none';
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

    const triggeredHTML=triggered.length>0?`
      <div class="card-triggered">
        <div class="card-triggered-hdr">⚠ ${triggered.length} rule${triggered.length>1?'s':''} triggered</div>
        ${triggered.map(r=>`<div class="card-triggered-item">
          <span class="ti-dot">▸</span>
          <span><span class="ti-label">${shortRuleLabel(r.rule_type)}:</span> ${cardRuleDetail(r)}</span>
        </div>`).join('')}
      </div>`:'';

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
        ${triggeredHTML}
      `:`<div class="stock-err">${s.error||'Loading...'}</div>`}
    </div>`;
  }).join('');

  if(selectedSym){const s=stocks.find(x=>x.symbol===selectedSym);if(s)renderDetail(s);}
}

function shortRuleLabel(rt){
  if(rt==='volatility') return 'Move';
  if(rt==='support_resistance') return 'S/R';
  if(rt==='consecutive_down') return 'Consec';
  return rt;
}

function cardRuleDetail(r){
  if(r.rule_type==='volatility') return `${r.direction==='UP'?'▲':'▼'} ${r.actual_value}% (thresh ${r.threshold}%)`;
  if(r.rule_type==='support_resistance'){
    if(r.actual_value<r.support) return `$${r.actual_value} below support $${r.support}`;
    return `$${r.actual_value} above resistance $${r.resistance}`;
  }
  if(r.rule_type==='consecutive_down') return `${r.actual_value} days down`;
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
      ${(s.rules||[]).map((r,i)=>ruleHTML(s.symbol,r,i,s.history_closes)).join('')}
      ${(!s.rules||s.rules.length===0)?'<div style="color:#6B7280;font-size:13px">No rule data yet — click Check Now</div>':''}
    </div>`;
}

// ── Rule card ─────────────────────────────────────────────────────────────────
function ruleHTML(sym,r,idx,historyClosed){
  const badge=r.disabled?'<span class="badge badge-disabled">DISABLED</span>':
               r.triggered?'<span class="badge badge-alert">⚠ ALERT</span>':
               '<span class="badge badge-ok">✓ OK</span>';
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

  const editFields={
    volatility:[{key:'volatility_multiplier',label:'Multiplier',step:0.1,min:0.5,max:10},{key:'volatility_lookback',label:'Lookback (days)',step:1,min:5,max:60}],
    support_resistance:[{key:'support_resist_pct',label:'Buffer (%)',step:0.5,min:0.5,max:20},{key:'support_resist_lookback',label:'Lookback (days)',step:1,min:5,max:180}],
    consecutive_down:[{key:'consecutive_down_days',label:'Min days',step:1,min:2,max:10}],
  }[r.rule_type]||[];

  const editRows=editFields.map(f=>`
    <div class="edit-row">
      <label>${f.label}</label>
      <input type="number" id="ep_${sym}_${f.key}" step="${f.step}" min="${f.min}" max="${f.max}" placeholder="value">
    </div>`).join('');

  return `<div class="rule-card${alertCls}">
    <div class="rule-header">
      <div class="rule-title">${r.label}</div>
      <div class="rule-header-right">
        ${badge}
        ${editFields.length>0?`<button class="edit-toggle" onclick="toggleEdit('${sym}','${r.rule_type}')">Edit</button>`:''}
      </div>
    </div>
    <div class="rule-desc">${r.description||''}</div>
    ${r.rationale?`<div class="rule-rationale">${r.rationale}</div>`:''}
    ${r.param_summary?`<div class="rule-params-line">⚙ ${r.param_summary}</div>`:''}
    ${chart}
    ${vals.length>0?`<div class="rule-vals" style="margin-top:10px">${vals.join('')}</div>`:''}
    <div class="rule-msg${msgCls}">▶ ${r.message}</div>
    <div id="edit_${sym}_${r.rule_type}" style="display:none">
      <div class="edit-panel">
        <div style="font-size:12px;font-weight:700;color:#3B82F6;margin-bottom:10px">Edit Parameters</div>
        ${editRows}
        <div class="edit-actions">
          <button class="btn-save" onclick="saveParams('${sym}','${r.rule_type}')">Save</button>
          <button class="btn-reset" onclick="resetParams('${sym}')">Reset to defaults</button>
        </div>
      </div>
    </div>
  </div>`;
}

function toggleEdit(sym,ruleType){
  const el=document.getElementById('edit_'+sym+'_'+ruleType);
  if(el) el.style.display=el.style.display==='none'?'block':'none';
}

async function saveParams(sym,ruleType){
  const fields={
    volatility:['volatility_multiplier','volatility_lookback'],
    support_resistance:['support_resist_pct','support_resist_lookback'],
    consecutive_down:['consecutive_down_days'],
  }[ruleType]||[];
  const params={};
  fields.forEach(k=>{
    const el=document.getElementById('ep_'+sym+'_'+k);
    if(el&&el.value!=='') params[k]=parseFloat(el.value);
  });
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

      const ruleLabel={volatility:'Unusual Move',support_resistance:'S/R Breach',consecutive_down:'Consec. Down'}[a.rule_type]||a.rule_type.replace(/_/g,' ');

      return `<div class="alert-entry">
        <div class="alert-entry-header">
          <span class="alert-entry-time">${a.time}</span>
          <span class="alert-entry-rule">${ruleLabel}</span>
          <span class="alert-entry-price">Price: $${a.price}</span>
        </div>
        <div class="alert-entry-msg">${a.message}</div>
        ${detail.description?`<div class="alert-entry-detail">${detail.description}</div>`:''}
        ${detailVals.length>0?`<div class="alert-detail-vals">${detailVals.join('')}</div>`:''}
        ${detail.rationale?`<div class="alert-rationale">${detail.rationale}</div>`:''}
      </div>`;
    }).join('');

    return `<div class="alert-group">
      <div class="alert-group-hdr" onclick="toggleAlertGroup('${sym}')">
        <span class="alert-group-sym">${sym}</span>
        <span class="alert-group-cnt">${entries.length} alert${entries.length>1?'s':''}</span>
        <span class="alert-group-cat" id="ag-cat-${sym}"></span>
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
</script>
</body>
</html>"""

@app.route("/")
def dashboard():
    return Response(DASHBOARD, mimetype="text/html")

if __name__ == "__main__":
    import webbrowser
    print("\n" + "="*60)
    print("  Tripwire v3 starting...")
    print("  Dashboard: http://localhost:5000")
    print("  Press Ctrl+C to stop")
    print("="*60 + "\n")
    threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5000")).start()
    app.run(port=5000, use_reloader=False)
