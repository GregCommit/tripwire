"""
Tripwire v3 - Single file, self-contained
==========================================
One Python file. No Node.js. No npm. No two windows.

Run:  python app.py
Open: http://localhost:5000

Features:
- Serves its own HTML/JS dashboard (no React build needed)
- Live price updates every 60 seconds via background thread
- Pre/post market prices
- Add/remove tickers via the UI
- Rule inspector per stock (volatility, support/resistance, consecutive downs)
- Edit rule parameters per stock
- Alert log
- Persistent storage (SQLite)
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
                rule_type TEXT, message TEXT, price REAL
            );
            CREATE TABLE IF NOT EXISTS rule_params (
                symbol TEXT PRIMARY KEY,
                params TEXT
            );
        """)
        # Default watchlist
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
    }
    try:
        info = ticker.fast_info
        pre  = getattr(info, "pre_market_price",  None)
        post = getattr(info, "post_market_price", None)
        if pre  and pre  > 0: result["pre_market"]  = round(float(pre),  2)
        if post and post > 0: result["post_market"] = round(float(post), 2)
    except: pass
    return result

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

def log_alert(symbol, rule_type, message, price):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol,timestamp,rule_type,message,price) VALUES (?,?,?,?,?)",
            (symbol, int(time.time()), rule_type, message, price)
        )
        conn.commit()

def get_alerts(limit=100):
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
            "description": f"Fires when price breaks {params['support_resist_pct']}% below the {params['support_resist_lookback']}-day low or above the {params['support_resist_lookback']}-day high.",
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
        closes = [h["close"] for h in history[-10:]]
        if len(closes) >= 4:
            downs = max_downs = 0
            for i in range(1, len(closes)):
                if closes[i] < closes[i-1]: downs += 1; max_downs = max(max_downs, downs)
                else: downs = 0
            threshold = params["consecutive_down_days"]
            triggered = max_downs >= threshold
            r3.update({
                "description": f"Fires after {threshold}+ consecutive down days — signals potential reversal.",
                "param_summary": f"Min consecutive days: {threshold}",
                "actual_value": max_downs, "threshold": threshold, "triggered": triggered,
                "message": f"{max_downs} consecutive down days ({'ALERT' if triggered else f'OK, threshold {threshold}'})",
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
            history = get_history(sym)
            params  = get_params(sym, cat)
            rules   = evaluate_rules(sym, quote, history, params)
            for rule in rules:
                if rule.get("triggered") and not rule.get("disabled"):
                    log_alert(sym, rule["rule_type"], rule["message"], quote["close"])
            state["results"][sym] = {"quote": quote, "rules": rules, "params": params, "error": None}
        except Exception as e:
            state["results"][sym] = {"error": str(e), "rules": [], "params": {}}
    state["last_check"] = datetime.now().isoformat()
    state["checking"] = False

def monitor_loop():
    # Initial check on startup
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
        row     = get_latest(sym)
        cached  = state["results"].get(sym, {})
        rules   = cached.get("rules", [])
        any_alert = any(r.get("triggered") and not r.get("disabled") for r in rules)
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
            })
        else:
            results.append({"symbol": sym, "category": cat, "price": None,
                            "error": cached.get("error", "Waiting for first check...")})
    return jsonify(results)

@app.route("/api/alerts")
def api_alerts():
    rows = get_alerts(100)
    return jsonify({"alerts": [{
        "id": r["id"], "symbol": r["symbol"], "rule_type": r["rule_type"],
        "message": r["message"], "price": r["price"],
        "time": datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M")
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
    # Validate via Yahoo
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
    # Run rules immediately
    threading.Thread(target=run_check, args=([symbol],), daemon=True).start()
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
# SERVE DASHBOARD (inline HTML — no Node.js needed)
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tripwire</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:#0F1117;color:#E4E0D8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh}
  button{cursor:pointer;border:none;outline:none}
  input{outline:none}

  /* Top bar */
  #topbar{background:#1A1D27;border-bottom:1px solid #2A2D3E;padding:0 20px;height:52px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
  #logo{font-weight:800;font-size:17px;color:#F59E0B;letter-spacing:-0.5px}
  #topbar-right{display:flex;align-items:center;gap:12px;font-size:13px;color:#6B7280}
  #status-dot{width:8px;height:8px;border-radius:50%;background:#10B981;display:inline-block;margin-right:4px}
  #status-dot.checking{background:#F59E0B}
  #btn-check{background:#F59E0B;color:#000;border-radius:6px;padding:7px 16px;font-weight:700;font-size:13px}
  #btn-check:disabled{opacity:0.5;cursor:not-allowed}

  /* Tabs */
  #tabs{display:flex;gap:0;border-bottom:1px solid #2A2D3E;background:#1A1D27;padding:0 20px}
  .tab{padding:12px 18px;font-size:14px;color:#6B7280;border-bottom:2px solid transparent;cursor:pointer;background:none;border-left:none;border-right:none;border-top:none}
  .tab.active{color:#F59E0B;border-bottom-color:#F59E0B;font-weight:700}

  /* Content */
  #content{padding:20px;max-width:1400px;margin:0 auto}

  /* Add ticker bar */
  #add-bar{background:#1A1D27;border:1px solid #2A2D3E;border-radius:10px;padding:14px 16px;margin-bottom:16px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}
  #add-bar input{background:#0F1117;border:1px solid #2A2D3E;color:#E4E0D8;border-radius:6px;padding:8px 12px;font-size:13px;width:120px}
  #add-bar select{background:#0F1117;border:1px solid #2A2D3E;color:#E4E0D8;border-radius:6px;padding:8px 12px;font-size:13px}
  #btn-add{background:#10B981;color:#fff;border-radius:6px;padding:8px 16px;font-weight:700;font-size:13px}
  #add-status{font-size:12px;color:#6B7280}
  #add-status.err{color:#EF4444}
  #add-status.ok{color:#10B981}

  /* Stock grid */
  #stock-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:10px}
  .stock-card{background:#1E2130;border:1px solid #2A2D3E;border-radius:10px;padding:14px;cursor:pointer;transition:border-color .15s;position:relative}
  .stock-card:hover{border-color:#4B5563}
  .stock-card.selected{border-color:#F59E0B}
  .stock-card.alert{border-color:#F59E0B88}
  .alert-dot{position:absolute;top:9px;right:9px;width:7px;height:7px;border-radius:50%;background:#F59E0B}
  .stock-symbol{font-weight:800;font-size:14px;margin-bottom:2px}
  .stock-cat{font-size:10px;color:#6B7280;margin-bottom:8px}
  .stock-price{font-size:20px;font-weight:800;margin-bottom:2px}
  .stock-pct{font-size:13px;margin-bottom:6px}
  .stock-ext{font-size:10px;margin-top:4px}
  .stock-time{font-size:10px;color:#6B7280;margin-top:4px}
  .stock-err{font-size:11px;color:#EF4444;margin-top:4px}
  .up{color:#10B981}.dn{color:#EF4444}.muted{color:#6B7280}
  .pre-clr{color:#A78BFA}.post-clr{color:#60A5FA}
  .remove-btn{position:absolute;top:8px;left:8px;background:#1A1D27;border:1px solid #2A2D3E;color:#6B7280;border-radius:4px;font-size:10px;padding:1px 5px;display:none}
  .stock-card:hover .remove-btn{display:block}

  /* Detail panel */
  #detail{background:#1A1D27;border:1px solid #2A2D3E;border-radius:12px;padding:20px;margin-bottom:16px}
  #detail-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px}
  #detail-title{font-size:22px;font-weight:800}
  #detail-meta{font-size:12px;color:#6B7280;margin-top:3px}
  #btn-close{background:#2A2D3E;color:#E4E0D8;border-radius:6px;padding:7px 14px;font-size:13px}
  #price-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:8px;margin-bottom:16px}
  .price-cell{background:#0F1117;border-radius:8px;padding:10px 12px}
  .price-cell-label{font-size:10px;color:#6B7280;margin-bottom:4px;letter-spacing:.5px}
  .price-cell-val{font-size:17px;font-weight:800}
  .price-cell-sub{font-size:11px;color:#6B7280;margin-top:2px}

  /* Rules */
  .rules-label{font-size:11px;color:#6B7280;letter-spacing:1px;margin-bottom:8px;font-weight:700}
  .rule-card{background:#0F1117;border:1px solid #2A2D3E;border-radius:8px;padding:14px;margin-bottom:8px}
  .rule-card.alert{border-color:#F59E0B}
  .rule-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
  .rule-title{font-weight:700;font-size:13px}
  .badge{border-radius:4px;padding:2px 8px;font-size:10px;font-weight:700;letter-spacing:.5px}
  .badge-ok{background:#10B98122;color:#10B981;border:1px solid #10B98144}
  .badge-alert{background:#F59E0B22;color:#F59E0B;border:1px solid #F59E0B44}
  .badge-disabled{background:#6B728022;color:#6B7280;border:1px solid #6B728044}
  .rule-desc{font-size:11px;color:#6B7280;margin-bottom:8px}
  .rule-params-line{font-size:11px;color:#3B82F6;margin-bottom:8px}
  .rule-vals{display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:6px;margin-bottom:8px}
  .val-cell{background:#1A1D27;border-radius:6px;padding:6px 8px}
  .val-cell-label{font-size:10px;color:#6B7280;margin-bottom:2px}
  .val-cell-val{font-size:13px;font-weight:700}
  .rule-msg{font-size:12px;background:#1A1D27;border-radius:6px;padding:7px 10px;color:#9CA3AF}
  .rule-msg.alert-msg{color:#F59E0B}
  .edit-toggle{background:#2A2D3E;color:#E4E0D8;border-radius:4px;padding:3px 10px;font-size:11px}
  .edit-panel{background:#1A1D27;border:1px solid #2A2D3E;border-radius:8px;padding:12px;margin-top:10px}
  .edit-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
  .edit-row label{font-size:12px;color:#6B7280}
  .edit-row input{background:#0F1117;border:1px solid #2A2D3E;color:#E4E0D8;border-radius:4px;padding:4px 8px;font-size:12px;width:80px}
  .edit-actions{display:flex;gap:8px;margin-top:10px}
  .btn-save{background:#F59E0B;color:#000;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:700}
  .btn-reset{background:#2A2D3E;color:#E4E0D8;border-radius:6px;padding:6px 14px;font-size:12px}

  /* Alert log */
  #alert-log{background:#1A1D27;border:1px solid #2A2D3E;border-radius:10px;padding:16px}
  #alert-log h3{font-size:15px;margin-bottom:12px}
  .alert-row{display:grid;grid-template-columns:140px 70px 160px 1fr;gap:8px;padding:7px 0;border-bottom:1px solid #2A2D3E;font-size:12px;align-items:center}
  .alert-row .a-time{color:#6B7280}
  .alert-row .a-sym{font-weight:700}
  .alert-row .a-rule{color:#F59E0B}
  .err-banner{background:#EF444422;border:1px solid #EF4444;color:#EF4444;padding:10px 16px;font-size:13px;margin-bottom:12px;border-radius:8px}
</style>
</head>
<body>

<!-- Top bar -->
<div id="topbar">
  <div id="logo">⚡ Tripwire</div>
  <div id="topbar-right">
    <span><span id="status-dot"></span><span id="status-txt">Connecting...</span></span>
    <span id="last-check-txt"></span>
    <button id="btn-check" onclick="manualCheck()">▶ Check Now</button>
  </div>
</div>

<!-- Tabs -->
<div id="tabs">
  <button class="tab active" onclick="switchTab('stocks',this)">Stocks</button>
  <button class="tab" onclick="switchTab('alerts',this)" id="tab-alerts">Alerts</button>
</div>

<div id="content">
  <div id="err-banner" class="err-banner" style="display:none"></div>

  <!-- STOCKS TAB -->
  <div id="tab-stocks">

    <!-- Add ticker -->
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

    <!-- Detail panel -->
    <div id="detail" style="display:none"></div>

    <!-- Grid -->
    <div id="stock-grid"></div>
  </div>

  <!-- ALERTS TAB -->
  <div id="tab-alerts" style="display:none">
    <div id="alert-log"><h3>Alert Log</h3><div id="alert-rows"></div></div>
  </div>
</div>

<script>
const API = '';  // same origin
let stocks=[], alerts=[], selectedSym=null, editingRule=null, checking=false;

// ── Fetch helpers ────────────────────────────────────────────────────────────
async function fetchJSON(url){ const r=await fetch(API+url); return r.json(); }
async function postJSON(url,body){ const r=await fetch(API+url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); return r.json(); }

// ── Main data loop ────────────────────────────────────────────────────────────
async function loadAll(){
  try{
    const [s,a,st]=await Promise.all([
      fetchJSON('/api/stocks'),
      fetchJSON('/api/alerts'),
      fetchJSON('/api/status'),
    ]);
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
setInterval(loadAll, 5000);
loadAll();

// ── Status bar ───────────────────────────────────────────────────────────────
function renderStatus(st){
  checking = st.status==='checking';
  const dot=document.getElementById('status-dot');
  dot.className = checking?'checking':'';
  document.getElementById('status-txt').textContent = checking?'Checking...':'Running';
  document.getElementById('btn-check').disabled = checking;
  if(st.last_check){
    const d=new Date(st.last_check);
    document.getElementById('last-check-txt').textContent='Last: '+d.toLocaleTimeString();
  }
}

// ── Manual check ─────────────────────────────────────────────────────────────
async function manualCheck(){
  document.getElementById('btn-check').disabled=true;
  await postJSON('/api/check',{});
  setTimeout(loadAll,1500);
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function switchTab(name,btn){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('tab-stocks').style.display=name==='stocks'?'':'none';
  document.getElementById('tab-alerts').style.display=name==='alerts'?'':'none';
}

// ── Add stock ─────────────────────────────────────────────────────────────────
async function addStock(){
  const sym=document.getElementById('inp-symbol').value.trim().toUpperCase();
  const cat=document.getElementById('inp-cat').value;
  const st=document.getElementById('add-status');
  if(!sym){ st.textContent='Enter a symbol'; st.className='err'; return; }
  st.textContent='Looking up '+sym+'...'; st.className='';
  const r=await postJSON('/api/stocks/add',{symbol:sym,category:cat});
  if(r.success){
    st.textContent=sym+' added @ $'+r.price; st.className='ok';
    document.getElementById('inp-symbol').value='';
    await loadAll();
  } else {
    st.textContent=r.error; st.className='err';
  }
}

// ── Remove stock ──────────────────────────────────────────────────────────────
async function removeStock(sym,e){
  e.stopPropagation();
  if(!confirm('Remove '+sym+' from watchlist?')) return;
  await postJSON('/api/stocks/remove',{symbol:sym});
  if(selectedSym===sym){ selectedSym=null; document.getElementById('detail').style.display='none'; }
  await loadAll();
}

// ── Stock grid ────────────────────────────────────────────────────────────────
function renderGrid(){
  const grid=document.getElementById('stock-grid');
  const alertCount=stocks.filter(s=>s.alert).length;
  document.getElementById('tab-alerts').textContent='Alerts'+(alertCount>0?' ('+alertCount+')':'');

  grid.innerHTML=stocks.map(s=>{
    const pctCls=s.pct>0?'up':s.pct<0?'dn':'muted';
    const sign=s.pct>0?'+':'';
    const selCls=selectedSym===s.symbol?' selected':'';
    const alertCls=s.alert?' alert':'';
    return `<div class="stock-card${selCls}${alertCls}" onclick="selectStock('${s.symbol}')">
      <button class="remove-btn" onclick="removeStock('${s.symbol}',event)">✕</button>
      ${s.alert?'<div class="alert-dot"></div>':''}
      <div class="stock-symbol">${s.symbol}</div>
      <div class="stock-cat">${(s.category||'').replace('_vol',' vol')}</div>
      ${s.price!=null?`
        <div class="stock-price">$${s.price}</div>
        <div class="stock-pct ${pctCls}">${sign}${s.pct}%</div>
        <div class="stock-ext">
          ${s.pre_market?`<span class="pre-clr">Pre $${s.pre_market}</span> `:''}
          ${s.post_market?`<span class="post-clr">Post $${s.post_market}</span>`:''}
        </div>
        <div class="stock-time">${s.date} ${s.time}</div>
      `:`<div class="stock-err">${s.error||'Loading...'}</div>`}
    </div>`;
  }).join('');

  if(selectedSym){ const s=stocks.find(x=>x.symbol===selectedSym); if(s) renderDetail(s); }
}

// ── Select stock ──────────────────────────────────────────────────────────────
function selectStock(sym){
  if(selectedSym===sym){ selectedSym=null; document.getElementById('detail').style.display='none'; renderGrid(); return; }
  selectedSym=sym;
  const s=stocks.find(x=>x.symbol===sym);
  if(s){ renderDetail(s); renderGrid(); }
}

// ── Detail panel ──────────────────────────────────────────────────────────────
function renderDetail(s){
  const panel=document.getElementById('detail');
  panel.style.display='block';
  const pctCls=s.pct>0?'up':s.pct<0?'dn':'muted';
  const sign=s.pct>0?'+':'';

  const preCellStyle=s.pre_market?'border-color:#A78BFA44':'';
  const postCellStyle=s.post_market?'border-color:#60A5FA44':'';

  panel.innerHTML=`
    <div id="detail-header">
      <div>
        <div id="detail-title">${s.symbol} <span style="font-size:13px;color:#6B7280;font-weight:400">${(s.category||'').replace('_vol',' vol')}</span></div>
        <div id="detail-meta">Updated: ${s.date} ${s.time}${s.alert?' · <span style="color:#F59E0B">⚠ Alert active</span>':''}</div>
      </div>
      <button id="btn-close" onclick="selectStock('${s.symbol}')">✕ Close</button>
    </div>
    <div id="price-grid">
      <div class="price-cell"><div class="price-cell-label">PRICE</div>
        <div class="price-cell-val">$${s.price||'—'}</div>
        <div class="price-cell-sub ${pctCls}">${sign}${s.pct}%</div></div>
      <div class="price-cell"><div class="price-cell-label">PREV CLOSE</div>
        <div class="price-cell-val">$${s.prev_close||'—'}</div></div>
      <div class="price-cell"><div class="price-cell-label">HIGH</div>
        <div class="price-cell-val up">$${s.high||'—'}</div></div>
      <div class="price-cell"><div class="price-cell-label">LOW</div>
        <div class="price-cell-val dn">$${s.low||'—'}</div></div>
      <div class="price-cell" style="${preCellStyle}">
        <div class="price-cell-label pre-clr">PRE-MARKET</div>
        <div class="price-cell-val">${s.pre_market?'$'+s.pre_market:'—'}</div>
        ${s.pre_market&&s.prev_close?`<div class="price-cell-sub">${pctStr(s.pre_market,s.prev_close)} vs close</div>`:''}
      </div>
      <div class="price-cell" style="${postCellStyle}">
        <div class="price-cell-label post-clr">POST-MARKET</div>
        <div class="price-cell-val">${s.post_market?'$'+s.post_market:'—'}</div>
        ${s.post_market&&s.price?`<div class="price-cell-sub">${pctStr(s.post_market,s.price)} vs close</div>`:''}
      </div>
    </div>
    <div class="rules-label">RULES</div>
    <div id="rules-container">
      ${(s.rules||[]).map((r,i)=>ruleHTML(s.symbol,r,i)).join('')}
      ${(!s.rules||s.rules.length===0)?'<div style="color:#6B7280;font-size:13px">No rule data yet — click Check Now</div>':''}
    </div>
  `;
}

function pctStr(a,b){ const p=((a-b)/b*100); return (p>0?'+':'')+p.toFixed(2)+'%'; }

function ruleHTML(sym,r,idx){
  const badge=r.disabled?'<span class="badge badge-disabled">DISABLED</span>':
               r.triggered?'<span class="badge badge-alert">⚠ ALERT</span>':
               '<span class="badge badge-ok">✓ OK</span>';
  const alertCls=r.triggered&&!r.disabled?' alert':'';
  const msgCls=r.triggered&&!r.disabled?' alert-msg':'';

  const vals=[];
  if(r.actual_value!=null){
    const unit=r.rule_type==='volatility'?'%':r.rule_type==='consecutive_down'?' days':'';
    vals.push(`<div class="val-cell"><div class="val-cell-label">ACTUAL</div><div class="val-cell-val" style="color:${r.triggered?'#F59E0B':'#E4E0D8'}">${r.actual_value}${unit}</div></div>`);
  }
  if(r.threshold!=null){
    const unit=r.rule_type==='volatility'?'%':r.rule_type==='consecutive_down'?' days':'';
    vals.push(`<div class="val-cell"><div class="val-cell-label">THRESHOLD</div><div class="val-cell-val">${r.threshold}${unit}</div></div>`);
  }
  if(r.avg_daily_vol!=null) vals.push(`<div class="val-cell"><div class="val-cell-label">AVG VOL</div><div class="val-cell-val">${r.avg_daily_vol}%</div></div>`);
  if(r.support!=null)       vals.push(`<div class="val-cell"><div class="val-cell-label">SUPPORT</div><div class="val-cell-val up">$${r.support}</div></div>`);
  if(r.resistance!=null)    vals.push(`<div class="val-cell"><div class="val-cell-label">RESIST</div><div class="val-cell-val dn">$${r.resistance}</div></div>`);
  if(r.period_low!=null)    vals.push(`<div class="val-cell"><div class="val-cell-label">PERIOD LOW</div><div class="val-cell-val">$${r.period_low}</div></div>`);
  if(r.period_high!=null)   vals.push(`<div class="val-cell"><div class="val-cell-label">PERIOD HIGH</div><div class="val-cell-val">$${r.period_high}</div></div>`);

  // Edit fields per rule type
  const editFields={
    volatility:[
      {key:'volatility_multiplier',label:'Multiplier',step:0.1,min:0.5,max:10},
      {key:'volatility_lookback',label:'Lookback (days)',step:1,min:5,max:60},
    ],
    support_resistance:[
      {key:'support_resist_pct',label:'Buffer (%)',step:0.5,min:0.5,max:20},
      {key:'support_resist_lookback',label:'Lookback (days)',step:1,min:5,max:180},
    ],
    consecutive_down:[
      {key:'consecutive_down_days',label:'Min days',step:1,min:2,max:10},
    ],
  }[r.rule_type]||[];

  const editRows=editFields.map(f=>`
    <div class="edit-row">
      <label>${f.label}</label>
      <input type="number" id="ep_${sym}_${f.key}" step="${f.step}" min="${f.min}" max="${f.max}" placeholder="value">
    </div>`).join('');

  return `<div class="rule-card${alertCls}" id="rule_${sym}_${idx}">
    <div class="rule-header">
      <div class="rule-title">${r.label}</div>
      <div style="display:flex;gap:8px;align-items:center">
        ${badge}
        ${editFields.length>0?`<button class="edit-toggle" onclick="toggleEdit('${sym}','${r.rule_type}')">Edit</button>`:''}
      </div>
    </div>
    <div class="rule-desc">${r.description||''}</div>
    ${r.param_summary?`<div class="rule-params-line">${r.param_summary}</div>`:''}
    ${vals.length>0?`<div class="rule-vals">${vals.join('')}</div>`:''}
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

// ── Alerts ────────────────────────────────────────────────────────────────────
function renderAlerts(){
  const el=document.getElementById('alert-rows');
  if(alerts.length===0){
    el.innerHTML='<div style="color:#6B7280;font-size:13px;padding:8px 0">No alerts yet — click Check Now to run the first check.</div>';
    return;
  }
  el.innerHTML=alerts.slice(0,50).map(a=>`
    <div class="alert-row">
      <span class="a-time">${a.time}</span>
      <span class="a-sym">${a.symbol}</span>
      <span class="a-rule">${(a.rule_type||'').replace(/_/g,' ')}</span>
      <span>${a.message}</span>
    </div>`).join('');
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
    # Open browser after short delay
    threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5000")).start()
    app.run(port=5000, use_reloader=False)
