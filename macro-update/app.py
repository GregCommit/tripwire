"""Macro Watch — keeps an eye on US stocks, bonds, commodities, investor mood, the yen and Japan's
inflation, and emails the owner when something moves unusually or crosses a level they set.

One Flask app + SQLite + a dashboard page (dashboard.html), built from the Web App Builder Kit
(same stack as Tripwire). A background thread refreshes the data (sources.py) and checks the
alert rules.

  python app.py            run locally on http://127.0.0.1:8062
"""
import hashlib
import hmac
import json
import logging
import os
import smtplib
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from email.message import EmailMessage
from functools import wraps
from html import escape
from pathlib import Path

from flask import Flask, jsonify, redirect, request, send_from_directory, session, url_for

import sources as S

HERE = Path(__file__).resolve().parent
APP_NAME = "Macro Watch"


def _load_dotenv():
    """Local development only: KEY=value lines from .env (never committed)."""
    env = HERE / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


_load_dotenv()
PASSWORD = os.environ.get("MACRO_PASSWORD", "")
GUEST_PASSWORD = os.environ.get("MACRO_GUEST_PASSWORD", "")
GUEST_EXPIRES = os.environ.get("MACRO_GUEST_EXPIRES", "").strip()   # YYYY-MM-DD, empty = never
PUBLIC_URL = os.environ.get("MACRO_PUBLIC_URL", "")
PORT = int(os.environ.get("MACRO_PORT", "8062"))
DATA_DIR = Path(os.environ.get("MACRO_DATA_DIR", Path.home() / ".macro"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "state.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("macro")

app = Flask(__name__, static_folder=str(HERE / "static"))
app.secret_key = os.environ.get("MACRO_SECRET_KEY") or os.urandom(32)
app.permanent_session_lifetime = timedelta(days=30)
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax",
                  SESSION_COOKIE_SECURE=PUBLIC_URL.startswith("https://"))

# How often each source is fetched (seconds); a failed fetch is retried after 10 minutes
EVERY = {"yahoo": 15 * 60, "fred": 2 * 3600, "bis": 12 * 3600, "boj": 12 * 3600, "hub": 3 * 3600}
SOURCE_NAMES = {"yahoo": "Yahoo Finance", "fred": "FRED (St. Louis Fed)", "bis": "BIS (Japan CPI)",
                "boj": "Bank of Japan forecasts", "hub": "Extremes (long histories)"}

DEFAULT_RULES = [
    ("MOOD", "below", 20), ("MOOD", "above", 80), ("VIX", "above", 30),
    ("USDJPY", "above", 160), ("USDJPY", "below", 140), ("CURVE", "below", 0),
    ("HYSPREAD", "above", 5), ("US10Y", "above", 5.25), ("WTI", "above", 120),
]


# ── Database ────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS access_log (id INTEGER PRIMARY KEY, ts INTEGER, role TEXT,
                ip TEXT, kind TEXT, what TEXT);
            CREATE TABLE IF NOT EXISTS series (key TEXT PRIMARY KEY, points TEXT, updated INTEGER);
            CREATE TABLE IF NOT EXISTS fetch_status (source TEXT PRIMARY KEY, ok_ts INTEGER,
                try_ts INTEGER, error TEXT);
            CREATE TABLE IF NOT EXISTS rules (id INTEGER PRIMARY KEY, key TEXT, op TEXT, level REAL,
                enabled INTEGER DEFAULT 1);
            CREATE TABLE IF NOT EXISTS alert_state (rule TEXT PRIMARY KEY, state TEXT);
            CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY, ts INTEGER, key TEXT, kind TEXT,
                title TEXT, detail TEXT, emailed INTEGER DEFAULT 0);
        """)
        if conn.execute("SELECT value FROM settings WHERE key='rules_seeded'").fetchone() is None:
            conn.executemany("INSERT INTO rules (key, op, level) VALUES (?,?,?)", DEFAULT_RULES)
            conn.execute("INSERT INTO settings VALUES ('rules_seeded','1')")


def get_setting(key, default=None):
    with get_db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key, value):
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))


def record_access(kind, what):
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
    with get_db() as conn:
        conn.execute("INSERT INTO access_log (ts, role, ip, kind, what) VALUES (?,?,?,?,?)",
                     (int(time.time()), session.get("role", "?"), ip, kind, what[:200]))


def save_series(key, pts):
    if not pts:
        return
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO series VALUES (?,?,?)",
                     (key, json.dumps([[d, round(v, 6)] for d, v in pts]), int(time.time())))


def load_series(key):
    with get_db() as conn:
        row = conn.execute("SELECT points FROM series WHERE key=?", (key,)).fetchone()
    return json.loads(row["points"]) if row else []


def load_all_series():
    with get_db() as conn:
        return {r["key"]: json.loads(r["points"]) for r in conn.execute("SELECT key, points FROM series")}


def mark_fetch(source, ok, error=""):
    now = int(time.time())
    with get_db() as conn:
        row = conn.execute("SELECT ok_ts FROM fetch_status WHERE source=?", (source,)).fetchone()
        ok_ts = now if ok else (row["ok_ts"] if row else None)
        conn.execute("INSERT OR REPLACE INTO fetch_status VALUES (?,?,?,?)",
                     (source, ok_ts, now, "" if ok else str(error)[:300]))


def fetch_status():
    with get_db() as conn:
        return {r["source"]: dict(r) for r in conn.execute("SELECT * FROM fetch_status")}


init_db()


# ── Refreshing the data ─────────────────────────────────────────────────────
def refresh_source(source):
    try:
        if source == "yahoo":
            inds = [i for i in S.INDICATORS if i["source"] == "yahoo"]
            got = S.fetch_yahoo([i["sym"] for i in inds])
            if not got:
                raise RuntimeError("Yahoo returned no prices")
            for i in inds:
                save_series(i["key"], got.get(i["sym"]))
            missing = [i["name"] for i in inds if i["sym"] not in got]
            if missing:
                log.warning("Yahoo missing: %s (keeping their previous data)", missing)
        elif source == "fred":
            errors = []
            for i in S.INDICATORS:
                if i["source"] != "fred":
                    continue
                try:
                    pts = S.fetch_fred(i["sym"])
                    save_series(i["key"], S.yoy(pts) if i.get("yoy") else pts)
                except Exception as e:
                    errors.append(f"{i['name']}: {e}")
            if errors:
                raise RuntimeError("; ".join(errors))
        elif source == "bis":
            save_series("JPCPI", S.fetch_japan_cpi())
        elif source == "boj":
            set_setting("boj_outlook", json.dumps(S.fetch_boj_outlook()))
        elif source == "hub":
            refresh_hub()
        mark_fetch(source, True)
        log.info("Refreshed %s", source)
    except Exception as e:
        log.warning("Refresh %s failed: %s", source, e)
        mark_fetch(source, False, e)


def refresh_hub():
    """Long histories for the Extremes tab. Each item succeeds or fails on its own; an item that
    fails keeps its previous data and its error is shown on the tab."""
    hd = S.HubData(lambda k: load_series(k))
    try:
        hd.load_yahoo()
    except Exception as e:
        log.warning("Hub Yahoo download failed: %s", e)
    errors, done = {}, 0
    for item in S.HUB:
        try:
            pts = S.build_hub(item, hd)
            if len(pts) < 36:
                raise RuntimeError(f"only {len(pts)} data points")
            save_series("H_" + item["key"], pts)
            done += 1
        except Exception as e:
            errors[item["key"]] = f"{type(e).__name__}: {e}"[:200]
            log.warning("Hub %s failed: %s", item["key"], e)
    set_setting("hub_errors", json.dumps(errors))
    if not done:
        raise RuntimeError("no Extremes series could be fetched: " + "; ".join(errors.values())[:250])
    if errors:
        log.warning("Hub: %d of %d items failed", len(errors), len(S.HUB))


_hub_cache = {"sig": None, "val": None}


def hub_summaries():
    """Every hub item that has data, with its percentile and zone (cached until the data changes)."""
    with get_db() as conn:
        sig = tuple(tuple(r) for r in conn.execute("SELECT key, updated, length(points), substr(points, -40) FROM series WHERE key GLOB 'H_*'"))
    if _hub_cache["sig"] == sig:
        return _hub_cache["val"]
    out = []
    for item in S.HUB:
        try:
            st = S.extreme_stats(item, load_series("H_" + item["key"]))
        except Exception as e:
            log.warning("extreme_stats %s: %s", item["key"], e)
            st = None
        if st:
            out.append(st)
    _hub_cache.update(sig=sig, val=out)
    return out


def compute_mood():
    mood, vr, latest = S.mood_series(load_series("SPX"), load_series("VIX"), load_series("IEF"),
                                     load_series("HYSPREAD"))
    save_series("MOOD", mood)
    save_series("VOLRATIO", vr)
    set_setting("mood_parts", json.dumps(latest))


def due_sources():
    st = fetch_status()
    now = time.time()
    due = []
    for src, every in EVERY.items():
        x = st.get(src, {})
        wait = every if x.get("ok_ts") and not x.get("error") else 600
        if now - (x.get("try_ts") or 0) >= wait:
            due.append(src)
    return due


_lock = threading.Lock()


def refresh_cycle(force=False):
    with _lock:
        todo = list(EVERY) if force else due_sources()
        for src in todo:
            refresh_source(src)
        if todo:
            try:
                compute_mood()
            except Exception as e:
                log.warning("Mood calculation failed: %s", e)
            try:
                check_alerts()
            except Exception as e:
                log.exception("Alert check failed: %s", e)
        return todo


def background_loop():
    time.sleep(3)
    while True:
        try:
            refresh_cycle()
        except Exception as e:
            log.exception("Refresh cycle failed: %s", e)
        time.sleep(60)


_started = False


def start_background():
    global _started
    if not _started and os.environ.get("MACRO_NO_BACKGROUND") != "1":
        _started = True
        threading.Thread(target=background_loop, daemon=True, name="refresh").start()


# ── Summaries and alerts ────────────────────────────────────────────────────
def summaries():
    data = load_all_series()
    out = {}
    for ind in S.INDICATORS:
        if ind["group"] is None:
            continue
        try:
            s = S.summarize(ind, data.get(ind["key"]))
        except Exception as e:
            log.warning("summarize %s: %s", ind["key"], e)
            s = None
        if s:
            out[ind["key"]] = s
    return out


def fmt(key, v):
    ind = S.BY_KEY[key]
    num = f"{v:,.{ind['dec']}f}"
    return ind["unit"] + num if ind["unit"] in ("$", "¥") else num + ind["unit"]


def fmt_hub(h, v):
    num = f"{v:,.{h['dec']}f}"
    return h["unit"] + num if h["unit"] in ("$", "¥") else num + h["unit"]


def date_label(d, freq):
    x = datetime.strptime(d, "%Y-%m-%d")
    return x.strftime("%b %Y") if freq in ("monthly", "quarterly") else x.strftime("%d %b %Y").lstrip("0")


OP_WORDS = {"above": "rose above", "below": "fell below"}


def rule_beyond(op, value, level):
    return value > level if op == "above" else value < level


def rule_back_inside(op, value, level):
    """A rule re-arms only after the value is clearly back (1% of the level, at least 0.05),
    so a number hovering around the level doesn't send an email every 15 minutes."""
    margin = max(abs(level) * 0.01, 0.05)
    return value < level - margin if op == "above" else value > level + margin


def get_rules():
    with get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM rules ORDER BY key, op")]


def check_alerts():
    """Compare the latest numbers with the rules; store new alerts and email them in one message.
    The very first check only records the current state (no flood of emails after setup)."""
    sm = summaries()
    with get_db() as conn:
        states = {r["rule"]: r["state"] for r in conn.execute("SELECT * FROM alert_state")}
    first_run = not states
    new, set_state = [], {}

    def raise_alert(key, kind, title, detail):
        if not first_run:
            new.append((key, kind, title, detail))

    # Levels the owner chose
    for r in get_rules():
        s = sm.get(r["key"])
        if not s or not r["enabled"]:
            continue
        rid = f"rule:{r['id']}"
        was = states.get(rid, "inside")
        if was != "beyond" and rule_beyond(r["op"], s["value"], r["level"]):
            set_state[rid] = "beyond"
            raise_alert(r["key"], "level",
                        f"{s['name']} {OP_WORDS[r['op']]} {fmt(r['key'], r['level'])}",
                        f"Now {fmt(r['key'], s['value'])} ({s['reading']}). {s['explain']}")
        elif was == "beyond" and rule_back_inside(r["op"], s["value"], r["level"]):
            set_state[rid] = "inside"
        elif rid not in states:
            set_state[rid] = was
    # Unusual moves (once per indicator per day of data)
    for key, s in sm.items():
        if s["unusual"]:
            rid = f"move:{key}"
            if states.get(rid) != s["date"]:
                set_state[rid] = s["date"]
                unit = S.BY_KEY[key]["unit"].strip() if S.BY_KEY[key]["unit"] not in ("$", "¥") else ""
                size = f"{s['d1']:+.1f}%" if s["chgKind"] == "pct" else f"{s['d1']:+.2f} {unit or 'pt'}"
                raise_alert(key, "move", f"Unusual move: {s['name']} {size}",
                            f"Now {fmt(key, s['value'])}. That is about {abs(s['z']):.1f}× its typical daily "
                            f"move over the past year. {s['reading']}.")
    # Indicators that reach an extreme of their own history (once on entering; re-armed when clearly back)
    if get_setting("extreme_alerts", "1") == "1":
        for h in hub_summaries():
            rid, is_ext = f"ext:{h['key']}", h["zone"].startswith("extreme")
            was = states.get(rid)
            if was is None:
                set_state[rid] = "beyond" if is_ext else "inside"      # first sight: note it, don't email it
            elif was != "beyond" and is_ext:
                set_state[rid] = "beyond"
                word = "highest" if h["side"] == "high" else "lowest"
                when = (f"{word} since {date_label(h['since'], h['freq'])}" if h["since"]
                        else f"{word} on record (data since {h['start'][:4]})")
                share = int(h["pct"]) if h["side"] == "high" else int(100 - h["pct"])
                raise_alert(h["key"], "extreme",
                            f"Extreme: {h['name']} at {fmt_hub(h, h['value'])} — {when}",
                            f"{'Higher' if h['side'] == 'high' else 'Lower'} than {share}% of all readings since "
                            f"{h['start'][:4]}. {h['meaning']}")
            elif was == "beyond" and h["zone"] == "quiet":
                set_state[rid] = "inside"
    # New official figures
    for key in ("JPCPI", "USCPI"):
        s = sm.get(key)
        if s:
            rid = f"news:{key}"
            if states.get(rid) != s["date"]:
                set_state[rid] = s["date"]
                if rid in states:
                    month = datetime.strptime(s["date"], "%Y-%m-%d").strftime("%B %Y")
                    prev = s["value"] - s["d1"] if s["d1"] is not None else None
                    raise_alert(key, "news", f"New figure — {s['name']}, {month}: {s['value']:.1f}%",
                                (f"Previous month: {prev:.1f}%. " if prev is not None else "") + f"{s['reading']}.")
    boj = json.loads(get_setting("boj_outlook", "null") or "null")
    if boj:
        rid = "news:BOJ"
        if states.get(rid) != boj["id"]:
            set_state[rid] = boj["id"]
            if rid in states:
                parts = ", ".join(f"fiscal {r['fy']}: {r['cpi']['median']:+.1f}%" for r in boj["rows"])
                raise_alert("JPCPI", "news", f"New Bank of Japan inflation forecast ({boj['report']})",
                            f"Median forecasts for core inflation — {parts}.")
    with get_db() as conn:
        conn.executemany("INSERT OR REPLACE INTO alert_state VALUES (?,?)", list(set_state.items()))
        now = int(time.time())
        ids = [conn.execute("INSERT INTO alerts (ts, key, kind, title, detail) VALUES (?,?,?,?,?)",
                            (now, *a)).lastrowid for a in new]
    if new:
        log.info("New alerts: %s", [a[2] for a in new])
        ok, _ = email_alerts(new)
        if ok:
            with get_db() as conn:
                conn.executemany("UPDATE alerts SET emailed=1 WHERE id=?", [(i,) for i in ids])


# ── Email ───────────────────────────────────────────────────────────────────
def send_email(subject, body, html=None, force=False):
    """Returns (ok, message) so the Alerts page can say exactly what happened."""
    if not force and get_setting("email_enabled", "0") != "1":
        return False, "Email alerts are switched off"
    user = get_setting("smtp_user", "")
    pwd = get_setting("smtp_password", "")
    to = get_setting("email_to", "") or user
    missing = [n for n, v in (("your Gmail address", user), ("the app password", pwd)) if not v]
    if missing:
        return False, "Missing " + " and ".join(missing)
    try:
        msg = EmailMessage()
        msg["Subject"], msg["From"], msg["To"] = subject, user, to
        msg.set_content(body)
        if html:
            msg.add_alternative(html, subtype="html")
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as s:
            s.starttls()
            s.login(user, pwd)
            s.send_message(msg)
        return True, f"Sent to {to}"
    except Exception as e:
        log.warning("Email failed: %s", e)
        return False, f"{type(e).__name__}: {e}"


def email_alerts(new):
    subject = f"{APP_NAME}: {new[0][2]}" + (f" (+{len(new) - 1} more)" if len(new) > 1 else "")
    link = PUBLIC_URL or f"http://127.0.0.1:{PORT}"
    body = "\n\n".join(f"• {t}\n  {d}" for _, _, t, d in new) + f"\n\nOpen {APP_NAME}: {link}\n"
    html = ("<div style='font-family:-apple-system,Segoe UI,sans-serif;font-size:15px'>" +
            "".join(f"<p><b>{escape(t)}</b><br><span style='color:#555'>{escape(d)}</span></p>"
                    for _, _, t, d in new) +
            f"<p><a href='{escape(link)}'>Open {APP_NAME}</a></p></div>")
    return send_email(subject, body, html)


# ── Login ───────────────────────────────────────────────────────────────────
def is_guest():
    return session.get("role") == "guest"


def guest_expired():
    if not GUEST_EXPIRES:
        return False
    try:
        return datetime.now().date() > datetime.strptime(GUEST_EXPIRES, "%Y-%m-%d").date()
    except ValueError:
        return True


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get("authed") or (is_guest() and guest_expired()):
            session.clear()
            if request.path.startswith("/api/"):
                return jsonify({"success": False, "error": "Not signed in"}), 401
            return redirect(url_for("login"))
        # Guests can look at everything but cannot change anything.
        if is_guest() and request.method not in ("GET", "HEAD") and request.path != "/api/track":
            return jsonify({"success": False, "guest": True, "error": "Guests can look around but not change things."}), 403
        return f(*args, **kwargs)
    return wrapped


HEAD = """<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="apple-touch-icon" href="/apple-touch-icon.png"><link rel="icon" type="image/png" href="/static/icon-512.png">
<meta name="apple-mobile-web-app-title" content="Macro Watch"><meta name="theme-color" content="#12151F">"""

LOGIN_PAGE = """<!doctype html><html><head>""" + HEAD + """<title>Macro Watch — Sign in</title>
<style>body{background:#0A0C12;color:#E4E0D8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
display:grid;place-items:center;min-height:100vh;margin:0}form{background:#12151F;border:1px solid #1E2235;border-radius:14px;
padding:28px;width:min(340px,90vw)}h1{color:#818CF8;font-size:22px;margin:0 0 16px}input{width:100%;box-sizing:border-box;
background:#0A0C12;border:1px solid #2A2F45;color:#E4E0D8;border-radius:10px;padding:12px;font-size:16px}
button{margin-top:12px;width:100%;background:#818CF8;color:#0A0C12;border:none;border-radius:10px;padding:12px;font-size:16px;font-weight:800}
.err{color:#F9A8D4;font-size:14px;margin-top:10px}</style></head><body>
<form method="post"><h1>Macro Watch</h1><input type="password" name="password" placeholder="Password" autofocus>
<button>Sign in</button>{err}</form></body></html>"""


@app.route("/login", methods=["GET", "POST"])
def login():
    err = ""
    if request.method == "POST":
        pw = request.form.get("password", "")
        role = None
        if PASSWORD and hmac.compare_digest(pw, PASSWORD):
            role = "owner"
        elif GUEST_PASSWORD and hmac.compare_digest(pw, GUEST_PASSWORD) and not guest_expired():
            role = "guest"
        if role:
            session.clear()
            session.permanent = True
            session.update(authed=True, role=role)
            record_access("login", "Signed in")
            return redirect("/")
        err = '<div class="err">That password didn\'t work.</div>'
    return LOGIN_PAGE.replace("{err}", err)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── Public bits ─────────────────────────────────────────────────────────────
@app.route("/apple-touch-icon.png")
@app.route("/apple-touch-icon-precomposed.png")
def apple_touch_icon():
    return send_from_directory(HERE / "static", "icon-180.png", mimetype="image/png", max_age=7 * 86400)


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True})


# ── API ─────────────────────────────────────────────────────────────────────
def in_short(sm, rules, hub=()):
    """The answer first: does anything need a look today?"""
    items = []
    for r in rules:
        s = sm.get(r["key"])
        if s and r["enabled"] and rule_beyond(r["op"], s["value"], r["level"]):
            items.append({"key": r["key"], "kind": "level",
                          "text": f"{s['name']} is {r['op']} your {fmt(r['key'], r['level'])} level "
                                  f"(now {fmt(r['key'], s['value'])})"})
    # Official rates arrive a business day late, so "recent" = the last 4 calendar days
    recent = (datetime.now() - timedelta(days=4)).strftime("%Y-%m-%d")
    for key, s in sm.items():
        if s["unusual"] and s["date"] >= recent:
            size = f"{s['d1']:+.1f}%" if s["chgKind"] == "pct" else f"{s['d1']:+.2f}"
            when = "today" if s["date"] == datetime.now().strftime("%Y-%m-%d") else "on " + \
                datetime.strptime(s["date"], "%Y-%m-%d").strftime("%a %d %b").replace(" 0", " ")
            items.append({"key": key, "kind": "move",
                          "text": f"{s['name']} made an unusually large move {when} ({size})"})
    for h in hub:
        if h["zone"].startswith("extreme"):
            items.append({"key": h["key"], "kind": "extreme",
                          "text": f"{h['name']} is at an extreme {h['side']} ({h['pct']:.0f}th percentile of its history)"})
    head = (f"{len(items)} thing{'s' if len(items) > 1 else ''} worth a look" if items
            else "Nothing unusual — no level crossed, no unusual moves")
    return {"headline": head, "items": items}


@app.route("/api/overview")
@login_required
def api_overview():
    sm = summaries()
    st = fetch_status()
    sources = []
    for src, name in SOURCE_NAMES.items():
        x = st.get(src, {})
        stale = not x.get("ok_ts") or time.time() - x["ok_ts"] > EVERY[src] * 3 + 600
        sources.append({"id": src, "name": name, "ok_ts": x.get("ok_ts"), "error": x.get("error") or "",
                        "stale": stale})
    groups = [{"id": g, "title": t, "intro": intro,
               "items": [sm[i["key"]] for i in S.INDICATORS if i["group"] == g and i["key"] in sm]}
              for g, t, intro in S.GROUPS]
    return jsonify({"role": session.get("role"), "groups": groups, "sources": sources,
                    "inShort": in_short(sm, get_rules(), hub_summaries()),
                    "moodParts": json.loads(get_setting("mood_parts", "{}") or "{}"),
                    "boj": json.loads(get_setting("boj_outlook", "null") or "null"),
                    "serverTime": int(time.time())})


def _code_version():
    """Changes whenever the app's code or page changes, so an open page knows to reload itself."""
    h = hashlib.sha1()
    for name in ("app.py", "sources.py", "dashboard.html"):
        try:
            h.update((HERE / name).read_bytes())
        except OSError:
            pass
    return h.hexdigest()[:12]


VERSION = _code_version()


@app.route("/api/version")
@login_required
def api_version():
    return jsonify({"version": VERSION})


@app.route("/api/series/<key>")
@login_required
def api_series(key):
    if key in S.HUB_BY_KEY:
        return jsonify({"key": key, "points": load_series("H_" + key)})
    if key not in S.BY_KEY:
        return jsonify({"error": "unknown"}), 404
    return jsonify({"key": key, "points": load_series(key)})


@app.route("/api/extremes")
@login_required
def api_extremes():
    items = hub_summaries()
    have = {h["key"] for h in items}
    errors = json.loads(get_setting("hub_errors", "{}") or "{}")
    missing = [{"key": h["key"], "name": h["name"], "cat": h["cat"],
                "error": errors.get(h["key"], "Not fetched yet")} for h in S.HUB if h["key"] not in have]
    return jsonify({"cats": [{"id": c, "title": t, "intro": i} for c, t, i in S.HUB_CATS],
                    "items": items, "missing": missing,
                    "limits": {"extreme": S.EXTREME_PCT, "stretched": S.STRETCHED_PCT},
                    "alertsOn": get_setting("extreme_alerts", "1") == "1"})


@app.route("/api/alerts")
@login_required
def api_alerts():
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT 200")]
    choices = [{"key": i["key"], "name": i["name"], "unit": i["unit"]} for i in S.INDICATORS if i["group"]]
    email = {"enabled": get_setting("email_enabled", "0") == "1", "extremeAlerts": get_setting("extreme_alerts", "1") == "1", "user": get_setting("smtp_user", ""),
             "to": get_setting("email_to", ""), "hasPassword": bool(get_setting("smtp_password", ""))}
    if is_guest():
        email.update(user="", to="", hasPassword=False)
    return jsonify({"alerts": rows, "rules": get_rules(), "choices": choices, "email": email})


@app.route("/api/rules", methods=["POST"])
@login_required
def api_rules():
    d = request.json or {}
    action = d.get("action")
    with get_db() as conn:
        if action == "delete":
            conn.execute("DELETE FROM rules WHERE id=?", (int(d["id"]),))
            conn.execute("DELETE FROM alert_state WHERE rule=?", (f"rule:{int(d['id'])}",))
        elif action == "add":
            key, op = d.get("key"), d.get("op")
            try:
                level = float(str(d.get("level")).replace(",", ""))
            except (TypeError, ValueError):
                return jsonify({"success": False, "error": "Please type a number for the level."}), 400
            if key not in S.BY_KEY or op not in ("above", "below"):
                return jsonify({"success": False, "error": "Unknown indicator."}), 400
            rid = conn.execute("INSERT INTO rules (key, op, level) VALUES (?,?,?)", (key, op, level)).lastrowid
            # Start from the current state, so a level that is already crossed doesn't email at once
            s = summaries().get(key)
            state = "beyond" if s and rule_beyond(op, s["value"], level) else "inside"
            conn.execute("INSERT OR REPLACE INTO alert_state VALUES (?,?)", (f"rule:{rid}", state))
        elif action == "toggle":
            conn.execute("UPDATE rules SET enabled=1-enabled WHERE id=?", (int(d["id"]),))
    return jsonify({"success": True})


@app.route("/api/email", methods=["POST"])
@login_required
def api_email():
    d = request.json or {}
    if "enabled" in d:
        set_setting("email_enabled", "1" if d["enabled"] else "0")
    if "extremeAlerts" in d:
        set_setting("extreme_alerts", "1" if d["extremeAlerts"] else "0")
    for field, key in (("user", "smtp_user"), ("to", "email_to")):
        if field in d:
            set_setting(key, (d[field] or "").strip())
    if d.get("password"):
        set_setting("smtp_password", d["password"].replace(" ", "").strip())
    if d.get("test"):
        ok, msg = send_email(f"{APP_NAME}: test email",
                             "This is a test. Alerts will arrive like this.", force=True)
        return jsonify({"success": ok, "message": msg})
    return jsonify({"success": True, "message": "Saved"})


@app.route("/api/refresh", methods=["POST"])
@login_required
def api_refresh():
    threading.Thread(target=refresh_cycle, kwargs={"force": True}, daemon=True).start()
    return jsonify({"success": True})


@app.route("/api/track", methods=["POST"])
@login_required
def api_track():
    """Guests' clicks, so the owner can see what testers tried."""
    if is_guest():
        record_access("event", (request.json or {}).get("what", "")[:200])
    return jsonify({"success": True})


# ── Dashboard ───────────────────────────────────────────────────────────────
@app.route("/")
@login_required
def index():
    return (HERE / "dashboard.html").read_text(encoding="utf-8").replace("{{HEAD}}", HEAD)


start_background()

if __name__ == "__main__":
    if not PASSWORD:
        log.warning("MACRO_PASSWORD is not set: nobody can sign in. Put it in .env (local) or /etc/macro.env (server).")
    app.run(host="127.0.0.1", port=PORT, debug=False, threaded=True)
