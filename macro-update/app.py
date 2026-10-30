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
import sys
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
EVERY = {"yahoo": 15 * 60, "fred": 2 * 3600, "bis": 12 * 3600, "boj": 12 * 3600, "hub": 3 * 3600, "geo": 15 * 60, "earn": 30 * 60}
SOURCE_NAMES = {"yahoo": "Yahoo Finance", "fred": "FRED (St. Louis Fed)", "bis": "BIS (Japan CPI)",
                "boj": "Bank of Japan forecasts", "hub": "Extremes (long histories)", "geo": "Geopolitics markets", "earn": "Earnings reactions"}

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
            CREATE TABLE IF NOT EXISTS news (id INTEGER PRIMARY KEY, ts INTEGER, source TEXT, title TEXT,
                summary TEXT, link TEXT UNIQUE, topics TEXT);
            CREATE INDEX IF NOT EXISTS news_ts ON news(ts);
            CREATE TABLE IF NOT EXISTS heat (idx TEXT PRIMARY KEY, ts INTEGER, data TEXT);
            CREATE TABLE IF NOT EXISTS ai_cache (k TEXT PRIMARY KEY, ts INTEGER, model TEXT, text TEXT);
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
        elif source == "geo":
            refresh_geo()
        elif source == "earn":
            refresh_earn()
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


def refresh_geo():
    """Prices for the geopolitics tab: defense, energy, shipping, airlines and so on (Yahoo), jet fuel (FRED), plus the averages."""
    syms = S.geo_symbols()
    got = S.fetch_yahoo(syms, period="2y")
    saved = 0
    for k, v in S.GEO_ITEMS.items():
        if v.get("sym") and v["sym"] in got:
            save_series(k, got[v["sym"]])
            saved += 1
    try:
        save_series("G_JETFUEL", S.fetch_fred("DJFUELUSGULF", "2023-01-01"))
    except Exception as e:
        log.warning("Jet fuel series failed: %s", e)
    for bk, members in S.GEO_BASKETS.items():
        pts = S.build_basket([load_series(m) for m in members if load_series(m)])
        if len(pts) > 30:
            save_series(bk, pts)
    if not saved:
        raise RuntimeError("Yahoo returned no geopolitics prices")
    missing = [v["name"] for k, v in S.GEO_ITEMS.items() if v.get("sym") and v["sym"] not in got]
    set_setting("geo_missing", json.dumps(missing))


def compute_internals():
    out, parts = S.compute_internals(load_series)
    for k, pts in out.items():
        save_series(k, pts)
    set_setting("sector_parts", json.dumps(parts))


def _earnings_date(tk):
    try:
        cal = tk.calendar
        v = cal.get("Earnings Date") if isinstance(cal, dict) else None
        if v:
            d = v[0] if isinstance(v, (list, tuple)) else v
            return d.date() if hasattr(d, "date") else d
    except Exception:
        pass
    return None


def _implied_for(tk, spot, on_or_after):
    exps = [e for e in (tk.options or []) if e >= on_or_after.isoformat()]
    if not exps:
        return None
    ch = tk.option_chain(exps[0])
    cols = lambda df: [{"strike": r["strike"], "bid": r["bid"], "ask": r["ask"], "last": r["lastPrice"]} for r in df.to_dict("records")]
    return S.implied_move_pct(spot, cols(ch.calls), cols(ch.puts))


def refresh_earn():
    """After-hours / next-day reaction of the big names, set against the move the options market expected BEFORE the report.
    The expectation is captured while the report is still ahead (earnings date within two days) and kept for the comparison."""
    import yfinance as yf
    from datetime import time as dtime
    try:
        from zoneinfo import ZoneInfo
        ny = ZoneInfo("America/New_York")
    except Exception:
        ny = None
    df = yf.download(S.EARN_WATCH, period="5d", interval="5m", prepost=True, auto_adjust=False, progress=False, group_by="ticker", threads=False)
    state = json.loads(get_setting("earn_state", "{}") or "{}")
    now = int(time.time())
    today = datetime.now(ny).date() if ny else datetime.now().date()
    rows = []
    for t in S.EARN_WATCH:
        try:
            s = df[t]["Close"].dropna()
            idx = s.index.tz_convert(ny) if s.index.tz is not None else s.index.tz_localize("UTC").tz_convert(ny)
            s = s.copy()
            s.index = idx
            reg = s[(s.index.time >= dtime(9, 30)) & (s.index.time <= dtime(16, 0))]
            if reg.empty:
                continue
            reg_close, reg_t = float(reg.iloc[-1]), reg.index[-1]
            prev = reg[reg.index.date < reg_t.date()]
            after = s[s.index > reg_t]
            ah = (float(after.iloc[-1]) / reg_close - 1) * 100 if len(after) else None
            day = (reg_close / float(prev.iloc[-1]) - 1) * 100 if len(prev) else None
        except Exception as e:
            log.warning("Earnings %s: %s", t, e)
            continue
        st = state.get(t, {})
        try:
            tk = yf.Ticker(t)
            ed = _earnings_date(tk)
            if ed and today <= ed <= today + timedelta(days=2) and (st.get("exp_date") != ed.isoformat() or now - st.get("exp_ts", 0) > 6 * 3600):
                pct = _implied_for(tk, reg_close, ed)
                if pct:
                    st = state[t] = {"exp_date": ed.isoformat(), "expect": pct, "exp_ts": now}
        except Exception as e:
            log.warning("Earnings options %s: %s", t, e)
        move = ah if ah is not None else day
        exp_recent = st.get("expect") and st.get("exp_date") and abs((today - datetime.strptime(st["exp_date"], "%Y-%m-%d").date()).days) <= 3
        if move is None or not (exp_recent or abs(move) >= 5):
            continue
        ratio, words = S.surprise_ratio(move, st.get("expect") if exp_recent else None)
        rows.append({"t": t, "move": round(move, 2), "when": "after hours" if ah is not None else "today", "expected": st.get("expect") if exp_recent else None,
                     "ratio": ratio, "words": words, "date": st.get("exp_date") if exp_recent else None})
    set_setting("earn_state", json.dumps(state))
    rows.sort(key=lambda r: -(r["ratio"] or abs(r["move"]) / 5))
    set_setting("earn_rows", json.dumps({"ts": now, "rows": rows}))


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
    with get_db() as conn:
        have = {r[0] for r in conn.execute("SELECT key FROM series WHERE key GLOB 'H_*'")}
    new_items = any("H_" + h["key"] not in have for h in S.HUB)        # a new indicator was added: do not wait three hours
    for src, every in EVERY.items():
        x = st.get(src, {})
        wait = every if x.get("ok_ts") and not x.get("error") else 600
        if src == "hub" and new_items:
            wait = min(wait, 600)
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
                compute_internals()
            except Exception as e:
                log.warning("Market internals failed: %s", e)
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


BROWSER_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
NEWS_EVERY = 15 * 60
NEWS_KEEP_DAYS = 7


def ingest_feeds(feeds):
    """Fetch the feeds, tag each headline with the indicators / conflicts it is about, keep the tagged ones.
    A feed that fails is skipped and reported; the rest still count. Returns (feeds reached, errors, new headlines)."""
    ok, errors, added = 0, {}, 0
    now = int(time.time())
    for name, url in feeds:
        label = name.split(" ")[0] if name.startswith(("CNBC", "MarketWatch")) else name
        try:
            items = S.parse_feed(S.http_get(url, timeout=15, ua=BROWSER_UA), label)
            ok += 1
        except Exception as e:
            errors[f"{name} ({url[:60]})"] = f"{type(e).__name__}: {e}"[:120]
            continue
        with get_db() as conn:
            for it in items:
                topics = S.tag_news(it["title"], it["summary"])
                if not topics:
                    continue
                if conn.execute("SELECT 1 FROM news WHERE lower(title)=?", (it["title"].lower(),)).fetchone():
                    continue
                cur = conn.execute("INSERT OR IGNORE INTO news (ts, source, title, summary, link, topics) VALUES (?,?,?,?,?,?)",
                                   (it["ts"] or now, it["source"], it["title"], it["summary"], it["link"], json.dumps(topics)))
                added += cur.rowcount
        time.sleep(0.25)
    with get_db() as conn:
        conn.execute("DELETE FROM news WHERE ts < ?", (now - NEWS_KEEP_DAYS * 86400,))
    return ok, errors, added


def refresh_news():
    feeds = S.news_feeds()
    ok, errors, added = ingest_feeds(feeds)
    set_setting("news_status", json.dumps({"ok": ok, "total": len(feeds), "ts": int(time.time()), "added": added, "errors": errors}))
    log.info("News: %d of %d feeds reachable, %d new headlines", ok, len(feeds), added)


def refresh_geo_news():
    """The two conflicts get their own, faster and much wider news run (every 5 minutes)."""
    feeds = S.geo_news_feeds()
    ok, errors, added = ingest_feeds(feeds)
    set_setting("geo_news_status", json.dumps({"ok": ok, "total": len(feeds), "ts": int(time.time()), "added": added, "errors": errors}))
    log.info("Geopolitics news: %d of %d feeds reachable, %d new headlines", ok, len(feeds), added)


GEOMAP_EVERY = 1800
FRONT_URLS = ["https://deepstatemap.live/api/history/last"]


def refresh_geo_map():
    """Open map data: the Ukraine front line (DeepStateMap) and a week of geolocated incidents from GDELT, for both conflicts."""
    for cid in S.GEO:
        from urllib.parse import quote
        st = {"ts": int(time.time()), "front": None, "points": [], "errors": {}}
        prev = json.loads(get_setting("geomap_" + cid, "null") or "null")
        if prev:
            st["front"], st["points"] = prev.get("front"), prev.get("points", [])
        if cid == "ua":
            for url in FRONT_URLS:
                try:
                    st["front"] = S.parse_frontline(S.http_get(url, timeout=40, ua=BROWSER_UA).decode("utf-8", "replace"))
                    st["errors"].pop("front", None)
                    break
                except Exception as e:
                    st["errors"]["front"] = f"{type(e).__name__}: {e}"[:140]
        from urllib.parse import quote
        tried = []
        for q in S.GEO_INCIDENT_Q[cid]:
            try:
                url = "https://api.gdeltproject.org/api/v2/geo/geo?query=" + quote(q) + "&format=GeoJSON&timespan=7d&maxpoints=1000"
                pts = S.parse_gdelt_points(S.http_get(url, timeout=40, ua=BROWSER_UA).decode("utf-8", "replace"))
                tried.append(f"{len(pts)} points")
                if pts:
                    st["points"], st["pointsTs"] = pts, int(time.time())
                    st["errors"].pop("points", None)
                    break
            except Exception as e:
                tried.append(f"{type(e).__name__}: {e}"[:110])
            time.sleep(6)       # GDELT asks for one request every few seconds
        else:
            st["errors"]["points"] = "GDELT: " + " | ".join(tried)
        ships, why = [], []
        words = [w for _, w in S.PORTWATCH_STRAITS[cid]]
        where = " OR ".join(f"UPPER(portname) LIKE '%{w[:6].upper()}%'" for w in words)
        for label_try, tpl in S.PORTWATCH_TRIES:
            try:
                url = S.PORTWATCH_URL + tpl.format(n=80 * len(words), where=quote(where))
                got = S.parse_portwatch(S.http_get(url, timeout=40, ua=BROWSER_UA).decode("utf-8", "replace"))
                for label, w in S.PORTWATCH_STRAITS[cid]:
                    ser = next((v for k, v in got.items() if w in k.lower()), None)
                    sm = ser and S.ship_summary(ser, label)
                    if sm:
                        ships.append(sm)
                if ships:
                    break
                why.append(f"{label_try}: answer had {sum(len(v) for v in got.values())} rows, none usable")
            except Exception as e:
                why.append(f"{label_try}: {type(e).__name__}: {e}"[:130])
        if ships:
            st["ships"] = ships
            st["errors"].pop("ships", None)
        else:
            st["errors"]["ships"] = "IMF PortWatch: " + " | ".join(why)
            st["ships"] = (prev or {}).get("ships", [])
        set_setting("geomap_" + cid, json.dumps(st))
        log.info("War map %s: front %s, %d incident points, errors %s", cid, len(st["front"] or []), len(st["points"]), st["errors"])


def refresh_cnn_fng():
    """CNN's own Fear & Greed Index (the number everyone quotes)."""
    raw = S.http_get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata", timeout=30, ua=BROWSER_UA)
    fg = json.loads(raw)["fear_and_greed"]
    set_setting("cnn_fng", json.dumps({"score": round(float(fg["score"])), "rating": str(fg.get("rating", "")), "prev": fg.get("previous_close"),
                                       "week": fg.get("previous_1_week"), "ts": int(time.time())}))


def geo_map_loop():
    time.sleep(60)
    while True:
        try:
            refresh_cnn_fng()
        except Exception as e:
            log.warning("CNN Fear & Greed not reached: %s", e)
        try:
            refresh_geo_map()
        except Exception as e:
            log.exception("War map failed: %s", e)
        time.sleep(GEOMAP_EVERY)


def geo_news_loop():
    time.sleep(45)
    while True:
        try:
            refresh_geo_news()
        except Exception as e:
            log.exception("Geopolitics news failed: %s", e)
        time.sleep(300)


def news_loop():
    time.sleep(20)
    while True:
        try:
            refresh_news()
        except Exception as e:
            log.exception("News refresh failed: %s", e)
        time.sleep(NEWS_EVERY)


def link_hash(link):
    return hashlib.md5(link.encode("utf-8", "replace")).hexdigest()[:10]


def news_recent(hours=24):
    """Headlines of the last `hours` that are directly about an indicator (the key is in the headline
    itself): {key: [headline, ...]} newest first. Drives the red "!" and the tiles at the top."""
    cutoff = int(time.time()) - hours * 3600
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM news WHERE ts > ? ORDER BY ts DESC LIMIT 400", (cutoff,))]
    out = {}
    for r in rows:
        try:
            topics = json.loads(r["topics"])
        except ValueError:
            continue
        for k, w in topics:
            if w >= 2:
                out.setdefault(k, []).append({"h": link_hash(r["link"]), "title": r["title"], "source": r["source"],
                                              "ts": r["ts"], "link": r["link"], "summary": r["summary"]})
    return out


def news_for(primary, drivers, effects, limit=10):
    """The headlines that fit this indicator best: about itself first, then about what explains it
    and what it moves. Newer headlines win ties."""
    rel = {}
    for k, w in [(k, 3) for k in effects] + [(k, 3.5) for k in drivers] + [(primary, 5)]:
        rel[k] = max(rel.get(k, 0), w)
    cutoff = int(time.time()) - NEWS_KEEP_DAYS * 86400
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM news WHERE ts > ? ORDER BY ts DESC LIMIT 800", (cutoff,))]
    out = []
    for r in rows:
        try:
            topics = json.loads(r["topics"])
        except ValueError:
            continue
        hit = [(k, w) for k, w in topics if k in rel]
        if not hit:
            continue
        score = max(rel[k] * w for k, w in hit) + max(0.0, 1 - (time.time() - r["ts"]) / 3600 / 96)
        tags = [S.item_name(k) for k, _ in sorted(hit, key=lambda x: -rel[x[0]])
                if k in S.BY_KEY or k in S.HUB_BY_KEY or k in S.GEO_ITEMS][:3]
        keys = [k for k, _ in sorted(hit, key=lambda x: -rel[x[0]])]
        out.append({"score": score, "keys": keys, "h": link_hash(r["link"]), "title": r["title"], "summary": r["summary"], "link": r["link"], "source": r["source"],
                    "ts": r["ts"], "tags": tags})
    out.sort(key=lambda x: -x["score"])
    return out[:limit]



# ── AI summary of the situation behind a headline (Anthropic SDK) ─────────────────────────────
AI_MODELS = [("claude-opus-5-5", "Claude Opus 5.5 — best quality, about 2 cents per summary"),
             ("claude-sonnet-5-5", "Claude Sonnet 5.5 — about 1 cent per summary"),
             ("claude-haiku-5-5", "Claude Haiku 5.5 — cheapest, a fraction of a cent")]
AI_SYSTEM = ("You write short, plain-language briefings about the market situation behind a news headline, inside a personal "
             "investing dashboard. Use ONLY the headlines and numbers you are given; never add facts, figures, dates or causes "
             "that are not in them. If the headlines and the numbers do not support each other, say so plainly. "
             "Write exactly three short labelled paragraphs: 'What the news says.' 'What the numbers show.' "
             "'How solid it is.' — the last one says whether the connected indicators confirm the story or not, how unusual the "
             "moves really are, and what to watch next. At most 150 words in total. No bullet points, no investment advice, no hype.")
AI_TTL = 3 * 3600


class AiError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


def ai_key():
    return get_setting("anthropic_key", "") or os.environ.get("ANTHROPIC_API_KEY", "")


def ai_model():
    m = get_setting("ai_model", AI_MODELS[0][0])
    return m if m in dict(AI_MODELS) else AI_MODELS[0][0]


def ai_prompt(h):
    cutoff = int(time.time()) - 5 * 86400
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM news WHERE ts > ? ORDER BY ts DESC LIMIT 800", (cutoff,))]
    row = next((r for r in rows if link_hash(r["link"]) == h), None)
    if not row:
        raise AiError("gone", "That headline is no longer stored (the app keeps four days of news).")
    items = all_items()
    topics = json.loads(row["topics"])
    keys = [k for k, _ in sorted(topics, key=lambda x: -(items[x[0]]["rank"] if x[0] in items else -1)) if k in items][:3]
    keyset = set(keys)
    related = [r for r in rows if r is not row and keyset & {t for t, _ in json.loads(r["topics"])}][:5]
    lines = ["HEADLINE: " + row["title"] + (f" — {row['summary']}" if row["summary"] else "") + f" ({row['source']}, {datetime.fromtimestamp(row['ts']).strftime('%d %b %H:%M')})"]
    if related:
        lines.append("OTHER RECENT HEADLINES ON THE SAME TOPIC:")
        lines += [f"- {r['title']}" + (f" — {r['summary']}" if r["summary"] else "") + f" ({r['source']})" for r in related]
    lines.append("CURRENT NUMBERS (from the dashboard):")
    for k in keys:
        it, ex = items[k], S.explain(k, items)
        move = f"{S.fchg(it['chgKind'], it.get('d1'))} today, {S.fchg(it['chgKind'], it.get('w1'))} this week, {S.fchg(it['chgKind'], it.get('m1'))} this month"
        extra = f"; {it['pct']:.0f}th percentile of its own history" if it.get("hub") else ""
        links = "; ".join(x["note"] for x in ex["drivers"] + ex["effects"] if x["status"] != "quiet")[:700]
        lines.append(f"- {it['name']}: now {fmt_hub(it, it['value'])}; {move}{extra}. Unusual: {it.get('horizon') or 'no'}. How it connects: {links or 'no connected indicator is moving'}")
    return "\n".join(lines), [{"title": r["title"], "source": r["source"], "link": r["link"], "ts": r["ts"]} for r in [row] + related]


def ai_error(e):
    import anthropic
    if isinstance(e, anthropic.AuthenticationError):
        return AiError("bad_key", "The Anthropic API key was not accepted. Check it on the Alerts tab.")
    if isinstance(e, anthropic.PermissionDeniedError):
        return AiError("denied", "This API key may not use that model.")
    if isinstance(e, anthropic.NotFoundError):
        return AiError("model", "That model is not available to this key. Pick another model on the Alerts tab.")
    if isinstance(e, anthropic.RateLimitError):
        return AiError("rate", "Anthropic says too many requests right now. Try again in a minute.")
    if isinstance(e, anthropic.APIConnectionError):
        return AiError("net", "Could not reach Anthropic from the server.")
    if isinstance(e, anthropic.APIStatusError):
        return AiError("api", f"Anthropic returned an error ({e.status_code}): {e.message}"[:300])
    return AiError("api", f"{type(e).__name__}: {e}"[:300])


def ai_summarize(h):
    key = ai_key()
    if not key:
        raise AiError("no_key", "AI summaries need an Anthropic API key. Add it on the Alerts tab.")
    try:
        import anthropic
    except ImportError:
        raise AiError("sdk", "The Anthropic library is not installed on the server yet. The updater installs it within a minute; try again shortly.")
    prompt, sources = ai_prompt(h)
    model = ai_model()
    ck = hashlib.md5((model + prompt).encode()).hexdigest()
    with get_db() as conn:
        row = conn.execute("SELECT * FROM ai_cache WHERE k=?", (ck,)).fetchone()
    if row and time.time() - row["ts"] < AI_TTL:
        return {"text": row["text"], "model": row["model"], "cached": True, "sources": sources}
    client = anthropic.Anthropic(api_key=key, timeout=60.0, max_retries=1)
    params = dict(model=model, max_tokens=4000, system=AI_SYSTEM, messages=[{"role": "user", "content": prompt}], output_config={"effort": "low"})
    try:
        resp = client.messages.create(**params)
        if resp.stop_reason == "refusal":      # a safety classifier declined: re-run once with the server-side fallback
            resp = client.beta.messages.create(betas=["server-side-fallback-2026-06-01"], fallbacks=[{"model": "claude-opus-4-8"}], **params)
    except anthropic.APIError as e:
        raise ai_error(e)
    text = "".join(b.text for b in resp.content if b.type == "text").strip()
    if not text:
        raise AiError("empty", "The model returned no text" + (" (it declined to answer)." if resp.stop_reason == "refusal" else "."))
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO ai_cache (k, ts, model, text) VALUES (?,?,?,?)", (ck, int(time.time()), model, text))
        conn.execute("DELETE FROM ai_cache WHERE ts < ?", (int(time.time()) - 7 * 86400,))
    return {"text": text, "model": model, "cached": False, "sources": sources}


# ── Heatmap ────────────────────────────────────────────────────────────────────────────────
HEAT_EVERY = 15 * 60


def refresh_heat():
    screener, errors = None, []
    try:
        screener = S.fetch_screener()
    except Exception as e:
        errors.append(f"Nasdaq screener: {type(e).__name__}: {e}"[:160])
    for idx, (title, url) in S.HEAT_INDEXES.items():
        wiki, errs = None, list(errors)
        try:
            wiki = S.parse_wiki_constituents(S.http_get(url, timeout=30, ua=BROWSER_UA).decode("utf-8", "replace"))
        except Exception as e:
            errs.append(f"Wikipedia {title}: {type(e).__name__}: {e}"[:160])
        with get_db() as conn:
            prev = conn.execute("SELECT data FROM heat WHERE idx=?", (idx,)).fetchone()
        caps = {r["t"]: r["c"] for r in json.loads(prev["data"])["rows"]} if prev else {}
        base = [r["t"] for r in (wiki and [{"t": t} for t, *_ in wiki] or [{"t": r["t"]} for r in S.heat_static(idx)])]
        caps = {**{r["t"]: r["m"] for r in S.heat_static(idx)}, **caps}
        yahoo = None
        if not screener:
            try:
                yahoo = S.yahoo_changes(base)
            except Exception as e:
                errs.append(f"Yahoo: {type(e).__name__}: {e}"[:160])
        try:
            built = S.build_heatmap(idx, wiki, screener, yahoo, caps)
        except Exception as e:
            log.warning("Heatmap %s failed: %s", idx, e)
            continue
        built["errors"] = errs
        with get_db() as conn:
            conn.execute("INSERT OR REPLACE INTO heat (idx, ts, data) VALUES (?,?,?)", (idx, int(time.time()), json.dumps(built)))
        log.info("Heatmap %s: %d stocks (%s)", idx, len(built["rows"]), "; ".join(built["notes"]))
        if idx == "sp500":
            try:
                st = S.heat_stats(built["rows"])
                if st:
                    st["ts"] = int(time.time())
                    set_setting("heat_stats", json.dumps(st))
            except Exception as e:
                log.warning("Heat stats failed: %s", e)


def heat_loop():
    time.sleep(30)
    while True:
        try:
            refresh_heat()
        except Exception as e:
            log.exception("Heatmap refresh failed: %s", e)
        time.sleep(HEAT_EVERY)


_started = False


def start_background():
    global _started
    if not _started and os.environ.get("MACRO_NO_BACKGROUND") != "1":
        _started = True
        threading.Thread(target=background_loop, daemon=True, name="refresh").start()
        threading.Thread(target=news_loop, daemon=True, name="news").start()
        threading.Thread(target=heat_loop, daemon=True, name="heat").start()
        threading.Thread(target=geo_news_loop, daemon=True, name="geonews").start()
        threading.Thread(target=geo_map_loop, daemon=True, name="geomap").start()


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
                share = min(99, int(h["pct"]) if h["side"] == "high" else int(100 - h["pct"]))
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
<meta name="apple-mobile-web-app-title" content="Macro Watch"><meta name="theme-color" content="#000000">"""

LOGIN_PAGE = """<!doctype html><html><head>""" + HEAD + """<title>Macro Watch — Sign in</title>
<style>body{background:#000;color:#fff;font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
display:grid;place-items:center;min-height:100vh;margin:0}form{background:#08080a;border:1px solid #1b1b1f;border-radius:6px;
padding:28px;width:min(340px,90vw)}h1{color:#fff;font-size:20px;font-weight:700;letter-spacing:-.01em;margin:0 0 16px}input{width:100%;box-sizing:border-box;
background:#000;border:1px solid #2b2b32;color:#fff;border-radius:4px;padding:12px;font-size:16px}
button{margin-top:12px;width:100%;background:#2447D6;color:#fff;border:none;border-radius:4px;padding:12px;font-size:16px;font-weight:600}
.err{color:#FF4D4F;font-size:14px;margin-top:10px}</style></head><body>
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
HZ_FIELD = {"today": "d1", "this week": "w1", "this month": "m1", "this quarter": "d1", "at an extreme": "d1"}


def cluster_of(key):
    for c in S.CLUSTERS:
        if key in c["main"]:
            return c["id"]
    return None


def crossed_levels(sm, rules):
    """The levels you set that are currently crossed."""
    out = []
    for r in rules:
        s = sm.get(r["key"])
        if s and r["enabled"] and rule_beyond(r["op"], s["value"], r["level"]):
            out.append({"key": r["key"], "cluster": cluster_of(r["key"]),
                        "text": f"{s['name']} is {r['op']} your {fmt(r['key'], r['level'])} level (now {fmt(r['key'], s['value'])})"})
    return out


def solid_signal(x):
    """Worth a tile at the top: a big move, or an extreme, or a clearly smaller one that its usual partners
    confirm. Small moves, loose ends and partners that did not follow are left out, however many headlines
    there are about them."""
    if not x.get("horizon"):
        return False
    spec, boost, factor = x.get("spec", 0), x.get("linkBoost", 0), x.get("linkFactor", 1)
    if x.get("hub") and str(x.get("zone", "")).startswith("extreme"):
        return boost > 0 or spec >= 1.6
    if factor < 0.9 and spec < 2.0:
        return False
    return spec >= 1.5 or (spec >= 1.0 and boost >= 1.0)


def build_summary(clusters, levels, recent):
    """What moved most: up to three tiles, one per group, each with the move in numbers and, only if
    there is one, a real headline. Nothing is written for things that did not move."""
    movers = []
    for c in clusters:
        special = [x for x in c["main"] if solid_signal(x)]
        if not special:
            continue
        x = max(special, key=lambda i: i["rank"])
        field = HZ_FIELD.get(x["horizon"], "d1")
        news = (recent.get(x["key"]) or [None])[0]
        movers.append({"key": x["key"], "name": x["name"], "cluster": c["id"], "clusterTitle": c["title"], "rank": x["rank"],
                       "horizon": x["horizon"], "hub": bool(x.get("hub")), "value": x["value"], "unit": x["unit"], "dec": x["dec"],
                       "signed": bool(x.get("signed")), "chgKind": x["chgKind"], "chg": x.get(field), "good": x.get("good", 0),
                       "extreme": bool(x.get("hub") and x["zone"].startswith("extreme")), "news": news, "context": x.get("context", "") if x.get("contextKind") == "diverge" else ""})
    movers.sort(key=lambda m: -m["rank"])
    return {"movers": movers[:3], "calm": not movers and not levels, "levels": levels}


def apply_link_ranking(items):
    """Re-rank by importance after the links: things that move together as usual reinforce each other,
    things whose usual partner did not follow lose weight. Items are updated in place."""
    for it in items.values():
        it.setdefault("rank0", it.get("rank", 0.0))
        it["rank"] = it["rank0"]
    adj, edges = S.adjusted_scores(items)
    ctx = S.context_lines(items, edges)
    for k, v in adj.items():
        items[k]["rank"] = v["adj"]
        items[k]["linkBoost"], items[k]["linkFactor"] = v["boost"], v["factor"]
        items[k]["context"], items[k]["contextKind"] = ctx.get(k, ("", ""))
    return adj, edges


def build_clusters(sm, hub):
    """The groups that belong together, most important first. Within a group the most special
    (and most important for stocks) comes first; 'context' members are shown alongside."""
    allm = {**sm, **{h["key"]: h for h in hub}}
    out = []
    for c in S.CLUSTERS:
        main = sorted((allm[k] for k in c["main"] if k in allm), key=lambda x: -x["rank"])
        ctx = [allm[k] for k in c["context"] if k in allm]
        if not main:
            continue
        out.append({"id": c["id"], "title": c["title"], "intro": c["intro"], "rank": max(x["rank"] for x in main),
                    "drivers": S.drivers(c["id"], {x["key"]: x for x in main + ctx}, set(c["main"])), "main": main, "context": []})
    out.sort(key=lambda c: -c["rank"])
    return out


def composite_parts():
    """What the calculated indicators are made of, with each member's move today."""
    def d1(k):
        pts = load_series(k)
        return (pts[-1][1] / pts[-2][1] - 1) * 100 if len(pts) >= 2 and pts[-2][1] else None
    def members(keys, names=None):
        out = [{"name": (names or {}).get(k, S.SECTOR_NAMES.get(k, k)), "pct": round(v, 2)} for k in keys for v in [d1(k)] if v is not None]
        return sorted(out, key=lambda x: -x["pct"])
    parts = {}
    sec = members([k for k, _ in S._SECTORS])
    if sec:
        parts["SECT_UP"] = {"note": "The 11 sectors of the S&P 500 (each as a fund): today's move.", "members": sec, "up": sum(1 for x in sec if x["pct"] > 0)}
    for key, keys, note in [("AIB", S.AI_BUILDERS, "The AI builders, counted equally: the chip, cloud and software giants that spend on AI."),
                            ("AIU", ["XLF", "XLI"], "The companies that use AI: banks & financials and industrials."),
                            ("DEFN", ["XLV", "XLP", "XLU", "XLRE"], "The defensive sectors: health care, consumer staples, utilities, real estate. They hold up when investors are afraid."),
                            ("BREADTH", ["RSP", "SPY"], "Equal-weight S&P 500 (every company the same) against the normal one (the biggest count most). If the equal-weight one lags, the index rests on a few giants.")]:
        m = members(keys, {"RSP": "S&P 500 equal weight", "SPY": "S&P 500 by size"})
        if m:
            parts[key] = {"note": note, "members": m, "up": sum(1 for x in m if x["pct"] > 0)}
    if "AIB" in parts and "AIU" in parts:
        a, u = [sum(x["pct"] for x in parts[k]["members"]) / len(parts[k]["members"]) for k in ("AIB", "AIU")]
        parts["AIB_AIU"] = {"note": "AI builders against AI users. Down means money moves out of the builders into the companies that use AI.",
                            "members": [{"name": "AI builders (average)", "pct": round(a, 2)}, {"name": "AI users (average)", "pct": round(u, 2)}]}
    return parts


def tomorrow_panel(sm):
    chg = {}
    for k in ("FUT_ES", "FUT_NQ", "FUT_RTY"):
        pts = load_series(k)
        if len(pts) >= 2 and pts[-2][1]:
            chg[k] = (pts[-1][1] / pts[-2][1] - 1) * 100
    vix = sm.get("VIX")
    vix = (vix["value"], vix["d1"]) if vix and vix.get("d1") is not None else None
    ts, up = load_series("VIXTS"), load_series("SECT_UP")
    heat = json.loads(get_setting("heat_stats", "null") or "null")
    earn = json.loads(get_setting("earn_rows", "null") or "null")
    if not (chg or vix or heat):
        return None
    v3 = load_series("VIX3M")
    fng = json.loads(get_setting("cnn_fng", "null") or "null")
    if not fng and sm.get("MOOD"):
        fng = {"score": sm["MOOD"]["value"], "rating": "", "estimate": True}
    out = S.build_tomorrow(chg, vix, ts[-1][1] if ts else None, up[-1][1] if up else None, heat, earn["rows"] if earn else None,
                           v3[-1][1] if v3 else None, fng)
    out["heat"] = heat and {k: heat[k] for k in ("worst", "best", "cap", "eq", "n", "ts")}
    return out


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
    hub = hub_summaries()
    apply_link_ranking({**sm, **{h["key"]: h for h in hub}})
    clusters = build_clusters(sm, hub)
    recent = news_recent(24)
    return jsonify({"role": session.get("role"), "groups": groups, "sources": sources,
                    "clusters": clusters,
                    "sectors": sorted(({"k": k, "name": S.SECTOR_NAMES.get(k, k), "pct": v, "ai": k in S.AI_BUILDER_SECTORS}
                                       for k, v in json.loads(get_setting("sector_parts", "{}") or "{}").items()), key=lambda x: -x["pct"]),
                    "earnings": json.loads(get_setting("earn_rows", "null") or "null"),
                    "tomorrow": tomorrow_panel(sm),
                    "parts": composite_parts(),
                    "summary": build_summary(clusters, crossed_levels(sm, get_rules()), recent),
                    "newsIdx": {k: [n["h"] for n in v] for k, v in recent.items()},
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


STARTED = int(time.time())


def deployed_commit():
    try:
        return Path("/var/lib/macro-autoupdate/sha").read_text().strip()[:7]
    except OSError:
        return ""


@app.route("/api/version")
@login_required
def api_version():
    return jsonify({"version": VERSION, "commit": deployed_commit(), "started": STARTED})


def all_items():
    items = {**summaries(), **{h["key"]: h for h in hub_summaries()}, **S.geo_summaries(load_series)}
    apply_link_ranking(items)
    return items


def node_why(k, items, edges, recent):
    it = items[k]
    names = lambda ks: ", ".join(items[x]["name"] for x in ks[:2])
    parts = []
    hz = it.get("horizon")
    if hz == "at an extreme" and it.get("hub"):
        parts.append(f"sits at the {it['side']} extreme of its own history ({it['pct']:.0f}th percentile)")
    elif hz:
        zk = {"today": "z1", "this week": "zw", "this month": "zm"}.get(hz, "z1")
        field = HZ_FIELD.get(hz, "d1")
        z = abs(it.get(zk) or 0)
        parts.append(f"moved {S.fchg(it['chgKind'], it.get(field))} {hz}" + (f", about {z:.1f}× a typical move" if z else ""))
    else:
        parts.append("no unusual move; shown because it is connected to indicators that are moving")
    boost = [e["affected"] if e["driver"] == k else e["driver"] for e in edges if k in (e["driver"], e["affected"]) and e.get("reinforce")]
    weak = [e["affected"] if e["driver"] == k else e["driver"] for e in edges if k in (e["driver"], e["affected"]) and e["status"] in ("noreact", "against")]
    if boost:
        parts.append(f"{names(boost)} moved the usual way with it")
    if weak:
        parts.append(f"{names(weak)} did not follow")
    n = len(recent.get(k, []))
    parts.append(f"{n} headline{'s' if n != 1 else ''} about it in the last 3 days" if n else "no headline about it right now")
    return "; ".join(parts) + "."


def build_graph(items, edges, n, clusters=None, news_on=True, linked=False, news_only=False):
    """The overview map: the most important indicators (after the link adjustment), the links between
    them and the headlines about them. Everything else is left out on purpose."""
    recent = news_recent(72)
    pool = list(items)
    if clusters:
        allowed = {k for c in S.CLUSTERS if c["id"] in clusters for k in c["main"]}
        pool = [k for k in pool if k in allowed]
    if news_only:
        pool = [k for k in pool if recent.get(k)]
    ranked = sorted(pool, key=lambda k: -items[k]["rank"])
    sel = [k for k in ranked if items[k]["rank"] >= 0.5][:n]
    if len(sel) < min(6, len(ranked)):
        sel = ranked[:min(6, len(ranked))]
    if linked:
        linked_keys = {e["driver"] for e in edges if e["driver"] in sel and e["affected"] in sel} | {e["affected"] for e in edges if e["driver"] in sel and e["affected"] in sel}
        sel = [k for k in sel if k in linked_keys]
    keyset = set(sel)
    nodes = []
    for k in sel:
        it = items[k]
        field = HZ_FIELD.get(it.get("horizon") or "today", "d1")
        nodes.append({"key": k, "name": it["name"], "value": it["value"], "unit": it["unit"], "dec": it["dec"], "signed": bool(it.get("signed")),
                      "chgKind": it["chgKind"], "chg": it.get(field), "horizon": it.get("horizon"), "good": it.get("good", 0),
                      "hub": bool(it.get("hub")), "zone": it.get("zone"), "tone": it.get("tone"), "rank": it["rank"],
                      "boost": it.get("linkBoost", 0), "factor": it.get("linkFactor", 1), "rank0": it.get("rank0", it["rank"]), "cluster": cluster_of(k),
                      "why": node_why(k, items, edges, recent), "nNews": len(recent.get(k, []))})
    es = [{"a": e["driver"], "b": e["affected"], "mode": e["mode"], "status": e["status"], "reinforce": e.get("reinforce", False),
           "note": e["note"]} for e in edges if e["driver"] in keyset and e["affected"] in keyset]
    notes = []
    for e in es:
        da, ab = items[e["a"]]["name"], items[e["b"]]["name"]
        if e["reinforce"]:
            notes.append(f"{da} and {ab} usually move {'together' if e['mode'] == 'same' else 'in opposite directions'} and both are far out.")
        elif e["status"] in ("noreact", "against"):
            notes.append(e["note"])
    cutoff = int(time.time()) - 72 * 3600
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM news WHERE ts > ? ORDER BY ts DESC LIMIT 600", (cutoff,))] if news_on else []
    news = []
    for r in rows:
        try:
            topics = json.loads(r["topics"])
        except ValueError:
            continue
        hit = [(k, w) for k, w in topics if k in keyset]
        if not hit:
            continue
        score = sum(items[k]["rank"] * (1.0 if w >= 2 else 0.4) for k, w in hit) * (0.5 + max(0.0, 1 - (time.time() - r["ts"]) / 3600 / 72))
        news.append({"score": score, "h": link_hash(r["link"]), "title": r["title"], "summary": r["summary"], "link": r["link"],
                     "source": r["source"], "ts": r["ts"], "keys": [k for k, _ in sorted(hit, key=lambda x: -items[x[0]]["rank"])][:2]})
    news.sort(key=lambda x: -x["score"])
    return {"nodes": nodes, "edges": es, "news": news[:6], "notes": notes[:6], "total": len(items), "shown": len(nodes)}



@app.route("/api/summary", methods=["POST"])
@login_required
def api_summary():
    h = str((request.json or {}).get("h", ""))[:20]
    try:
        return jsonify({"success": True, **ai_summarize(h)})
    except AiError as e:
        return jsonify({"success": False, "error": e.code, "message": e.message}), 400


@app.route("/api/ai", methods=["GET", "POST"])
@login_required
def api_ai():
    if request.method == "POST":
        d = request.json or {}
        if d.get("key"):
            set_setting("anthropic_key", str(d["key"]).strip())
        if d.get("clearKey"):
            set_setting("anthropic_key", "")
        if d.get("model") in dict(AI_MODELS):
            set_setting("ai_model", d["model"])
        if d.get("test"):
            try:
                import anthropic
                anthropic.Anthropic(api_key=ai_key(), timeout=30.0, max_retries=0).messages.create(
                    model=ai_model(), max_tokens=200, messages=[{"role": "user", "content": "Reply with the single word OK."}], output_config={"effort": "low"})
                return jsonify({"success": True, "message": "The key works."})
            except ImportError:
                return jsonify({"success": False, "message": "The Anthropic library is not installed on the server yet; it is installed within a minute of the update."})
            except Exception as e:
                return jsonify({"success": False, "message": ai_error(e).message if "anthropic" in sys.modules else str(e)})
    try:
        import anthropic  # noqa: F401
        sdk = True
    except ImportError:
        sdk = False
    return jsonify({"hasKey": bool(ai_key()), "model": ai_model(), "models": [{"id": i, "label": l} for i, l in AI_MODELS], "sdk": sdk})


@app.route("/api/heatmap")
@login_required
def api_heatmap():
    idx = request.args.get("index", "sp500")
    if idx not in S.HEAT_INDEXES:
        return jsonify({"error": "unknown index"}), 404
    with get_db() as conn:
        row = conn.execute("SELECT * FROM heat WHERE idx=?", (idx,)).fetchone()
    if not row:
        return jsonify({"index": idx, "title": S.HEAT_INDEXES[idx][0], "rows": [], "notes": [], "errors": [], "ts": None})
    d = json.loads(row["data"])
    return jsonify({"index": idx, "title": S.HEAT_INDEXES[idx][0], "ts": row["ts"], **d})


@app.route("/api/graph")
@login_required
def api_graph():
    items = all_items()
    _, edges = S.adjusted_scores(items)
    n = max(4, min(24, int(request.args.get("n", 14))))
    clusters = [c for c in request.args.get("clusters", "").split(",") if c]
    return jsonify(build_graph(items, edges, n, clusters or None, request.args.get("news", "1") != "0",
                               request.args.get("linked") == "1", request.args.get("newsonly") == "1"))



def build_geo_graph(cid, items, edges):
    """The conflict as a map: the conflict itself in the middle, the markets it moves around it, the links between
    those markets, and the headlines about it."""
    G = S.GEO[cid]
    keys = []
    for g in G["groups"]:
        if g.get("basket"):
            keys.append(g["basket"][0])
        else:
            keys += [S.geo_key(s) for s, _, _ in g["members"]][:2]
        keys += [k for k, _, _ in g.get("extra", [])] + list(g.get("refs", []))
    keys = [k for k in dict.fromkeys(keys) if k in items]
    keyset = set(keys)
    recent = news_recent(72)
    nodes = [{"key": G["topic"], "name": G["title"], "special": True, "value": 0, "unit": "", "dec": 0, "chgKind": "pct", "chg": None, "horizon": None, "good": 0,
              "hub": False, "zone": None, "tone": None, "rank": 99, "boost": 0, "factor": 1, "rank0": 99, "cluster": None,
              "why": "The conflict itself. The boxes around it are the markets it moves.", "nNews": len(recent.get(G["topic"], []))}]
    for k in keys:
        it = items[k]
        field = HZ_FIELD.get(it.get("horizon") or "today", "d1")
        nodes.append({"key": k, "name": it["name"], "value": it["value"], "unit": it["unit"], "dec": it["dec"], "signed": bool(it.get("signed")), "chgKind": it["chgKind"],
                      "chg": it.get(field), "horizon": it.get("horizon"), "good": it.get("good", 0), "hub": False, "zone": None, "tone": it.get("tone"),
                      "rank": max(it["rank"], 1.0), "boost": it.get("linkBoost", 0), "factor": it.get("linkFactor", 1), "rank0": it.get("rank0", it["rank"]),
                      "cluster": None, "why": node_why(k, items, edges, recent), "nNews": len(recent.get(k, []))})
    es = [{"a": e["driver"], "b": e["affected"], "mode": e["mode"], "status": e["status"], "reinforce": e.get("reinforce", False), "note": e["note"]}
          for e in edges if e["driver"] in keyset and e["affected"] in keyset]
    linked = {x for e in es for x in (e["a"], e["b"])}
    for k in keys:                                                       # the conflict touches every market in the map that has no other link yet
        if k not in linked:
            es.append({"a": G["topic"], "b": k, "mode": "same", "status": "quiet", "reinforce": False, "note": ""})
    es += [{"a": G["topic"], "b": k, "mode": "same", "status": "quiet", "reinforce": False, "note": ""} for k in keys if k in linked and S.GEO_ITEMS.get(k, {}).get("basket")]
    cutoff = int(time.time()) - 72 * 3600
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM news WHERE ts > ? ORDER BY ts DESC LIMIT 800", (cutoff,))]
    news = []
    for r in rows:
        try:
            topics = json.loads(r["topics"])
        except ValueError:
            continue
        if G["topic"] not in {t for t, _ in topics}:
            continue
        hit = [k for k, w in topics if k in keyset and w >= 2][:1]
        news.append({"score": r["ts"], "h": link_hash(r["link"]), "title": r["title"], "summary": r["summary"], "link": r["link"], "source": r["source"],
                     "ts": r["ts"], "keys": [G["topic"]] + hit})
    news.sort(key=lambda x: -x["score"])
    notes = []
    return {"nodes": nodes, "edges": es, "news": news[:6], "notes": notes, "total": len(keys), "shown": len(keys), "allNews": news[:14]}


def geo_headlines(cid, items, limit=60):
    """Every headline that touches this conflict, ranked by importance × urgency. A company that moved a lot today lifts its headlines."""
    G, keys = S.GEO[cid], S.geo_topic_keys(cid)
    cutoff = int(time.time()) - 5 * 86400
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM news WHERE ts > ? ORDER BY ts DESC LIMIT 2500", (cutoff,))]
    now, out = time.time(), []
    for r in rows:
        try:
            topics = json.loads(r["topics"])
        except ValueError:
            continue
        hit = [k for k, w in topics if k in keys]
        if not hit:
            continue
        age = max(0.0, (now - r["ts"]) / 3600)
        impact = S.geo_impact(r["title"], r["summary"])
        moved = [k for k in hit if k in items and items[k].get("spec", 0) >= 1.0 and k != G["topic"]]
        markets = [S.item_name(k) for k in hit if k != G["topic"] and (k in S.GEO_ITEMS or k in S.BY_KEY)][:3]
        out.append({"h": link_hash(r["link"]), "title": r["title"], "summary": r["summary"], "link": r["link"], "source": r["source"], "ts": r["ts"],
                    "category": S.geo_category(r["title"], r["summary"]), "impact": impact, "urgency": S.geo_urgency(age, impact),
                    "markets": markets, "moved": [S.item_name(k) for k in moved][:2], "rank": S.geo_rank(impact, age, 0.35 * min(2, len(moved)))})
    out.sort(key=lambda x: -x["rank"])
    return out[:limit]


@app.route("/api/geomap")
@login_required
def api_geomap():
    cid = request.args.get("c", "ua")
    if cid not in S.GEO:
        return jsonify({"error": "unknown"}), 404
    st = json.loads(get_setting("geomap_" + cid, "null") or "null") or {"front": None, "points": [], "errors": {}, "ts": None}
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT ts, source, title, summary, link FROM news WHERE ts > ? ORDER BY ts DESC LIMIT 2500", (int(time.time()) - 7 * 86400,))]
    return jsonify({"view": S.GEO_VIEW[cid], "sites": S.site_status(cid, rows), "front": st.get("front"), "points": st.get("points", []),
                    "errors": st.get("errors", {}), "ships": st.get("ships", []), "incidents": S.news_incidents(cid, rows), "ts": st.get("ts"), "pointsTs": st.get("pointsTs")})


@app.route("/api/geo")
@login_required
def api_geo():
    cid = request.args.get("c", "ua")
    if cid not in S.GEO:
        return jsonify({"error": "unknown"}), 404
    G = S.GEO[cid]
    items = all_items()
    _, edges = S.adjusted_scores(items)
    groups = []
    for g in G["groups"]:
        ks = ([g["basket"][0]] if g.get("basket") else []) + [S.geo_key(s) for s, _, _ in g["members"]] + [k for k, _, _ in g.get("extra", [])] + list(g.get("refs", []))
        groups.append({"id": g["id"], "title": g["title"], "says": g["says"], "items": [items[k] for k in dict.fromkeys(ks) if k in items]})
    graph = build_geo_graph(cid, items, edges)
    heads = geo_headlines(cid, items)
    members = [it for k, it in items.items() if S.GEO_ITEMS.get(k, {}).get("conflict") == cid and not S.GEO_ITEMS[k].get("basket") and it.get("d1") is not None]
    movers = [{"key": it["key"], "name": it["name"], "d1": it["d1"]} for it in sorted(members, key=lambda x: -abs(x["d1"]))[:6]]
    now = time.time()
    rec = news_recent(7 * 24).get(G["topic"], [])
    pulse = {"day": sum(1 for h in heads if now - h["ts"] < 86400), "week": len(heads)}
    return jsonify({"id": cid, "title": G["title"], "blurb": G["blurb"], "groups": groups, "graph": graph, "pulse": pulse, "headlines": heads, "movers": movers,
                    "headStatus": json.loads(get_setting("geo_news_status", "null") or "null"),
                    "missing": json.loads(get_setting("geo_missing", "[]") or "[]"),
                    "conflicts": [{"id": k, "title": v["title"]} for k, v in S.GEO.items()]})


@app.route("/api/explain/<key>")
@login_required
def api_explain(key):
    items = all_items()
    ex = S.explain(key, items)
    if ex is None:
        return jsonify({"error": "unknown"}), 404
    drivers, effects = [x["driver"] for x in ex["drivers"]], [x["affected"] for x in ex["effects"]]
    return jsonify({**ex, "items": {k: items[k] for k in dict.fromkeys([key] + drivers + effects) if k in items},
                    "news": news_for(key, drivers, effects),
                    "newsStatus": json.loads(get_setting("news_status", "null") or "null")})


@app.route("/api/series/<key>")
@login_required
def api_series(key):
    if key in S.HUB_BY_KEY:
        return jsonify({"key": key, "points": load_series("H_" + key)})
    if key in S.GEO_ITEMS:
        return jsonify({"key": key, "points": load_series(key)})
    if key not in S.BY_KEY:
        return jsonify({"error": "unknown"}), 404
    return jsonify({"key": key, "points": load_series(key)})


@app.route("/api/extremes")
@login_required
def api_extremes():
    everything = all_items()
    items = [everything[h["key"]] for h in S.HUB if h["key"] in everything and everything[h["key"]].get("hub")]
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
