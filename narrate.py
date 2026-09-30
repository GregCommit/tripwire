"""Record the Guide tab as audio for its "Listen" button, read by an open-source neural voice
(Kokoro, voice "Emma"). The Guide text is taken from GUIDE_SECTIONS in app.py, rewritten for
speaking (tickers, symbols, abbreviations) and recorded as one MP3 with chapter start times,
so the player can jump to a section and follow along.

app.py only imports the light parts (plan / status); kokoro_onnx is imported when recording.
On startup the app re-records by itself when the Guide text no longer matches the recording.

  python narrate.py            record if the Guide changed since the last recording
  python narrate.py --force    record even if it is up to date
  python narrate.py --text     print the script that would be spoken, record nothing
"""
import hashlib
import html
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT_DIR = HERE / "narration"
MODEL = HERE / "tts_models" / "kokoro-v1.0.onnx"
VOICES = HERE / "tts_models" / "voices-v1.0.bin"
VOICE, VOICE_NAME, LANG, SPEED = "bf_emma", "Emma", "en-gb", 1.0
SCRIPT_VERSION = 1          # bump when the text rewriting changes, to force a re-record
PAUSE = {"title": 0.8, "h4": 0.7, "p": 0.55, "li": 0.45}

# Things a voice reads badly, in the order they are applied.
SAY = [
    (r"S&P\s*500", "S and P 500"),
    (r"\bNVDA\b", "Nvidia"), (r"\bAAPL\b", "Apple"), (r"\bMU\b", "Micron"),
    (r"\bRSI\b", "R S I"), (r"\bUS\b", "U.S."),
    (r"\bvs\.?(?=\s)", "versus"), (r"\be\.g\.", "for example"), (r"\bi\.e\.", "that is"),
    (r"(\d)\s*–\s*(\d)", r"\1 to \2"),
    (r"~\s*", "roughly "), (r"±\s*", "plus or minus "), (r"(\d)\s*×", r"\1 times"),
    (r"\s*—\s*", ", "),
    (r"Support / resistance", "Support and resistance"),
    (r"\s/\s", " or "),
]
SYMBOLS = re.compile("[\U0001F000-\U0001FFFF☀-➿️]")
# The Guide's figures, said in words where they carry content (GUIDE_FIG_NOISE is only a picture).
FIGURES = {
    "GUIDE_FIG_FLOW": "Step by step: Tripwire watches your stocks every minute in market hours, checks seven "
                      "rules for anything unusual for that stock, and lets the four proven rules vote. When two "
                      "or more agree, you get a strong signal on the Stocks page and by email. You decide, and "
                      "five trading days later a follow-up tells you how it turned out.",
    "GUIDE_FIG_VOTE": "For example: on Nvidia today, two of the four voting rules, the unusual move and R S I "
                      "oversold, both point to a sharp drop. Two of four agreeing makes a strong dip. With only "
                      "one, it would be a dip forming, not yet a signal.",
    "GUIDE_FIG_EXCESS": "For example, if in the 5 trading days after a signal the stock gains 3% and your stocks "
                        "gain 1% on average, the signal beat your stocks by 2 points, or 1.6 after the 0.4% costs.",
}


def _source():
    return (HERE / "app.py").read_text(encoding="utf-8")


def guide_sections(src=None):
    """[(id, title, short_html, long_html)] from the GUIDE_SECTIONS array in app.py."""
    src = src or _source()
    start = src.index("const GUIDE_SECTIONS=[")
    block = src[start:src.index("\n];", start)]
    rx = re.compile(r"\{id:'(\w+)',\s*icon:'[^']*',\s*title:'([^']*)',\s*short:`(.*?)`,\s*long:`(.*?)`\}", re.S)
    return rx.findall(block)


def _clean(fragment):
    text = re.sub(r"<[^>]+>", "", fragment)
    text = html.unescape(SYMBOLS.sub("", text))
    for pat, rep in SAY:
        text = re.sub(pat, rep, text)
    text = re.sub(r"\b[A-Z]{3,}\b", lambda m: m.group(0).lower(), text)   # STRONG DIP -> strong dip
    return re.sub(r"\s+", " ", text).strip()


def _blocks(fragment):
    """Headings, paragraphs, list items and figures as (sentence, pause after) in reading order.
    A heading with nothing spoken under it is dropped."""
    items = []
    for m in re.finditer(r"<(h4|p|li)\b[^>]*>(.*?)</\1>|\$\{(\w+)\}", fragment, re.S):
        tag, text = (m.group(1), _clean(m.group(2))) if m.group(1) else ("p", _clean(FIGURES.get(m.group(3), "")))
        if text:
            items.append((tag, text if text[-1] in ".!?:" else text + "."))
    out = []
    for i, (tag, text) in enumerate(items):
        if tag == "h4" and (i + 1 == len(items) or items[i + 1][0] == "h4"):
            continue
        out.append((text, PAUSE[tag]))
    return out


def plan(src=None):
    """The chapters to record: each section's summary, then its details."""
    chapters = []
    for i, (sid, title, short, long) in enumerate(guide_sections(src)):
        spoken_title = title.replace(" — ", ": ")
        topic = title.split(" — ")[-1]
        intro = [("Welcome to the Tripwire guide.", 0.5)] if i == 0 else []
        chapters.append({"id": sid, "kind": "short", "title": title,
                         "blocks": intro + [(spoken_title + ".", PAUSE["title"])] + _blocks(short)})
        chapters.append({"id": sid, "kind": "long", "title": title,
                         "blocks": [(topic[0].upper() + topic[1:] + ", in more detail.", PAUSE["title"])]
                                   + _blocks(long)})
    return chapters


def plan_sha(chapters):
    payload = json.dumps([SCRIPT_VERSION, VOICE, SPEED, [c["blocks"] for c in chapters]], ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def read_manifest():
    try:
        return json.loads((OUT_DIR / "manifest.json").read_text(encoding="utf-8"))
    except Exception:
        return None


def status(src=None, sha=None):
    """Is there a recording that matches the Guide as it is now?"""
    sha = sha or plan_sha(plan(src))
    man = read_manifest()
    ok = bool(man and man.get("sha") == sha and (OUT_DIR / man.get("file", "")).exists())
    return {"available": ok, "sha": sha, "manifest": man if ok else None}


def recording_in_progress():
    lock = OUT_DIR / ".recording"
    return lock.exists() and time.time() - lock.stat().st_mtime < 3 * 3600


def can_record():
    try:
        import importlib.util
        return importlib.util.find_spec("kokoro_onnx") is not None and MODEL.exists() and VOICES.exists()
    except Exception:
        return False


def record(force=False):
    chapters = plan()
    sha = plan_sha(chapters)
    if not force and status(sha=sha)["available"]:
        print("Narration is up to date.")
        return 0
    OUT_DIR.mkdir(exist_ok=True)
    if recording_in_progress():
        print("Another recording is in progress.")
        return 1
    lock = OUT_DIR / ".recording"
    lock.write_text(str(os.getpid()))
    try:
        import numpy as np
        import soundfile as sf
        from kokoro_onnx import Kokoro
        tts = Kokoro(str(MODEL), str(VOICES))
        parts, marks, pos, rate = [], [], 0, 24000
        t0 = time.time()
        for ch in chapters:
            marks.append({"id": ch["id"], "kind": ch["kind"], "title": ch["title"], "start": round(pos / rate, 2)})
            for text, pause in ch["blocks"]:
                samples, rate = tts.create(text, voice=VOICE, speed=SPEED, lang=LANG)
                gap = np.zeros(int(pause * rate), dtype=np.float32)
                parts += [np.asarray(samples, dtype=np.float32), gap]
                pos += len(samples) + len(gap)
            print(f"  {ch['id']}/{ch['kind']}: done at {pos / rate / 60:.1f} min of audio "
                  f"({time.time() - t0:.0f}s so far)", flush=True)
        # A new name for every recording, so phones never replay a cached older one.
        name = f"guide-{sha[:10]}-{int(time.time())}.mp3"
        tmp = OUT_DIR / (name + ".part")
        # Constant bitrate keeps seeking exact, so chapter jumps land on the right word.
        sf.write(str(tmp), np.concatenate(parts), rate, format="MP3", bitrate_mode="CONSTANT",
                 compression_level=0.5)
        tmp.replace(OUT_DIR / name)
        manifest = {"sha": sha, "file": name, "duration": round(pos / rate, 1), "voice": VOICE_NAME,
                    "generated": datetime.now().strftime("%Y-%m-%d %H:%M"), "chapters": marks}
        (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
        for old in OUT_DIR.glob("guide-*.mp3"):
            if old.name != name:
                old.unlink()
        print(f"Recorded {name}: {pos / rate / 60:.1f} min in {(time.time() - t0) / 60:.1f} min.")
        return 0
    finally:
        lock.unlink(missing_ok=True)


if __name__ == "__main__":
    if "--text" in sys.argv:
        for ch in plan():
            print(f"\n=== {ch['id']} / {ch['kind']} ===")
            for text, pause in ch["blocks"]:
                print(f"[{pause}] {text}")
    else:
        sys.exit(record(force="--force" in sys.argv))
