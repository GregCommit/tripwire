#!/bin/bash
# Macro Watch auto-update. Runs every 15 seconds (systemd service loop).
# 1. asks GitHub for the newest commit id (tiny request, never stale; "304 Not Modified" answers
#    are free and do not count against GitHub's rate limit)
# 2. if it is new, downloads the app files from exactly that commit
# 3. installs them only if they changed, pass a syntax check, and the app is healthy afterwards;
#    otherwise puts the previous version back. A commit that failed is not retried (no restart loop).
# The script also updates itself from the same folder.
REPO="${REPO:-GregCommit/tripwire}"
BRANCH="${BRANCH:-main-ls87mz}"
DIR="${DIR:-macro-update}"
API="${API:-https://api.github.com/repos/$REPO/commits/$BRANCH}"
RAW="${RAW:-https://raw.githubusercontent.com/$REPO}"
APP="${APP:-/opt/macro}"
BK="${BK:-/home/ubuntu/macro-backup-auto}"
STATE="${STATE:-/var/lib/macro-autoupdate}"
PY="${PY:-/opt/macro/venv/bin/python}"
HEALTH="${HEALTH:-http://127.0.0.1:8062/healthz}"
SELF="${SELF:-/usr/local/bin/macro-autoupdate.sh}"
SETTLE="${SETTLE:-30}"        # at most this many seconds to wait for the app to answer after a restart
FILES="app.py sources.py dashboard.html"

mkdir -p "$STATE"
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
ETAG=$(cat "$STATE/etag" 2>/dev/null)
CODE=$(curl -sS --max-time 15 -D "$TMP/hdr" -o "$TMP/body" -w '%{http_code}' -H "Accept: application/vnd.github.sha" \
       ${ETAG:+-H "If-None-Match: $ETAG"} "$API" 2>/dev/null)
[ "$CODE" = "304" ] && exit 0                                     # nothing new on GitHub
SHA=$(cat "$TMP/body" 2>/dev/null)
if [ "$CODE" = "200" ] && [[ "$SHA" =~ ^[0-9a-f]{40}$ ]]; then
  NEWETAG=$(grep -i '^etag:' "$TMP/hdr" | head -1 | cut -d' ' -f2- | tr -d '\r\n')
  if [ "$SHA" = "$(cat "$STATE/sha" 2>/dev/null)" ]; then [ -n "$NEWETAG" ] && echo "$NEWETAG" > "$STATE/etag"; exit 0; fi
  REF="$SHA"
else
  SHA=""; NEWETAG=""; REF="$BRANCH"                               # API unreachable or rate-limited: use the branch (can be a few minutes stale)
  [ "$CODE" = "000" ] || [ "$CODE" = "403" ] || [ "$CODE" = "429" ] || exit 0
fi
BASE="$RAW/$REF/$DIR"
remember() { [ -n "$SHA" ] && echo "$SHA" > "$STATE/sha"; [ -n "$NEWETAG" ] && echo "$NEWETAG" > "$STATE/etag"; return 0; }

for f in $FILES; do
  curl -fsS --max-time 60 -o "$TMP/$f" "$BASE/$f" || { echo "download of $f failed - nothing changed"; exit 0; }
  [ "$(stat -c %s "$TMP/$f")" -gt 5000 ] || { echo "$f looks too small - nothing changed"; exit 0; }
done
# the updater itself (optional; replaced atomically at the very end)
curl -fsS --max-time 30 -o "$TMP/self.sh" "$BASE/autoupdate.sh" 2>/dev/null && bash -n "$TMP/self.sh" 2>/dev/null || rm -f "$TMP/self.sh"
update_self() { [ -s "$TMP/self.sh" ] && ! cmp -s "$TMP/self.sh" "$SELF" && install -m 755 "$TMP/self.sh" "$SELF.new" && mv -f "$SELF.new" "$SELF" && echo "updater itself updated"; }

changed=0
for f in $FILES; do cmp -s "$TMP/$f" "$APP/$f" || changed=1; done
if [ $changed = 0 ]; then remember; update_self; exit 0; fi
"$PY" -c "import ast,sys; [ast.parse(open(f).read()) for f in sys.argv[1:]]" "$TMP/app.py" "$TMP/sources.py" \
  || { echo "syntax check failed for ${SHA:-$BRANCH} - nothing changed"; remember; update_self; exit 1; }
mkdir -p "$BK"; for f in $FILES; do cp "$APP/$f" "$BK/"; done
for f in $FILES; do cp "$TMP/$f" "$APP/$f"; chown ubuntu:ubuntu "$APP/$f" 2>/dev/null; done
systemctl restart macro.service
healthy=0
for _ in $(seq 1 "$SETTLE"); do
  curl -fsS --max-time 3 "$HEALTH" >/dev/null 2>&1 && { healthy=1; break; }
  sleep 1
done
if [ $healthy = 1 ]; then
  echo "updated to ${SHA:-$BRANCH} and healthy"; remember; update_self; exit 0
else
  echo "app not healthy after update ${SHA:-$BRANCH} - rolling back"
  for f in $FILES; do cp "$BK/$f" "$APP/$f"; done
  systemctl restart macro.service
  remember; update_self; exit 1
fi
