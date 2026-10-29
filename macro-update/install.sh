#!/bin/bash
# One-time setup of the Macro Watch live updater. Safe to run again.
set -e
ROOT="${ROOT:-}"                                   # only changed in tests
RAW="${RAW:-https://raw.githubusercontent.com/GregCommit/tripwire/main-ls87mz/macro-update}"
BIN="$ROOT/usr/local/bin/macro-autoupdate.sh"
UNIT="$ROOT/etc/systemd/system/macro-autoupdate.service"
ENVF="$ROOT/etc/macro.env"
mkdir -p "$(dirname "$BIN")" "$(dirname "$UNIT")"

echo "1/5 downloading the updater"
curl -fsSL "$RAW/autoupdate.sh" -o "$BIN.new" && bash -n "$BIN.new" && chmod +x "$BIN.new" && mv -f "$BIN.new" "$BIN"

echo "2/5 switching off the old 10-minute timer"
systemctl disable --now macro-autoupdate.timer 2>/dev/null || true

echo "3/5 installing the 15-second service"
cat > "$UNIT" <<'UNITEOF'
[Unit]
Description=Macro Watch live updater (checks GitHub every 15 seconds)
After=network-online.target
[Service]
ExecStart=/bin/bash -c "while true; do /usr/local/bin/macro-autoupdate.sh; sleep 15; done"
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
UNITEOF

echo "4/5 fixing the login-session key (so you stay signed in when the app restarts)"
if ! grep -q '^MACRO_SECRET_KEY=.\+' "$ENVF" 2>/dev/null; then
  sed -i '/^MACRO_SECRET_KEY=/d' "$ENVF" 2>/dev/null || true
  echo "MACRO_SECRET_KEY=$(openssl rand -hex 32)" >> "$ENVF"
fi

echo "5/5 starting"
systemctl daemon-reload
systemctl enable --now macro-autoupdate.service
echo "waiting for the first check..."; sleep 40
journalctl -u macro-autoupdate.service -n 5 --no-pager || true
echo "DONE - from now on changes go live by themselves."
