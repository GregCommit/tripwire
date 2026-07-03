# Remote / Mobile Access via Cloudflare Tunnel

Tripwire runs on `localhost:5000` by default, which only your laptop can reach.
Cloudflare Tunnel gives you a free, temporary public HTTPS URL so you can open
the dashboard from your phone or another device anywhere.

**As of v6, `run.bat` starts this tunnel automatically** — you don't need a
second terminal or a manual command. The details below explain what's
happening and how to opt out.

## One-time setup

1. Download `cloudflared` for Windows:
   https://github.com/cloudflare/cloudflared/releases/latest
   (grab `cloudflared-windows-amd64.exe`, rename to `cloudflared.exe`)
2. Keep it in this folder (`run.bat` looks for it right next to itself).

## Every time you want remote access

Just run `run.bat`. It will:

1. Warn you (with a pause) if `TRIPWIRE_PASSWORD` isn't set — see **Security** below.
2. Launch `cloudflared` in its own minimized window (titled "Tripwire Tunnel"),
   logging to `tunnel.log`.
3. Print the public URL directly in the main window once it's up (usually a
   few seconds), e.g. `https://random-words.trycloudflare.com`.
4. Start the Tripwire server itself in the foreground, as before.

Open the printed URL on your phone (over WiFi or mobile data) — same dashboard.
The tunnel stays alive as long as the "Tripwire Tunnel" window is open; closing
`run.bat`'s main window closes the tunnel window too (and vice versa if you
close the tunnel window manually, the app keeps running locally).

**To disable automatic tunneling** (local-only, e.g. if you're on a network
where you don't want any internet exposure), set `TRIPWIRE_NO_TUNNEL=1` before
running:

```
set TRIPWIRE_NO_TUNNEL=1
run.bat
```

**Manual fallback** — if you ever want to start a tunnel by hand instead
(e.g. from a different machine, or to point at an already-running instance):

```
cloudflared tunnel --url http://localhost:5000
```

## Security

Since the dashboard is now reachable from the internet, it's protected by a
login screen. Set a real password before exposing it:

```
set TRIPWIRE_PASSWORD=your-strong-password
run.bat
```

(On PowerShell: `$env:TRIPWIRE_PASSWORD = "your-strong-password"`)

If `TRIPWIRE_PASSWORD` isn't set, the app falls back to the default password
`tripwire` — **and because `run.bat` now tunnels automatically by default,
this means anyone with the tunnel URL could log in.** `run.bat` will pause
with a loud warning in this case; press Ctrl+C to cancel and set a real
password, or any other key to continue anyway (e.g. for a quick local-only
test where you're using `TRIPWIRE_NO_TUNNEL=1`).

## AI features & notifications

Set `ANTHROPIC_API_KEY` in the environment before starting Tripwire to enable:

- **News synthesis** — a short AI explanation attached to each new alert.
- **AI Assistant tab** — a chat assistant that can answer questions, research
  moves (web search), and change your watchlist, rules, and settings on request.

```
set ANTHROPIC_API_KEY=sk-ant-...
set TRIPWIRE_PASSWORD=your-strong-password
run.bat
```

Everything else — check cadence, market-hours gating, email/WhatsApp alerts, and
which AI models are used — is configured in the in-app **Settings** tab, so no
extra environment variables are needed.

**WhatsApp** uses the free [CallMeBot](https://www.callmebot.com/blog/free-api-whatsapp-messages/)
API: message their bot once to receive a personal API key, then enter your phone
number and that key in Settings. **Email** uses standard SMTP (for Gmail, create
an App Password and use it as the SMTP password).
