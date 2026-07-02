# Remote / Mobile Access via Cloudflare Tunnel

Tripwire runs on `localhost:5000` by default, which only your laptop can reach.
Cloudflare Tunnel gives you a free, temporary public HTTPS URL so you can open
the dashboard from your phone or another device anywhere.

## One-time setup

1. Download `cloudflared` for Windows:
   https://github.com/cloudflare/cloudflared/releases/latest
   (grab `cloudflared-windows-amd64.exe`, rename to `cloudflared.exe`)
2. Put it somewhere on your PATH, e.g. `C:\Windows\cloudflared.exe`, or just
   keep it in this folder and call it with the full path.

## Every time you want remote access

1. Start Tripwire normally first (`run.bat`), confirm it's at `http://localhost:5000`.
2. Open a **second** terminal window and run:
   ```
   cloudflared tunnel --url http://localhost:5000
   ```
3. Cloudflare prints a URL like `https://random-words.trycloudflare.com`.
   Open that URL on your phone (over WiFi or mobile data) — same dashboard.
4. The URL is valid as long as that terminal window stays open. Closing it
   ends the tunnel; just rerun the command for a new URL.

## Security

Since the dashboard is now reachable from the internet, it's protected by a
login screen. Set a real password before exposing it:

```
set TRIPWIRE_PASSWORD=your-strong-password
run.bat
```

(On PowerShell: `$env:TRIPWIRE_PASSWORD = "your-strong-password"`)

If `TRIPWIRE_PASSWORD` isn't set, the app falls back to the default password
`tripwire` and logs a warning — fine for local-only use, not for tunneling.

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
