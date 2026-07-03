@echo off
REM Launched by run.bat in its own window. Writes cloudflared's output (including the
REM public URL) to tunnel.log, which find_tunnel_url.ps1 polls and run.bat's window prints.
cd /d "%~dp0"
cloudflared.exe tunnel --url http://localhost:5000 > tunnel.log 2>&1
