@echo off
title Tripwire Portfolio
cd /d "%~dp0"
if exist venv\Scripts\activate.bat call venv\Scripts\activate.bat

if "%PORTFOLIO_PASSWORD%"=="" (
  echo.
  echo  ============================================================
  echo   WARNING: PORTFOLIO_PASSWORD is not set.
  echo   This will expose the app to the internet using the DEFAULT
  echo   password "tripwire". Anyone with the tunnel URL below could
  echo   log in while it is running.
  echo.
  echo   Press Ctrl+C now to cancel, set a real password, and re-run:
  echo     set PORTFOLIO_PASSWORD=your-strong-password
  echo     run_portfolio.bat
  echo   Or press any key to continue anyway.
  echo  ============================================================
  echo.
  pause
)

echo.
echo  Tripwire Portfolio starting...
echo  Open http://localhost:5010 - or the tunnel URL below on your phone,
echo  then use the browser's "Add to Home Screen" to install it as an app.
echo  DO NOT CLOSE THIS WINDOW
echo.

if "%PORTFOLIO_NO_TUNNEL%"=="1" (
  echo  Internet access tunnel disabled via PORTFOLIO_NO_TUNNEL=1. App is local-only.
  echo.
  goto :run_app
)
if not exist cloudflared.exe (
  echo  cloudflared.exe not found - skipping tunnel, app is local-only.
  echo  See REMOTE_ACCESS.md for the one-time download step.
  echo.
  goto :run_app
)

del "portfolio_tunnel.log" >nul 2>&1
echo  Starting internet access tunnel via cloudflared...
start "Portfolio Tunnel" /min cmd /c "cloudflared.exe tunnel --url http://localhost:5010 > portfolio_tunnel.log 2>&1"
powershell -NoProfile -ExecutionPolicy Bypass -File "find_tunnel_url.ps1" -LogPath "portfolio_tunnel.log" -TimeoutSeconds 30
echo  To disable this and stay local-only, set PORTFOLIO_NO_TUNNEL=1 before running.
echo.

:run_app
python portfolio.py

echo.
taskkill /FI "WINDOWTITLE eq Portfolio Tunnel*" /T /F >nul 2>&1
echo Stopped. Press any key...
pause
