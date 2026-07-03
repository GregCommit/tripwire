@echo off
title Tripwire v6
cd /d "C:\Users\deado\OneDrive\Desktop\TripWire\Program"
call venv\Scripts\activate.bat

if "%TRIPWIRE_PASSWORD%"=="" (
  echo.
  echo  ============================================================
  echo   WARNING: TRIPWIRE_PASSWORD is not set.
  echo   This will expose the app to the internet using the DEFAULT
  echo   password "tripwire". Anyone with the tunnel URL below could
  echo   log in while it is running.
  echo.
  echo   Press Ctrl+C now to cancel, set a real password, and re-run:
  echo     set TRIPWIRE_PASSWORD=your-strong-password
  echo     run.bat
  echo   Or press any key to continue anyway.
  echo  ============================================================
  echo.
  pause
)

echo.
echo  Tripwire v6 starting...
echo  Browser opens at http://localhost:5000
echo  AI features: set ANTHROPIC_API_KEY env var to enable news synthesis + the AI Assistant tab
echo  Notifications, market-hours, and models are configured in the Settings tab
echo  DO NOT CLOSE THIS WINDOW
echo.

if "%TRIPWIRE_NO_TUNNEL%"=="1" (
  echo  Internet access tunnel disabled via TRIPWIRE_NO_TUNNEL=1. App is local-only.
  echo.
  goto :run_app
)

del "tunnel.log" >nul 2>&1
echo  Starting internet access tunnel via cloudflared...
start "Tripwire Tunnel" /min run_tunnel.bat
powershell -NoProfile -ExecutionPolicy Bypass -File "find_tunnel_url.ps1" -LogPath "tunnel.log" -TimeoutSeconds 30
echo  To disable this and stay local-only, set TRIPWIRE_NO_TUNNEL=1 before running.
echo.

:run_app
python app.py

echo.
taskkill /FI "WINDOWTITLE eq Tripwire Tunnel*" /T /F >nul 2>&1
echo Stopped. Press any key...
pause
