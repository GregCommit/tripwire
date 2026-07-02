@echo off
title Tripwire v3
cd /d "C:\Users\deado\OneDrive\Desktop\TripWire\Program"
call venv\Scripts\activate.bat
echo.
echo  Tripwire v4 starting...
echo  Browser opens at http://localhost:5000
echo  Login password: set via TRIPWIRE_PASSWORD env var (default: tripwire)
echo  AI features: set ANTHROPIC_API_KEY env var to enable news synthesis + the AI Assistant tab
echo  Notifications, market-hours, and models are configured in the Settings tab
echo  DO NOT CLOSE THIS WINDOW
echo.
echo  For remote/mobile access over the internet, open a SEPARATE terminal and run:
echo    cloudflared tunnel --url http://localhost:5000
echo  See REMOTE_ACCESS.md for setup instructions.
echo.
python app.py
echo.
echo Stopped. Press any key...
pause
