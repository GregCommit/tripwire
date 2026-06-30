@echo off
title Tripwire v3
cd /d "C:\Users\deado\OneDrive\Desktop\TripWire\Program"
call venv\Scripts\activate.bat
echo.
echo  Tripwire v3 starting...
echo  Browser opens at http://localhost:5000
echo  DO NOT CLOSE THIS WINDOW
echo.
python app.py
echo.
echo Stopped. Press any key...
pause
