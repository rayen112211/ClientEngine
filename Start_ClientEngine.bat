@echo off
title ClientEngine v4 - Server
echo Starting ClientEngine v4...
echo.
echo Please leave this window open while using the app!
echo.
timeout /t 2 /nobreak > nul

:: Start the python server
start "ClientEngine Server" cmd /c "python run.py & pause"

:: Wait 3 seconds for the server to spin up
timeout /t 3 /nobreak > nul

:: Open the browser to the local app
start http://localhost:5000

exit
