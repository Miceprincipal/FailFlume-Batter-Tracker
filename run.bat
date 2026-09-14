@echo off
setlocal
cd /d "%~dp0"

if not exist "failflume_batter_scanner\run.bat" (
  echo FAILFLUME launcher could not find failflume_batter_scanner\run.bat
  echo Make sure the full repository/ZIP was extracted before running.
  pause
  exit /b 1
)

call "failflume_batter_scanner\run.bat"
exit /b %ERRORLEVEL%
