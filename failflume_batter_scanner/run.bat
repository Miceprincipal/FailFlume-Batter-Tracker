@echo off
setlocal
cd /d "%~dp0"
title FAILFLUME Batter Scanner

rem Keep a globally configured Python environment from poisoning this app.
set "PYTHONHOME="
set "PYTHONPATH="

set "PYTHON_EXE="
if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" set "PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
if not defined PYTHON_EXE if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" set "PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
if not defined PYTHON_EXE for /f "delims=" %%I in ('where python 2^>NUL') do if not defined PYTHON_EXE set "PYTHON_EXE=%%I"

if not defined PYTHON_EXE (
  echo No usable Python installation found.
  echo Expected Python 3.12/3.13 under %%LOCALAPPDATA%%\Programs\Python or on PATH.
  pause
  exit /b 1
)

echo Python: %PYTHON_EXE%
"%PYTHON_EXE%" -c "import re,argparse; print('Python stdlib OK')"
if ERRORLEVEL 1 (
  echo.
  echo This Python interpreter cannot load its own standard library.
  pause
  exit /b 1
)

echo Starting FAILFLUME...
"%PYTHON_EXE%" app.py --open --auto-backfill-days 90

if ERRORLEVEL 1 (
  echo.
  echo FAILFLUME failed to start. Copy the error above if you need me to diagnose it.
)
pause
