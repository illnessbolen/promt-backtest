@echo off
rem One-click launcher of the @bosona strategy inside updown, paper mode (no real orders), Windows:
rem   start-paper.bat          -> menu
rem   start-paper.bat paper    -> paper trading with tick recording right away
rem   start-paper.bat where    -> show which updown folder is used
rem   start-paper.bat <args>   -> any "python -m bosona" command, e.g. updown-grid data\paper\ticks
rem First run creates .venv and installs dependencies. The updown folder is taken from UPDOWN_PATH,
rem from .updown_path (remembered), or looked up near this folder and in Downloads; otherwise you are
rem asked for it (drag the folder into the window).
setlocal
cd /d "%~dp0"
chcp 65001 >nul

set "PY="
where py >nul 2>nul
if errorlevel 1 goto try_python
py -3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
if errorlevel 1 goto try_python
set "PY=py -3"
goto have_py
:try_python
where python >nul 2>nul
if errorlevel 1 goto no_py
python -c "import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
if errorlevel 1 goto no_py
set "PY=python"
:have_py

set "UD="
if defined UPDOWN_PATH call :probe "%UPDOWN_PATH%"
if defined UD goto have_updown
if not exist ".updown_path" goto search_updown
set "SAVED="
set /p SAVED=<".updown_path"
if defined SAVED call :probe "%SAVED%"
if defined UD goto have_updown
:search_updown
for /d %%D in (".\updown*" "..\updown*" "..\..\updown*" "..\illnessbolen\updown" "%USERPROFILE%\Downloads\updown*") do call :probe "%%~fD"
if defined UD goto save_updown
:ask_updown
echo.
echo The updown folder was not found. Drag the updown folder (the one with bot.py and a folder
echo named latarb inside) into this window and press Enter.
set "ANSWER="
set /p "ANSWER=updown folder: "
if not defined ANSWER goto no_updown
set "ANSWER=%ANSWER:"=%"
:trim_answer
if not defined ANSWER goto no_updown
if not "%ANSWER:~-1%"==" " goto trimmed
set "ANSWER=%ANSWER:~0,-1%"
goto trim_answer
:trimmed
call :probe "%ANSWER%"
if defined UD goto save_updown
echo There is no latarb folder in "%ANSWER%" - this is not the updown folder. Try again.
goto ask_updown
:save_updown
> ".updown_path" echo %UD%
:have_updown
set "UPDOWN_PATH=%UD%"
echo [setup] updown: %UPDOWN_PATH%
if /i "%~1"=="where" goto done

if exist ".venv\Scripts\python.exe" goto have_venv
echo [setup] creating virtual environment .venv
%PY% -m venv .venv
if errorlevel 1 goto fail
:have_venv
set "VPY=.venv\Scripts\python.exe"

if exist ".venv\.installed" goto have_deps
echo [setup] installing dependencies (first run only, a few minutes)
"%VPY%" -m pip install --upgrade pip >nul
"%VPY%" -m pip install -e ".[updown]"
if errorlevel 1 goto fail
type nul > ".venv\.installed"
:have_deps

if "%~1"=="" goto menu
if /i "%~1"=="check" goto run_check
if /i "%~1"=="paper" goto run_paper
if /i "%~1"=="grid" goto run_grid
if /i "%~1"=="test" goto run_tests
"%VPY%" -m bosona %*
goto done

:menu
echo.
echo   @bosona strategy inside updown, paper mode (no real orders) - what to run?
echo     1) check   5-minute paper run without recording, to see that everything works
echo     2) paper   paper trading with tick recording; runs until you close this window
echo     3) grid    compare rule settings on the recorded ticks (after a few days of recording)
echo     4) test    run the tests
echo     0) exit
echo   Pause quoting: create a file named STOP in this folder (type nul ^> STOP); delete it to resume.
echo.
set "choice="
set /p "choice=Choice: "
if "%choice%"=="1" goto run_check
if "%choice%"=="2" goto run_paper
if "%choice%"=="3" goto run_grid
if "%choice%"=="4" goto run_tests
exit /b 0

:run_check
"%VPY%" -m bosona updown-paper --profile conservative --duration 300
goto done
:run_paper
"%VPY%" -m bosona updown-paper --profile conservative --record
goto done
:run_grid
"%VPY%" -m bosona updown-grid data\paper\ticks --profile conservative
goto done
:run_tests
"%VPY%" -m pip install -q -e ".[dev,updown]"
"%VPY%" -m pytest -q
goto done

:done
set "RC=%errorlevel%"
echo.
pause
exit /b %RC%

rem the folder %1, or an updown* folder right inside it (Windows "Extract all" nests the ZIP folder)
:probe
if defined UD exit /b 0
if exist "%~1\latarb\__init__.py" (
  set "UD=%~f1"
  exit /b 0
)
for /d %%E in ("%~1\updown*") do if not defined UD if exist "%%~fE\latarb\__init__.py" set "UD=%%~fE"
exit /b 0

:no_py
echo Python 3.11 or newer is required (python --version). Install it from python.org,
echo tick "Add python.exe to PATH", then run start-paper.bat again.
pause
exit /b 1

:no_updown
echo No updown folder given. Download updown (github.com/illnessbolen/updown), unpack it and run
echo start-paper.bat again.
pause
exit /b 1

:fail
echo Setup failed - see the messages above.
pause
exit /b 1
