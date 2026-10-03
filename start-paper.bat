@echo off
rem One-click launcher of the @bosona strategy inside updown, paper mode (no real orders), Windows:
rem   start-paper.bat          -> menu
rem   start-paper.bat paper    -> paper trading with tick recording right away
rem   start-paper.bat <args>   -> any "python -m bosona" command, e.g. updown-grid data\paper\ticks
rem First run creates .venv and installs dependencies. The updown folder is looked up next to this one
rem (updown, updown-*, illnessbolen\updown) or taken from the UPDOWN_PATH variable.
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

if defined UPDOWN_PATH if exist "%UPDOWN_PATH%\latarb\__init__.py" goto have_updown
set "UPDOWN_PATH="
for /d %%D in ("..\updown" "..\updown-*" "..\illnessbolen\updown") do (
  if not defined UPDOWN_PATH if exist "%%~fD\latarb\__init__.py" set "UPDOWN_PATH=%%~fD"
)
if not defined UPDOWN_PATH goto no_updown
:have_updown
echo [setup] updown: %UPDOWN_PATH%

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

:no_py
echo Python 3.11 or newer is required (python --version). Install it from python.org,
echo tick "Add python.exe to PATH", then run start-paper.bat again.
pause
exit /b 1

:no_updown
echo The updown folder was not found. Download updown (github.com/illnessbolen/updown) and put it
echo next to this folder, e.g. ..\updown, or set UPDOWN_PATH to its path.
pause
exit /b 1

:fail
echo Setup failed - see the messages above.
pause
exit /b 1
