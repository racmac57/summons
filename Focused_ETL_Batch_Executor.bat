@echo off
REM 🕒 2025-07-09-21-00-00
REM Project: SummonsMaster/Focused_ETL_Batch_Executor.bat
REM Author: R. A. Carucci
REM Purpose: Execute focused current month ETL with proper error handling

echo ==========================================
echo    HACKENSACK PD - FOCUSED SUMMONS ETL
echo ==========================================
echo Processing CURRENT MONTH data only...
echo.

REM Set script name for focused ETL
set SCRIPT_NAME=focused_summons_etl_current_month.py

REM Show current directory and timestamp
echo Current directory: %cd%
echo Start time: %date% %time%
echo.

REM Check if the focused ETL script exists
if exist "%SCRIPT_NAME%" (
    echo ✅ Found %SCRIPT_NAME%
) else (
    echo ❌ ERROR: %SCRIPT_NAME% not found in current directory
    echo.
    echo Please ensure both files are in the same folder:
    echo   - Focused_ETL_Batch_Executor.bat
    echo   - focused_summons_etl_current_month.py
    echo.
    echo Current directory contents:
    dir *.py
    echo.
    pause
    exit /b 1
)

REM Check Python installation
echo 🐍 Checking Python installation...
python --version
if errorlevel 1 (
    echo ❌ ERROR: Python not found in PATH
    echo.
    echo Please install Python or add it to your PATH
    echo Download from: https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
) else (
    echo ✅ Python found
)
echo.

REM Check required packages and install if missing
echo 📦 Checking required packages...
echo Installing/updating required packages...
pip install pandas numpy openpyxl pathlib logging --quiet
if errorlevel 1 (
    echo ⚠️  Warning: Some packages may not have installed correctly
    echo Continuing anyway...
) else (
    echo ✅ All packages ready
)
echo.

REM Check if input directories exist
echo 📁 Checking input directories...
set COURT_DIR=C:\Users\carucci_r\OneDrive - City of Hackensack\_MONTHLY_DATA\_EXPORTS\Summons\Court
set ASSIGN_FILE=C:\Users\carucci_r\OneDrive - City of Hackensack\_Hackensack_Data_Repository\ASSIGNED_SHIFT\Assignment_Master_V2.xlsx

if exist "%COURT_DIR%" (
    echo ✅ Court exports directory found
    echo    Checking for Excel files...
    dir "%COURT_DIR%\*.xlsx" /b 2>nul
    if errorlevel 1 (
        echo ⚠️  No .xlsx files found in court directory
    ) else (
        echo ✅ Excel files found
    )
) else (
    echo ❌ WARNING: Court exports directory not found:
    echo    %COURT_DIR%
)

if exist "%ASSIGN_FILE%" (
    echo ✅ Assignment Master V2 file found
) else (
    echo ❌ WARNING: Assignment Master V2 not found:
    echo    %ASSIGN_FILE%
)
echo.

REM Create output directory if it doesn't exist
set OUTPUT_DIR=C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons
if not exist "%OUTPUT_DIR%" (
    echo 📁 Creating output directory...
    mkdir "%OUTPUT_DIR%" 2>nul
    if exist "%OUTPUT_DIR%" (
        echo ✅ Output directory created
    ) else (
        echo ❌ Could not create output directory
    )
) else (
    echo ✅ Output directory exists
)
echo.

REM Run the focused ETL script
echo 🚀 Starting Focused Summons ETL...
echo ==========================================
echo.
python "%SCRIPT_NAME%"
set ETL_RESULT=%errorlevel%
echo.
echo ==========================================

REM Check results
if %ETL_RESULT%==0 (
    echo ✅ ETL COMPLETED SUCCESSFULLY!
    echo.
    echo 📊 Expected output files:
    echo   - summons_powerbi_current_month.xlsx
    echo   - officer_summary_%date:~10,4%_%date:~4,2%.xlsx
    echo   - complete_current_month_analysis.xlsx
    echo.
    echo 📍 Output location:
    echo   %OUTPUT_DIR%
    echo.
    echo 🎯 Next steps:
    echo   1. Open Power BI Desktop
    echo   2. Connect to summons_powerbi_current_month.xlsx
    echo   3. Use the updated DAX measures
    echo   4. Build your dashboard!
) else (
    echo ❌ ETL FAILED (Error code: %ETL_RESULT%)
    echo.
    echo 🔍 Check the log file for details:
    echo   summons_etl_focused_%date:~10,4%%date:~4,2%%date:~7,2%.log
    echo.
    echo 💡 Common issues:
    echo   - File paths incorrect
    echo   - Court export file missing
    echo   - Assignment Master not accessible
    echo   - Python package missing
)

echo.
echo End time: %date% %time%
echo ==========================================
pause