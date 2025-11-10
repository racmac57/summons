@echo off

echo ==========================================
echo    SUMMONS MASTER ETL - HISTORICAL
echo ==========================================

REM Show current directory
echo Current directory: %cd%

REM Try multiple script names in order of preference
set SCRIPT_FOUND=0

if exist "simple_perfect_etl.py" (
    set SCRIPT_NAME=simple_perfect_etl.py
    set SCRIPT_FOUND=1
    echo Found simple_perfect_etl.py
) else if exist "perfect_100_percent_etl.py" (
    set SCRIPT_NAME=perfect_100_percent_etl.py
    set SCRIPT_FOUND=1
    echo Found perfect_100_percent_etl.py
) else if exist "summons_etl_enhanced.py" (
    set SCRIPT_NAME=summons_etl_enhanced.py
    set SCRIPT_FOUND=1
    echo Found summons_etl_enhanced.py
) else if exist "summons_etl.py" (
    set SCRIPT_NAME=summons_etl.py
    set SCRIPT_FOUND=1
    echo Found summons_etl.py
) else (
    echo ERROR: No ETL script found!
    echo Looking for: simple_perfect_etl.py, perfect_100_percent_etl.py, summons_etl_enhanced.py, or summons_etl.py
    echo.
    echo Available Python files in current directory:
    dir *.py
    echo.
    echo Please copy one of these scripts to the current directory:
    echo - C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\simple_perfect_etl.py
    echo - C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons\perfect_100_percent_etl.py
    pause
    exit
)

echo Using script: %SCRIPT_NAME%
echo.

REM Basic Python check
echo Checking Python...
python --version
if errorlevel 1 (
    echo ERROR: Python not found
    echo Please install Python from: https://www.python.org/downloads/
    pause
    exit
)

echo Installing required packages...
pip install pandas numpy openpyxl pathlib datetime logging

echo.
echo ==========================================
echo Running %SCRIPT_NAME% for HISTORICAL data processing...
echo This will process ALL files in your export folder:
echo C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\Court
echo ==========================================
echo.

python %SCRIPT_NAME%

echo.
echo ==========================================
echo ETL Process Complete!
echo.
echo Expected output files:
echo - SummonsMaster_PowerBI_Ready.xlsx (12-month rolling data)
echo - Location: C:\Users\carucci_r\OneDrive - City of Hackensack\_MONTHLY_DATA\_PROCESSED\Summons\
echo.
echo Next steps:
echo 1. Open Power BI Desktop
echo 2. Connect to the PowerBI_Ready.xlsx file
echo 3. Build your 12-month matrix visual
echo ==========================================

pause