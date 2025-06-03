@echo off
REM Benchmark execution script for Imprint Java implementation (Windows)
REM This script runs all benchmark suites and saves results with timestamps

setlocal enabledelayedexpansion

for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "TIMESTAMP=%dt:~0,4%-%dt:~4,2%-%dt:~6,2%-%dt:~8,2%%dt:~10,2%%dt:~12,2%"
set "RESULTS_DIR=.\benchmark-results"
set "SYSTEM_INFO_FILE=%RESULTS_DIR%\system-info-%TIMESTAMP%.txt"

echo 🏃 Running Imprint Java Benchmarks - %TIMESTAMP%
echo ================================================

REM Ensure results directory exists
if not exist "%RESULTS_DIR%" mkdir "%RESULTS_DIR%"

REM Capture system information
echo 📊 Capturing system information...
(
    echo Benchmark Run: %TIMESTAMP%
    echo ==============================
    echo.
    echo System Information:
    echo - Date: %date% %time%
    echo - OS: %OS%
    echo - Processor: %PROCESSOR_IDENTIFIER%
    echo - Java Version:
    java -version 2^>^&1
    echo.
    echo - Gradle Version:
    call gradlew --version
    echo.
    echo - Git Commit:
    for /f %%i in ('git rev-parse HEAD') do echo   - Hash: %%i
    for /f %%i in ('git rev-parse --abbrev-ref HEAD') do echo   - Branch: %%i
    for /f "tokens=*" %%i in ('git log -1 --format^=%%cd') do echo   - Date: %%i
    for /f "tokens=*" %%i in ('git log -1 --format^=%%s') do echo   - Message: %%i
    echo.
) > "%SYSTEM_INFO_FILE%"

echo ✅ System info saved to: %SYSTEM_INFO_FILE%

echo 🚀 Starting benchmark execution...
echo.

REM Function to run complete benchmark suite
echo 🔄 Running complete benchmark suite...
set "complete_output=%RESULTS_DIR%\complete-benchmarks-%TIMESTAMP%.json"
call gradlew jmh -Pjmh.resultsFile="%complete_output%" --console=plain

if exist "%complete_output%" (
    echo ✅ Complete benchmark suite completed: %complete_output%
    
    REM Generate overall summary
    set "overall_summary=%RESULTS_DIR%\overall-summary-%TIMESTAMP%.txt"
    (
        echo Complete Imprint Java Benchmark Results - %TIMESTAMP%
        echo ==================================================
        echo.
        echo Benchmark execution completed successfully.
        echo Results saved to: %complete_output%
        echo.
        echo To analyze results:
        echo - Use JMH Visualizer: https://jmh.morethan.io/
        echo - Import JSON into analysis tools
        echo - Use jq for command-line analysis
        echo.
    ) > "!overall_summary!"
    
    echo 📊 Overall summary: !overall_summary!
) else (
    echo ❌ Complete benchmark suite failed
)

echo.
echo 🎉 Benchmark execution completed!
echo 📁 All results saved in: %RESULTS_DIR%
echo 📄 Files created:
dir "%RESULTS_DIR%\*%TIMESTAMP%*" 2>nul

echo.
echo 💡 To view results:
echo    - JSON files can be analyzed with jq or imported into analysis tools
echo    - Visit https://jmh.morethan.io/ to visualize results
echo    - Summary files provide human-readable overviews
echo    - System info file contains environment details for reproducibility

pause