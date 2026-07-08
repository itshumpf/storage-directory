@echo off
echo ============================================
echo  STORAGE DIRECTORY - DAILY SCRAPER
echo  %date% %time%
echo ============================================
echo.

cd /d "%~dp0"

python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found!
    pause
    exit /b 1
)

echo Installing/checking dependencies...
pip install requests beautifulsoup4 --quiet --disable-pip-version-check

echo.
echo Running scraper... this takes 25-30 mins. DO NOT close this window!
echo.

python daily_scraper.py

if errorlevel 1 (
    echo.
    echo ============================================
    echo  SCRAPER ABORTED - existing data is SAFE!
    echo  Check output above for details.
    echo ============================================
    pause
    exit /b 1
)

echo.
echo Pushing to GitHub...
git add enriched_locations.json
git diff --staged --quiet && (
    echo No changes - data already up to date!
) || (
    git commit -m "Daily update: %date%"
    git push
    echo Pushed! Netlify will redeploy in ~30 seconds.
)

echo.
echo ============================================
echo  ALL DONE! %time%
echo ============================================
timeout /t 10
