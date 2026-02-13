@echo off
chcp 65001 > nul
echo ========================================================
echo 🚀 G-ValueMap Data Update & Deploy Script
echo ========================================================
echo.

echo [1/3] Generating Seed Data (Please wait 5~10 mins)...
python generate_seed_data.py

echo.
echo [2/3] Adding files to Git...
git add .

echo.
echo [3/3] Pushing to GitHub...
git commit -m "Manual Update via Batch Script"
git push origin master

echo.
echo ========================================================
echo ✅ Update Complete! 
echo The website will reflect changes in 3-5 minutes.
echo ========================================================
pause
