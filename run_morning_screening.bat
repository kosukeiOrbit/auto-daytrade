@echo off
REM 朝スクリーニング実行バッチファイル
REM タスクスケジューラから実行される

cd /d %~dp0
call venv\Scripts\activate.bat
python morning_screening.py
if %ERRORLEVEL% NEQ 0 (
    echo Error occurred: %ERRORLEVEL% >> logs\scheduler_error.log
)
deactivate
