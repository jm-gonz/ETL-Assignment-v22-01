cd ..
python -m venv .venv
call .venv\scripts\activate.bat
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
pause
