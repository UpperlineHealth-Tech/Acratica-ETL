@echo off

cd /d "%~dp0"

if "%1" == "" (
    set env=uv
) else if "%1" == "conda" (
    set env=conda
) else if "%1" == "uv" (
    set env=uv
) else (
    echo Invalid parameter: %1
    echo Please specify either 'conda' or 'uv'. If not specified, uv is defaulted.
    exit /b
)

if not exist ".venv" (
    if %env% == uv (
        call uv venv --python 3.11
        echo .venv created in "%cd%".
        REM Use the batch activation script when running this .bat file
        call .\.venv\Scripts\activate.bat

        REM Ensure pip is bootstrapped into the venv (uses bundled wheel)
        call .\.venv\Scripts\python.exe -m ensurepip --upgrade

        REM Upgrade pip, setuptools and wheel from PyPI (requires network)
        call .\.venv\Scripts\python.exe -m pip install --upgrade pip setuptools wheel

        REM Install project requirements using the venv's pip
        call .\.venv\Scripts\python.exe -m pip install -r requirements.txt
        echo .venv setup complete.
    )

    if %env% == conda (
        call conda activate local_databricks_15.4
        call conda deactivate
        call .\.venv\Scripts\activate.bat

        REM Ensure pip is available in the venv and upgrade packaging tools
        call .\.venv\Scripts\python.exe -m ensurepip --upgrade
        call .\.venv\Scripts\python.exe -m pip install --upgrade pip setuptools wheel

        call .\.venv\Scripts\python.exe -m pip install -r requirements.txt
        call deactivate
    )
) else (
    echo .venv already exists in project directory - "%cd%".  Please delete your .venv folder and try again.
    exit /b
)