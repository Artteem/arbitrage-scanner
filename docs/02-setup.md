# Environment Setup

## Prerequisites
- **Git** ≥ 2.40
- **Python** 3.11.x (Windows x64)
- **Node.js** 20 LTS (will be used later for frontend)
- **VS Code** with extensions: *Python*, *Pylance*, *Black Formatter*

## 1) Clone or Initialize Repository
```bash
# if you already created a remote (e.g., GitHub), clone it:
git clone <your-remote-url> arbitrage-scanner
cd arbitrage-scanner

# otherwise, initialize locally for now:
mkdir arbitrage-scanner && cd arbitrage-scanner
git init -b main
```

## 2) Create Python virtual environment
```bash
# Windows PowerShell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

## 3) Install dev tooling
```bash
pip install -r backend/requirements-dev.txt
pre-commit install
```

## 4) Configure VS Code
In `.vscode/settings.json`, the interpreter is set to `.venv`. Open the folder in VS Code and accept the prompt if asked.

## 5) Run the backend (dev)
See `docs/03-backend-dev.md`.
