# Backend Development Guide

## Install Dependencies
```bash
pip install -r backend/requirements.txt
```

## Run API
```bash
uvicorn arbitrage_scanner.app:app --reload --port 8000
```

Open http://127.0.0.1:8000/docs

## Tests
```bash
pytest -q
```
