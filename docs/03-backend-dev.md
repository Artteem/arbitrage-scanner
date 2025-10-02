# Backend Development Guide

## Install Dependencies
```bash
pip install -r backend/requirements.txt
```

## Configure Environment
Copy `backend/.env.example` to `.env` (or update your existing `.env`) and ensure `ENABLED_EXCHANGES` lists every supported venue:

```bash
ENABLED_EXCHANGES=binance,bybit,mexc,bingx
```

> **Note:** BingX support is mandatory for parity with production. If you already have a `.env` file, add `bingx` to the list so the connector loads.
> The backend will temporarily enable any missing required exchanges (such as BingX) at runtime, but you should update your `.env` to keep the configuration persistent.

## Run API
```bash
uvicorn arbitrage_scanner.app:app --reload --port 8000
```

Open http://127.0.0.1:8000/docs

## Tests
```bash
pytest -q
```

## Adding a new exchange connector
1. Create a module in `arbitrage_scanner/connectors/` with the exchange name (e.g. `kraken.py`). Existing examples include Binance (`binance.py`), Bybit (`bybit.py`), and MEXC (`mexc.py`).
2. Inside the module, expose a `connector: ConnectorSpec`.
   Provide the `run` coroutine and optionally `discover_symbols` and a taker fee.
3. Add the exchange name to the `ENABLED_EXCHANGES` environment variable (comma separated).
4. Restart the backend — the exchange will be loaded automatically.
