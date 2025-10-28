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

## Adding a new exchange connector
1. Create a module in `arbitrage_scanner/connectors/` with the exchange name (e.g. `kraken.py`). Existing examples include Binance (`binance.py`), Bybit (`bybit.py`), and MEXC (`mexc.py`).
2. Inside the module, expose a `connector: ConnectorSpec`.
   Provide the `run` coroutine and optionally `discover_symbols` and a taker fee.
3. Add the exchange name to the `ENABLED_EXCHANGES` environment variable (comma separated)
   if you need to load *additional* connectors.  By default the scanner enables
   all bundled exchanges (Binance, Bybit, MEXC, BingX, Gate).  Prefix an entry
   with `-` to exclude it, e.g. `-gate` removes Gate from the default set.
4. Restart the backend — the exchange will be loaded automatically.
