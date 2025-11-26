from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Sequence, Set, Tuple, List

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from ..domain import Symbol, Ticker
from ..store import TickerStore

logger = logging.getLogger(__name__)

# Websocket endpoint for Gate USDT-margined perpetual futures.
WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
HEARTBEAT_INTERVAL = 20.0  # seconds


def _symbol_to_contract(symbol: Symbol) -> str:
    """
    Convert internal symbol representation (e.g. BTCUSDT) to Gate contract form (e.g. BTC_USDT).
    """
    code = str(symbol).upper()
    if code.endswith("USDT") and "_" not in code:
        base = code[:-4]
        return f"{base}_USDT"
    return code


def _contract_to_symbol(contract: str) -> Symbol:
    """
    Convert Gate contract (e.g. BTC_USDT) back to internal symbol (e.g. BTCUSDT).
    """
    return Symbol(contract.replace("_", ""))


def _safe_float(value) -> float | None:
    """
    Try to convert a value to float. Returns None for invalid values.
    """
    try:
        if value is None:
            return None
        f = float(value)
        if f != f or f == float("inf") or f == float("-inf"):
            return None
        return f
    except Exception:
        return None


async def _send_heartbeat(ws) -> None:
    """
    Periodically send a ping via the Gate-specific futures.ping channel to keep the
    websocket connection alive. Gate's websocket ping/pong messages are not sufficient
    on their own to maintain the session.
    """
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            msg = {
                "time": int(time.time()),
                "channel": "futures.ping",
            }
            await ws.send(json.dumps(msg))
        except Exception:
            # Propagate the exception so the caller can reconnect
            logger.warning("Gate WS heartbeat failed, closing connection")
            raise


async def _consume_messages(
    store: TickerStore, ws, contracts: Set[str]
) -> None:
    """
    Consume order book messages from the websocket and update the ticker and order book
    in the shared TickerStore. Only messages for contracts in the provided set are processed.
    """
    while True:
        raw = await ws.recv()
        try:
            data = json.loads(raw)
        except Exception:
            logger.debug("Gate WS: failed to decode JSON: %s", raw)
            continue

        channel = data.get("channel")
        event = data.get("event")
        if channel != "futures.order_book":
            # Ignore all other channels (e.g. subscription acks or ticker updates).
            continue
        if event == "subscribe":
            # Subscription acknowledgement; nothing further to do here.
            logger.info(
                "Gate WS: subscribed to futures.order_book: %s", data.get("result")
            )
            continue
        if event != "update":
            # Only handle update events.
            continue

        result = data.get("result")
        if not result:
            continue

        # Gate may send a dict or a list of dicts for result; normalise to a list.
        items: List[dict] = []
        if isinstance(result, dict):
            items = [result]
        elif isinstance(result, list):
            items = [x for x in result if isinstance(x, dict)]
        else:
            continue

        ts_raw = data.get("time") or data.get("time_ms") or 0
        # Convert ms to seconds if necessary.
        if isinstance(ts_raw, (int, float)) and ts_raw > 1_000_000_000_000:
            ts = ts_raw / 1000.0
        else:
            ts = float(ts_raw) if ts_raw else time.time()

        for item in items:
            contract = str(item.get("s") or item.get("contract") or "")
            if not contract or contract not in contracts:
                continue
            symbol = _contract_to_symbol(contract)

            # Extract bids and asks; Gate may use "bids"/"asks" or "b"/"a".
            raw_bids = item.get("bids") or item.get("b") or []
            raw_asks = item.get("asks") or item.get("a") or []
            bids: List[Tuple[float, float]] = []
            asks: List[Tuple[float, float]] = []

            # Parse bids: entries are [price, size]; best bid has highest price.
            for entry in raw_bids:
                if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                    continue
                price = _safe_float(entry[0])
                size = _safe_float(entry[1])
                if price is None or size is None:
                    continue
                if price <= 0 or size <= 0:
                    continue
                bids.append((price, size))
            if bids:
                bids.sort(key=lambda x: -x[0])

            # Parse asks: entries are [price, size]; best ask has lowest price.
            for entry in raw_asks:
                if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                    continue
                price = _safe_float(entry[0])
                size = _safe_float(entry[1])
                if price is None or size is None:
                    continue
                if price <= 0 or size <= 0:
                    continue
                asks.append((price, size))
            if asks:
                asks.sort(key=lambda x: x[0])

            # Require both sides to update the ticker; otherwise skip.
            if not bids or not asks:
                continue

            best_bid = bids[0][0]
            best_ask = asks[0][0]

            # Update the top-of-book ticker.
            try:
                ticker = Ticker(
                    exchange="gate", symbol=symbol, bid=best_bid, ask=best_ask, ts=ts
                )
                store.upsert_ticker(ticker)
            except Exception:
                logger.exception("Gate WS: failed to upsert ticker for %s", symbol)

            # Update the order book (limit to 5 levels on each side).
            try:
                store.upsert_order_book(
                    "gate",
                    symbol,
                    bids=bids[:5] or None,
                    asks=asks[:5] or None,
                    ts=ts,
                )
            except Exception:
                logger.exception("Gate WS: failed to upsert order book for %s", symbol)


async def run_gate(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    """
    Entry point for connecting to the Gate USDT-futures websocket and streaming order book data.

    This will continuously reconnect on failure with exponential backoff. It converts the provided
    list of symbols to Gate contract names and subscribes to depth updates via the
    futures.order_book channel. The function returns only if there are no symbols to subscribe to.
    """
    # Resolve the list of contracts we need to subscribe to.
    contracts: Set[str] = set()
    for s in symbols:
        try:
            c = _symbol_to_contract(s)
            if c:
                contracts.add(c)
        except Exception:
            # Skip invalid symbols gracefully.
            continue

    # Log the start of the run for troubleshooting.
    print("Gate WS: run_gate STARTED, symbols =", sorted(list(contracts)), flush=True)
    if not contracts:
        logger.warning("Gate WS: no contracts to subscribe")
        return

    backoff = 3.0
    while True:
        try:
            logger.info(
                "Gate WS: connecting to %s, %d contracts", WS_URL, len(contracts)
            )
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=20
            ) as ws:
                # Subscribe to order book updates for all contracts.
                sub = {
                    "time": int(time.time()),
                    "channel": "futures.order_book",
                    "event": "subscribe",
                    "payload": sorted(list(contracts)),
                }
                await ws.send(json.dumps(sub))

                # Start heartbeat and consumer tasks concurrently.
                heartbeat_task = asyncio.create_task(_send_heartbeat(ws))
                consume_task = asyncio.create_task(
                    _consume_messages(store, ws, contracts)
                )
                # Wait until one of the tasks raises an exception.
                done, pending = await asyncio.wait(
                    {heartbeat_task, consume_task},
                    return_when=asyncio.FIRST_EXCEPTION,
                )
                # Cancel the other task if one fails.
                for task in pending:
                    task.cancel()
                # Propagate exceptions.
                for task in done:
                    if task.exception():
                        raise task.exception()

            # If we exit the context manager normally, reset backoff.
            backoff = 3.0
        except asyncio.CancelledError:
            # Propagate cancellations to allow graceful shutdown.
            raise
        except Exception:
            logger.exception("Gate WS: unexpected error, will reconnect")
            logger.info("Gate WS: reconnecting in %.1f seconds", backoff)
            # Sleep for backoff seconds before reconnecting.
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 60.0)