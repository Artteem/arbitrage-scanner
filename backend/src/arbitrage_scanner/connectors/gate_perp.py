from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Sequence, Set, Tuple

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore

logger = logging.getLogger(__name__)

# Websocket endpoint for Gate USDT-margined perpetual futures.
WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
HEARTBEAT_INTERVAL = 20.0  # seconds
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
MIN_SYMBOL_THRESHOLD = 5
SUB_DEPTH = 50
SUB_INTERVAL = "0"



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
    return Symbol(contract.replace("_", "").upper())


def _safe_float(value) -> float | None:
    """Try to convert a value to float. Returns None for invalid values."""
    try:
        if value is None:
            return None
        f = float(value)
        if f != f or f == float("inf") or f == float("-inf"):
            return None
        return f
    except Exception:
        return None


def _now_ts(raw: float | int | None = None) -> float:
    if raw is None:
        return time.time()
    try:
        ts_val = float(raw)
    except (TypeError, ValueError):
        return time.time()
    if ts_val > 1_000_000_000_000:
        ts_val /= 1000.0
    return ts_val if ts_val > 0 else time.time()


def _iter_levels(levels: Iterable[Iterable]) -> Iterable[tuple[float, float]]:
    for entry in levels:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        price = _safe_float(entry[0])
        size = _safe_float(entry[1])
        if price and price > 0 and size and size > 0:
            yield price, size


@dataclass
class _OrderBookState:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    ts: float = 0.0

    def snapshot(self, bids, asks, ts: float) -> None:
        self.bids.clear()
        self.asks.clear()
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)
        self.ts = ts

    def update(self, bids, asks, ts: float) -> None:
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)
        self.ts = ts

    def top_levels(
        self, depth: int = 5
    ) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        best_bids = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:depth]
        best_asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:depth]
        return best_bids, best_asks

    def _apply(self, side: Dict[float, float], updates) -> None:
        for price, size in _iter_levels(updates or []):
            if size <= 0:
                side.pop(price, None)
            else:
                side[price] = size


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
            logger.warning("Gate WS heartbeat failed, closing connection")
            raise


def _extract_order_books(result) -> Iterable[dict]:
    if isinstance(result, dict):
        if {"bids", "asks"} & set(result.keys()):
            yield result
        return
    if isinstance(result, list):
        for item in result:
            if isinstance(item, dict):
                yield item


async def _resolve_contracts(symbols: Sequence[Symbol]) -> List[str]:
    unique: List[Symbol] = []
    seen: set[str] = set()
    for sym in symbols:
        if not sym:
            continue
        normalized = str(sym).strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(Symbol(normalized))

    if len(unique) < MIN_SYMBOL_THRESHOLD:
        try:
            from .discovery import discover_gate_usdt_perp

            discovered = await discover_gate_usdt_perp()
        except Exception:
            discovered = set()
        if discovered:
            unique = sorted({Symbol(sym) for sym in discovered})
    return [_symbol_to_contract(sym) for sym in unique if sym]


async def _consume_messages(
    store: TickerStore, ws, contracts: Set[str]
) -> None:
    """
    Consume order book messages from the websocket and update the ticker and order book
    in the shared TickerStore. Only messages for contracts in the provided set are processed.
    """
    books: Dict[str, _OrderBookState] = {}
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
            continue
        if event == "subscribe":
            logger.info(
                "Gate WS: subscribed futures.order_book payload=%s", data.get("result")
            )
            continue

        if event == "error":
            logger.error("Gate WS: subscription error %s", data)
            continue
        if event != "update":
            continue


        ts = _now_ts(data.get("time") or data.get("time_ms"))
        for item in _extract_order_books(data.get("result")):
            contract = str(item.get("s") or item.get("contract") or "")
            if not contract or contract not in contracts:
                continue
            symbol = _contract_to_symbol(contract)
            book = books.setdefault(contract, _OrderBookState())

            bids = item.get("bids") or item.get("b")
            asks = item.get("asks") or item.get("a")

            if str(data.get("type") or "").lower() == "snapshot":
                book.snapshot(bids, asks, ts)
            else:
                book.update(bids, asks, ts)

            top_bids, top_asks = book.top_levels()
            if not top_bids and not top_asks:
                continue

            try:
                last_price = _safe_float(item.get("last"))
                store.upsert_order_book(
                    "gate",
                    symbol,
                    bids=top_bids or None,
                    asks=top_asks or None,
                    ts=book.ts or ts,

                    last_price=last_price or None,
                    last_price_ts=book.ts or ts,

                )
            except Exception:
                logger.exception("Gate WS: failed to upsert order book for %s", symbol)

            if top_bids and top_asks:
                try:
                    ticker = Ticker(
                        exchange="gate",
                        symbol=symbol,
                        bid=top_bids[0][0],
                        ask=top_asks[0][0],
                        ts=book.ts or ts,
                    )
                    store.upsert_ticker(ticker)
                except Exception:
                    logger.exception("Gate WS: failed to upsert ticker for %s", symbol)


async def run_gate(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    """
    Entry point for connecting to the Gate USDT-futures websocket and streaming order book data.

    This will continuously reconnect on failure with exponential backoff. It converts the provided
    list of symbols to Gate contract names and subscribes to depth updates via the
    futures.order_book channel. The function returns only if there are no symbols to subscribe to.
    """
    contracts = await _resolve_contracts(symbols)
    if not contracts:
        logger.warning("Gate WS: no contracts to subscribe")
        return

    backoff = WS_RECONNECT_INITIAL
    while True:
        try:
            logger.info(
                "Gate WS: connecting to %s, %d contracts", WS_URL, len(contracts)
            )
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=20

                payloads = [
                    {
                        "time": int(time.time()),
                        "channel": "futures.order_book",
                        "event": "subscribe",
                        "payload": [contract, str(SUB_DEPTH), SUB_INTERVAL],
                    }
                    for contract in sorted(contracts)
                ]
                for msg in payloads:
                    await ws.send(json.dumps(msg))


                heartbeat_task = asyncio.create_task(_send_heartbeat(ws))
                consume_task = asyncio.create_task(
                    _consume_messages(store, ws, set(contracts))
                )
                done, pending = await asyncio.wait(
                    {heartbeat_task, consume_task},
                    return_when=asyncio.FIRST_EXCEPTION,
                )
                for task in pending:
                    task.cancel()
                for task in done:
                    if task.exception():
                        raise task.exception()

            backoff = WS_RECONNECT_INITIAL
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Gate WS: unexpected error, will reconnect")
            logger.info("Gate WS: reconnecting in %.1f seconds", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WS_RECONNECT_MAX)
