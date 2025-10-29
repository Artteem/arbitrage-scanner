from __future__ import annotations

import asyncio
import gzip
import json
import logging
import time
import zlib
from typing import Any, Iterable, List, Sequence, Tuple

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .credentials import ApiCreds
from .discovery import GATE_HEADERS, discover_gate_usdt_perp
from .normalization import normalize_gate_symbol

WS_ENDPOINT = "wss://fx-ws.gateio.ws/v4/ws/usdt"
WS_SUB_BATCH = 80
WS_ORDERBOOK_DEPTH = 30
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
MIN_SYMBOL_THRESHOLD = 1
FALLBACK_SYMBOLS: tuple[Symbol, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")

logger = logging.getLogger(__name__)


def _normalize_common_symbol(symbol: Symbol) -> str:
    return str(symbol).replace("-", "").replace("_", "").upper()


async def _resolve_gate_symbols(symbols: Sequence[Symbol]) -> list[Symbol]:
    requested: list[Symbol] = []
    seen: set[str] = set()
    for symbol in symbols:
        if not symbol:
            continue
        normalized = _normalize_common_symbol(symbol)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        requested.append(normalized)

    discovered: set[str] = set()
    try:
        discovered = await discover_gate_usdt_perp()
    except Exception:
        discovered = set()

    if discovered:
        discovered_normalized = {_normalize_common_symbol(sym) for sym in discovered}
        filtered: list[Symbol] = []
        used: set[str] = set()
        for symbol in requested:
            if symbol in discovered_normalized and symbol not in used:
                filtered.append(symbol)
                used.add(symbol)
        if filtered:
            return filtered
        return sorted(discovered_normalized)

    if not requested:
        return list(FALLBACK_SYMBOLS)

    return requested


def _to_gate_symbol(symbol: Symbol) -> str:
    sym = str(symbol).upper().replace("-", "_")
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    return sym


def _from_gate_symbol(symbol: str | None) -> Symbol | None:
    """
    Пытаемся привести контракт Gate к общему виду 'BTCUSDT'.
    Если нормализатор по какой-то причине вернул None, используем безопасный фолбэк.
    """
    s = normalize_gate_symbol(symbol)
    if s:
        return s
    if not symbol:
        return None
    return str(symbol).replace("_", "").replace("-", "").upper()


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _iter_items(payload):
    if isinstance(payload, dict):
        yield payload
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item


def _iter_levels(source) -> Iterable[Tuple[float, float]]:
    if isinstance(source, dict):
        source = source.get("list") or source.get("data") or source.get("levels") or []
    if not isinstance(source, (list, tuple)):
        return []
    result: List[Tuple[float, float]] = []
    for entry in source:
        price, size = _parse_level(entry)
        if price is None or size is None:
            continue
        result.append((price, size))
    return result


def _parse_level(level) -> Tuple[float | None, float | None]:
    price = size = None
    if isinstance(level, dict):
        for key in ("price", "p", "px", "bid", "ask"):
            val = level.get(key)
            if val is None:
                continue
            try:
                price = float(val)
                break
            except Exception:
                continue
        for key in ("size", "qty", "q", "amount", "volume"):
            val = level.get(key)
            if val is None:
                continue
            try:
                size = float(val)
                break
            except Exception:
                continue
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        try:
            price = float(level[0])
        except Exception:
            price = None
        try:
            size = float(level[1])
        except Exception:
            size = None
    if price is None or price <= 0:
        return None, None
    if size is None or size < 0:
        size = 0.0
    return price, size


def _is_active_contract(item: dict) -> bool:
    state = str(item.get("state") or item.get("status") or "").strip().lower()
    if state and state not in {"open", "trading", "live"}:
        return False
    if bool(item.get("is_delisted")):
        return False
    in_delisting = item.get("in_delisting")
    if isinstance(in_delisting, str):
        if in_delisting.strip().lower() in {"true", "1"}:
            return False
    elif in_delisting:
        return False
    return True


def _extract_price(item: dict, keys: Iterable[str]) -> float:
    for key in keys:
        val = item.get(key)
        price = _as_float(val)
        if price > 0:
            return price
    return 0.0


def _chunk(symbols: Sequence[Symbol], size: int) -> Iterable[Sequence[Symbol]]:
    for idx in range(0, len(symbols), size):
        yield symbols[idx : idx + size]


async def run_gate(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    subscribe = await _resolve_gate_symbols(symbols)
    if not subscribe:
        return

    chunks = [tuple(chunk) for chunk in _chunk(subscribe, WS_SUB_BATCH)]
    if not chunks:
        return

    while True:
        tasks: list[asyncio.Task] = [
            asyncio.create_task(_run_gate_ws(store, chunk)) for chunk in chunks
        ]

        try:
            await asyncio.gather(*tasks)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Gate websocket workers crashed; restarting")
            await asyncio.sleep(WS_RECONNECT_INITIAL)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def _run_gate_ws(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    symbol_pairs: list[tuple[str, str]] = []
    for sym in symbols:
        if not sym:
            continue
        native = _to_gate_symbol(sym)
        if not native:
            continue
        symbol_pairs.append((str(sym), native))

    native_symbols = [native for _, native in symbol_pairs]
    if not native_symbols:
        return

    async for ws in _reconnect_ws():
        try:
            if not await _perform_initial_ping(ws):
                continue

            await _send_subscriptions(ws, symbol_pairs)

            async for raw in ws:
                message = _decode_ws_message(raw)
                if message is None:
                    continue

                if await _handle_ping(ws, message):
                    continue

                if _is_ack(message):
                    continue

                channel = str(message.get("channel") or "")
                data = (
                    message.get("result")
                    or message.get("payload")
                    or message.get("data")
                )

                if channel == "futures.tickers":
                    _handle_tickers(store, data)
                elif channel == "futures.funding_rate":
                    _handle_funding(store, data)
                elif channel == "futures.order_book":
                    _handle_orderbook(store, data)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(WS_RECONNECT_INITIAL)


async def _reconnect_ws():
    delay = WS_RECONNECT_INITIAL
    while True:
        try:
            async with websockets.connect(
                WS_ENDPOINT,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                extra_headers=GATE_HEADERS,
                compression=None,
            ) as ws:
                delay = WS_RECONNECT_INITIAL
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


async def _send_subscriptions(ws, symbols: Sequence[tuple[str, str]]) -> None:
    unique: list[tuple[str, str]] = []
    seen: set[str] = set()
    for common, native in symbols:
        if not native:
            continue
        if native in seen:
            continue
        seen.add(native)
        unique.append((common, native))

    if not unique:
        return

    now = int(time.time())
    for common, native in unique:
        logger.info("Gate subscribe ticker -> %s (native=%s)", common, native)

    for channel in ("futures.tickers", "futures.funding_rate"):
        for _, native in unique:
            message = {
                "time": now,
                "channel": channel,
                "event": "subscribe",
                "payload": [native],
            }
            await ws.send(json.dumps(message))

    for _, native in unique:
        message = {
            "time": now,
            "channel": "futures.order_book",
            "event": "subscribe",
            "payload": [native, str(WS_ORDERBOOK_DEPTH), "0.1"],
        }
        await ws.send(json.dumps(message))


def _decode_ws_message(message: str | bytes) -> dict | None:
    if isinstance(message, str):
        raw = message
    elif isinstance(message, (bytes, bytearray)):
        raw = _decode_ws_bytes(bytes(message))
    else:
        return None

    if not raw:
        return None

    try:
        return json.loads(raw)
    except Exception:
        return None


def _decode_ws_bytes(data: bytes) -> str | None:
    if not data:
        return None

    for decoder in (_decode_utf8, _decode_gzip, _decode_zlib):
        try:
            text = decoder(data)
        except Exception:
            continue
        if text:
            return text
    return None


def _decode_utf8(data: bytes) -> str:
    return data.decode("utf-8", errors="strict")


def _decode_gzip(data: bytes) -> str:
    if len(data) < 2 or data[0] != 0x1F or data[1] != 0x8B:
        raise ValueError("not gzip")
    return gzip.decompress(data).decode("utf-8")


def _decode_zlib(data: bytes) -> str:
    if len(data) < 2 or data[0] != 0x78:
        raise ValueError("not zlib")
    return zlib.decompress(data).decode("utf-8")


async def _perform_initial_ping(ws) -> bool:
    message = {
        "time": int(time.time()),
        "channel": "futures.ping",
        "event": "ping",
    }

    try:
        await ws.send(json.dumps(message))
    except Exception:
        return False

    while True:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
        except asyncio.TimeoutError:
            return False
        except Exception:
            return False

        payload = _decode_ws_message(raw)
        if payload is None:
            continue

        event = str(payload.get("event") or "").lower()
        channel = str(payload.get("channel") or "")

        if event == "pong" or channel == "futures.ping":
            return True

        if event == "ping" or channel.endswith(".ping"):
            reply = {
                "time": int(time.time()),
                "channel": channel or "futures.ping",
                "event": "pong",
            }
            try:
                await ws.send(json.dumps(reply))
            except Exception:
                return False
            if event == "ping" and channel == "futures.ping":
                continue
            if event == "pong" or channel == "futures.ping":
                return True

        if event in {"subscribe", "unsubscribe"}:
            continue

        # Unexpected message before handshake; continue waiting.


async def _handle_ping(ws, message: dict) -> bool:
    if not isinstance(message, dict):
        return False

    event = str(message.get("event") or "").lower()
    channel = str(message.get("channel") or "")

    if event not in {"ping", "pong"} and not channel.endswith(".ping"):
        return False

    if event == "ping" or channel.endswith(".ping"):
        reply = {
            "time": int(time.time()),
            "channel": channel or "futures.ping",
            "event": "pong",
        }
        try:
            await ws.send(json.dumps(reply))
        except Exception:
            pass
        return True

    return event == "pong"


def _is_ack(message: dict) -> bool:
    if not isinstance(message, dict):
        return False
    event = str(message.get("event") or "").lower()
    if event in {"subscribe", "unsubscribe"}:
        return True
    if message.get("error") or message.get("code"):
        return True
    return False


def _handle_tickers(store: TickerStore, payload) -> None:
    now = time.time()
    for item in _iter_items(payload):
        contract = (
            item.get("contract")
            or item.get("name")
            or item.get("symbol")
            or item.get("s")
        )
        if not contract:
            continue

        if not _is_active_contract(item):
            continue

        symbol = _from_gate_symbol(str(contract))
        if not symbol:
            continue

        bid = _extract_price(
            item,
            (
                "best_bid_price",
                "highest_bid",
                "bid1",
                "best_bid",
                "bid",
            ),
        )
        ask = _extract_price(
            item,
            (
                "best_ask_price",
                "lowest_ask",
                "ask1",
                "best_ask",
                "ask",
            ),
        )

        if bid <= 0 or ask <= 0:
            continue

        store.upsert_ticker(
            Ticker(exchange="gate", symbol=symbol, bid=bid, ask=ask, ts=now)
        )

        last_raw = (
            item.get("last")
            or item.get("last_price")
            or item.get("mark_price")
            or item.get("index_price")
        )
        last_price = _as_float(last_raw)
        if last_price > 0:
            store.upsert_order_book(
                "gate",
                symbol,
                last_price=last_price,
                last_price_ts=now,
            )

        rate_raw = (
            item.get("funding_rate")
            or item.get("funding_rate_indicative")
            or item.get("next_funding_rate")
        )
        if rate_raw is not None:
            rate = _as_float(rate_raw)
            store.upsert_funding("gate", symbol, rate=rate, interval="8h", ts=now)


def _handle_funding(store: TickerStore, payload) -> None:
    now = time.time()
    for item in _iter_items(payload):
        contract = item.get("contract") or item.get("name") or item.get("symbol")
        if not contract:
            continue
        symbol = _from_gate_symbol(str(contract))
        if not symbol:
            continue
        rate_raw = item.get("rate") or item.get("funding_rate")
        if rate_raw is None:
            continue
        rate = _as_float(rate_raw)
        store.upsert_funding("gate", symbol, rate=rate, interval="8h", ts=now)


def _handle_orderbook(store: TickerStore, payload) -> None:
    now = time.time()
    for item in _iter_items(payload):
        contract = (
            item.get("contract")
            or item.get("name")
            or item.get("symbol")
            or item.get("s")
        )
        if not contract:
            continue
        symbol = _from_gate_symbol(str(contract))
        if not symbol:
            continue

        bids_raw = item.get("bids") or item.get("bid") or item.get("buy")
        asks_raw = item.get("asks") or item.get("ask") or item.get("sell")

        bids = list(_iter_levels(bids_raw))[:20]
        asks = list(_iter_levels(asks_raw))[:20]

        last_price = _extract_price(
            item,
            (
                "last",
                "last_price",
                "mark_price",
                "index_price",
            ),
        )

        store.upsert_order_book(
            "gate",
            symbol,
            bids=bids or None,
            asks=asks or None,
            ts=now,
            last_price=last_price if last_price > 0 else None,
            last_price_ts=now if last_price > 0 else None,
        )



async def authenticate_ws(ws: Any, creds: ApiCreds | None) -> None:
    """Placeholder for future authenticated Gate channels."""
    del ws, creds
    return None
