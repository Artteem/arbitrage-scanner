from __future__ import annotations

"""
Modified Gate.io USDT‑perpetual connector.

This file preserves the original message parsing logic while bringing the
high‑level connection management in line with the Binance/Bybit style.  The
notable changes and clarifications are:

* A detailed comment on the WebSocket subscription limits: Gate.io prohibits
  duplicate depth subscriptions for a given contract within a single
  connection【817334707807301†L1990-L2013】.  There is no strict per‑connection
  topic limit published, but for consistency we limit the number of contracts
  per connection to ``MAX_TOPICS_PER_CONN`` (100) to avoid overwhelming the
  server.
* Resolved symbols are discovered via ``discover_gate_usdt_perp``.  If the
  caller provides fewer than one symbol, the connector falls back to the full
  contract list; otherwise only requested symbols that exist on Gate are used.
* Comments have been expanded to document the behaviour of each helper and
  pipeline stage.

Usage: call ``run_gate(store, symbols)`` with a list of USDT‑perpetual
``Symbol`` values.  The connector will manage reconnections and heartbeat
automatically.
"""

import asyncio
import gzip
import json
import logging
import time
import zlib
from typing import Any, Iterable, List, Sequence, Tuple, Dict

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .credentials import ApiCreds  # noqa: F401 – reserved for future use
from .discovery import GATE_HEADERS, discover_gate_usdt_perp
from .normalization import normalize_gate_symbol

# WebSocket endpoint for Gate.io USDT futures.  The exchange also offers
# endpoints for other quote currencies such as USD and BTC; if needed, you
# should adjust WS_ENDPOINT accordingly.
WS_ENDPOINT = "wss://fx-ws.gateio.ws/v4/ws/usdt"

# Maximum number of contracts per WebSocket connection.  Gate.io does not
# specify a hard limit on the number of subscriptions, but anecdotal
# experience suggests that grouping too many contracts can lead to drops.
# Furthermore, the documentation warns that duplicate order book subscriptions
# for the same contract in a single connection will return an error
#【817334707807301†L1990-L2013】.  Limiting to 100 contracts per connection is
# conservative and matches the legacy implementation.
MAX_TOPICS_PER_CONN = 100

# Number of order book levels requested.  The Gate API supports depths of 50
# or 400; using 100 provides a mid‑range snapshot without overwhelming the
# client.
WS_ORDERBOOK_DEPTH = 100

# Delay between successive subscription messages in seconds.
WS_SUB_DELAY = 0.1

# Interval (seconds) between heartbeat pings.
HEARTBEAT_INTERVAL = 20.0

# Reconnect backoff times.
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0

# If the number of supplied symbols is less than this threshold, the
# connector falls back to the discovered list of all active contracts.  Gate.io
# provides a comprehensive contract list via ``discover_gate_usdt_perp``.
MIN_SYMBOL_THRESHOLD = 1

# Default fallback set if discovery fails.
FALLBACK_SYMBOLS: tuple[Symbol, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")

logger = logging.getLogger(__name__)


def _normalize_common_symbol(symbol: Symbol) -> str:
    """Normalize an incoming symbol by removing separators and upper‑casing."""
    return str(symbol).replace("-", "").replace("_", "").upper()


async def _resolve_gate_symbols(symbols: Sequence[Symbol]) -> list[Symbol]:
    """Resolve user‑provided symbols against discovered Gate USDT perpetual contracts."""
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
        # If none of the requested symbols exist, fall back to the full list
        return sorted(discovered_normalized)
    # Discovery failed; fall back to either the user‑supplied symbols or defaults
    if not requested:
        return list(FALLBACK_SYMBOLS)
    return requested


def _to_gate_symbol(symbol: Symbol) -> str:
    """Convert a common symbol (BTCUSDT) to Gate contract format (BTC_USDT)."""
    sym = str(symbol).upper().replace("-", "_")
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    return sym


def _from_gate_symbol(symbol: str | None) -> Symbol | None:
    """Convert a Gate contract symbol back into the common format (BTCUSDT)."""
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
    """Yield dict items from a payload that may be a list or dict."""
    if isinstance(payload, dict):
        yield payload
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item


def _iter_levels(source) -> Iterable[Tuple[float, float]]:
    """Parse order book levels from various possible field formats."""
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
    """Filter out delisted or inactive contracts."""
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


async def run_gate(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    """Entry point for the Gate connector.

    Resolves symbols, splits them into batches and launches a WebSocket worker
    for each batch.  Workers run until cancelled or until an unrecoverable
    error occurs, at which point all workers are restarted.  This mirrors the
    behaviour of the Binance and Bybit connectors.
    """
    subscribe = await _resolve_gate_symbols(symbols)
    if not subscribe:
        return
    # Build list of (common_symbol, native_symbol) tuples.
    symbol_pairs: list[tuple[str, str]] = []
    for sym in subscribe:
        native = _to_gate_symbol(sym)
        if not native:
            continue
        symbol_pairs.append((sym, native))
    if not symbol_pairs:
        return
    # Split into chunks to avoid overloading a single connection.  This
    # constant does not correspond to the number of topics but rather the
    # number of contracts; each contract results in three subscriptions
    # (tickers, orderbook and funding rate).
    chunks = [
        tuple(symbol_pairs[idx : idx + MAX_TOPICS_PER_CONN])
        for idx in range(0, len(symbol_pairs), MAX_TOPICS_PER_CONN)
    ]
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


async def _run_gate_ws(
    store: TickerStore, symbol_pairs: Sequence[tuple[str, str]]
) -> None:
    native_symbols = [native for _, native in symbol_pairs]
    if not native_symbols:
        return
    async for ws in _reconnect_ws():
        heartbeat: asyncio.Task | None = None
        try:
            # Perform initial ping handshake; Gate requires a ping/pong before
            # allowing subscriptions.  If the handshake fails we retry.
            if not await _perform_initial_ping(ws):
                continue
            # Send subscriptions for each contract.
            await _send_subscriptions(ws, symbol_pairs)
            # Start heartbeat to keep the connection alive.
            heartbeat = asyncio.create_task(
                _ws_heartbeat(ws, interval=HEARTBEAT_INTERVAL, name="gate")
            )
            async for raw in ws:
                _log_ws_raw_frame("gate", raw)
                message = _decode_ws_message(raw)
                if int(time.time()) % 10 == 0:
                    logger.debug("WS RX sample: %s", str(message)[:300])
                if message is None:
                    continue
                # Handle ping/pong messages
                if await _handle_ping(ws, message):
                    continue
                # Ignore subscription acknowledgements
                if _is_gate_ack(message):
                    logger.info("Gate WS ack: %s", str(message)[:500])
                    continue
                # Log any error messages from the server
                if isinstance(message, dict) and (
                    message.get("error") or message.get("code")
                ):
                    logger.error("Gate WS error: %s", str(message)[:500])
                    continue
                # Dispatch by channel
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
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)


async def _reconnect_ws():
    """Yield a connected WebSocket, with exponential backoff on failure."""
    delay = WS_RECONNECT_INITIAL
    while True:
        try:
            async with websockets.connect(
                WS_ENDPOINT,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                extra_headers=GATE_HEADERS,
                compression="deflate",
            ) as ws:
                delay = WS_RECONNECT_INITIAL
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


async def _send_subscriptions(ws, symbols: Sequence[tuple[str, str]]) -> None:
    """Subscribe to ticker, order book and funding rate channels for each contract."""
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
    for common, native in unique:
        now = int(time.time())
        logger.debug("Gate subscribe ticker -> %s (native=%s)", common, native)
        ticker_payload = {
            "time": now,
            "channel": "futures.tickers",
            "event": "subscribe",
            "payload": [native],
        }
        await _send_ws_payload(ws, ticker_payload)
        await asyncio.sleep(WS_SUB_DELAY)
        logger.debug("Gate subscribe depth -> %s", native)
        depth_payload = {
            "time": now,
            "channel": "futures.order_book",
            "event": "subscribe",
            "payload": [native, WS_ORDERBOOK_DEPTH],
        }
        await _send_ws_payload(ws, depth_payload)
        await asyncio.sleep(WS_SUB_DELAY)
        logger.debug("Gate subscribe funding -> %s", native)
        funding_payload = {
            "time": now,
            "channel": "futures.funding_rate",
            "event": "subscribe",
            "payload": [native],
        }
        await _send_ws_payload(ws, funding_payload)
        await asyncio.sleep(WS_SUB_DELAY)


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


async def _send_ws_payload(ws, payload: dict) -> None:
    try:
        await ws.send(json.dumps(payload))
        logger.debug("Gate WS send -> %s", json.dumps(payload)[:200])
    except Exception:
        logger.exception("Gate subscription send failed", extra={"payload": payload})


async def _perform_initial_ping(ws) -> bool:
    """Perform the mandatory initial ping handshake with the Gate WS server."""
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
        _log_ws_raw_frame("gate", raw)
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


def _is_gate_ack(message: dict) -> bool:
    if not isinstance(message, dict):
        return False
    event = str(message.get("event") or "").lower()
    result = message.get("result")
    if event in {"subscribe", "unsubscribe"}:
        if isinstance(result, str):
            return result.lower() == "success"
        if isinstance(result, dict):
            return str(result.get("status") or "").lower() == "success"
        if result is True:
            return True
    if event == "update" and message.get("success") is True:
        return True
    return False


def _log_ws_raw_frame(exchange: str, message: str | bytes | bytearray) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return
    if isinstance(message, (bytes, bytearray)):
        logger.debug(
            "%s WS RX raw frame (%d bytes)",
            exchange.upper(),
            len(message),
        )
        return
    logger.debug(
        "%s WS RX raw frame: %s",
        exchange.upper(),
        str(message)[:512],
    )


async def _ws_heartbeat(ws, *, interval: float, name: str) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            if ws.closed:
                raise ConnectionError(f"{name} websocket closed during heartbeat")
            try:
                pong = await ws.ping()
                await asyncio.wait_for(pong, timeout=interval)
            except asyncio.TimeoutError as exc:
                raise ConnectionError(f"{name} websocket heartbeat timeout") from exc
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("%s heartbeat failed", name)
        try:
            await ws.close()
        except Exception:
            pass
        raise


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
        logger.info(
            "WS PARSE exchange=%s native=%s -> common=%s",
            "gate",
            contract,
            symbol,
        )
        if not symbol:
            logger.warning(
                "WS DROP reason=normalize_none exchange=%s native=%s payload=%s",
                "gate",
                contract,
                str(item)[:500],
            )
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
        logger.info(
            "WS PARSE exchange=%s native=%s -> common=%s",
            "gate",
            contract,
            symbol,
        )
        if not symbol:
            logger.warning(
                "WS DROP reason=normalize_none exchange=%s native=%s payload=%s",
                "gate",
                contract,
                str(item)[:500],
            )
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
        logger.info(
            "WS PARSE exchange=%s native=%s -> common=%s",
            "gate",
            contract,
            symbol,
        )
        if not symbol:
            logger.warning(
                "WS DROP reason=normalize_none exchange=%s native=%s payload=%s",
                "gate",
                contract,
                str(item)[:500],
            )
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