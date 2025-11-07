from __future__ import annotations

"""
MEXC USDT perpetual futures connector.

This module implements an asynchronous connector for MEXC's futures WebSocket API.
It subscribes to ticker, order book and funding rate topics for the configured
symbols and updates a shared ``TickerStore`` with live best bid/ask quotes and
funding rates.

The implementation follows the official documentation for the MEXC futures
WebSocket API available at:
https://www.mexc.com/api-docs/futures/websocket-api

Key differences from the older ``mexc_perp.py``:

* The WebSocket endpoint has been switched from ``wss://contract.mexc.com/ws``
  to ``wss://contract.mexc.com/edge``.  The ``/edge`` endpoint uses a newer
  message format where subscription commands are expressed via a ``method``
  field and parameters are provided in a ``param`` object.
* Subscriptions now use ``method`` keys (e.g. ``sub.ticker``, ``sub.depth``,
  ``sub.funding.rate``) rather than the legacy ``op`` keys.  A ``gzip`` flag
  set to ``False`` is sent with each request to request uncompressed payloads.
* Funding rate subscription uses the correct topic name ``sub.funding.rate``
  instead of the old ``sub.funding_rate``.

The remainder of the implementation retains backwards-compatible message
parsing logic so that both ``push.depth`` and ``depth.update`` messages are
supported.  Order book updates are consolidated per-symbol and best bid/ask
quotes are derived from the top of the book.  Funding rate and ticker
messages are also processed to update the store.
"""

import asyncio
import gzip
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import websockets
import zlib

from .domain import Symbol, Ticker
from .store import TickerStore
from .credentials import ApiCreds
from .discovery import discover_mexc_usdt_perp

# MEXC futures WebSocket endpoint (new format)
WS_ENDPOINT = "wss://contract.mexc.com/edge"
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
MAX_TOPICS_PER_CONN = 100
WS_SUB_DELAY = 0.1
HEARTBEAT_INTERVAL = 20.0
MIN_SYMBOL_THRESHOLD = 1
FALLBACK_SYMBOLS: tuple[Symbol, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")

MEXC_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Origin": "https://www.mexc.com",
    "Referer": "https://www.mexc.com/",
    "Accept": "application/json, text/plain, */*",
}

logger = logging.getLogger(__name__)


def _normalize_common_symbol(symbol: Symbol) -> str:
    return str(symbol).replace("-", "").replace("_", "").upper()


async def _resolve_mexc_symbols(symbols: Sequence[Symbol]) -> list[Symbol]:
    """Resolve configured symbols against discovered MEXC USDT perpetual markets.

    If no symbols are explicitly configured, all discovered symbols are returned.
    Falling back to a default list in case discovery fails.
    """
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

    try:
        discovered = await discover_mexc_usdt_perp()
    except Exception:
        logger.exception("Failed to discover MEXC symbols; falling back to configured list")
        discovered = set()

    discovered_normalized: dict[str, Symbol] = {}
    for sym in discovered:
        normalized = _normalize_common_symbol(sym)
        if normalized and normalized not in discovered_normalized:
            discovered_normalized[normalized] = normalized

    if discovered_normalized:
        if not requested:
            return sorted(discovered_normalized.keys())
        filtered = [symbol for symbol in requested if symbol in discovered_normalized]
        if filtered:
            return filtered
        logger.warning(
            "Requested symbols are unavailable on MEXC; using discovered symbols",
            extra={"requested": requested},
        )
        return sorted(discovered_normalized.keys())

    if requested:
        logger.warning(
            "MEXC discovery unavailable; using configured symbol list",
            extra={"requested": requested},
        )
        return requested

    logger.warning(
        "MEXC discovery unavailable; using fallback symbols",
        extra={"fallback": FALLBACK_SYMBOLS},
    )
    return list(FALLBACK_SYMBOLS)


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_mexc_contract(symbol: Symbol) -> str:
    sym = str(symbol).replace('-', '').replace(' ', '')
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    return sym


def _from_mexc_symbol(symbol: str | None) -> Symbol | None:
    if not symbol:
        return None
    return symbol.replace("_", "")


def _extract_bid(item) -> float:
    for key in ("bid1", "bestBidPrice", "bestBid"):
        val = item.get(key)
        bid = _as_float(val)
        if bid > 0:
            return bid
    return 0.0


def _extract_ask(item) -> float:
    for key in ("ask1", "bestAskPrice", "bestAsk"):
        val = item.get(key)
        ask = _as_float(val)
        if ask > 0:
            return ask
    return 0.0


def _parse_interval(item) -> str:
    interval = item.get("fundingInterval") or item.get("interval")
    if isinstance(interval, (int, float)):
        return f"{interval}h"
    if isinstance(interval, str) and interval:
        return interval
    return "8h"


async def run_mexc(store: TickerStore, symbols: Sequence[Symbol]):
    """Entry point for the MEXC connector.

    Resolves symbols, splits them into connection chunks and starts the WS
    workers.  Restarts all workers if any connection crashes.
    """
    subscribe = await _resolve_mexc_symbols(symbols)
    if not subscribe:
        logger.warning("No symbols resolved for MEXC connector; skipping startup")
        return

    # Build list of (common_symbol, native_symbol) tuples.
    symbol_pairs: list[tuple[str, str]] = []
    for sym in subscribe:
        native = _to_mexc_contract(sym)
        if not native:
            continue
        symbol_pairs.append((sym, native))

    if not symbol_pairs:
        logger.warning("MEXC connector resolved symbols but produced no native pairs")
        return

    # Split into chunks to stay within the subscription limit.
    chunks = [
        tuple(symbol_pairs[idx : idx + MAX_TOPICS_PER_CONN])
        for idx in range(0, len(symbol_pairs), MAX_TOPICS_PER_CONN)
    ]

    if not chunks:
        return

    while True:
        tasks = [asyncio.create_task(_run_mexc_ws(store, chunk)) for chunk in chunks]
        try:
            await asyncio.gather(*tasks)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("MEXC websocket worker crashed; restarting all connections")
            await asyncio.sleep(WS_RECONNECT_INITIAL)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def _run_mexc_ws(
    store: TickerStore, symbol_pairs: Sequence[tuple[str, str]]
) -> None:
    """Run a single WebSocket connection for a batch of symbols."""
    wanted_exchange = [native for _, native in symbol_pairs]
    if not wanted_exchange:
        return
    wanted_common = {common for common, _ in symbol_pairs if common}
    if not wanted_common:
        return
    books: Dict[str, _MexcOrderBookState] = {}
    async for ws in _reconnect_ws():
        heartbeat: asyncio.Task | None = None
        try:
            await _send_mexc_subscriptions(ws, symbol_pairs)
            heartbeat = asyncio.create_task(
                _ws_heartbeat(ws, interval=HEARTBEAT_INTERVAL, name="mexc")
            )
            async for raw in ws:
                _log_ws_raw_frame("mexc", raw)
                msg = _decode_ws_message(raw)
                if int(time.time()) % 10 == 0:
                    logger.debug("WS RX sample: %s", str(msg)[:300])
                if msg is None:
                    continue
                # Ignore ack and error messages
                raw_message = str(msg)
                if _is_mexc_ack(msg):
                    logger.info("MEXC WS ack: %s", raw_message[:500])
                    continue
                if isinstance(msg, dict) and msg.get("channel") == "rs.error":
                    logger.error("MEXC WS error: %s", raw_message[:500])
                    continue
                if _is_ping(msg):
                    await _reply_pong(ws, msg)
                    continue

                # Extract order book updates
                depth_payload = _extract_depth_message(msg)
                if depth_payload:
                    snapshot, data, sym_raw = depth_payload
                    if not isinstance(data, dict):
                        continue
                    sym_common = _from_mexc_symbol(sym_raw)
                    logger.info(
                        "WS PARSE exchange=%s native=%s -> common=%s",
                        "mexc",
                        sym_raw,
                        sym_common,
                    )
                    if not sym_common:
                        logger.warning(
                            "WS DROP reason=normalize_none exchange=%s native=%s payload=%s",
                            "mexc",
                            sym_raw,
                            raw_message[:500],
                        )
                        continue
                    if sym_common not in wanted_common:
                        continue
                    bids_raw = data.get("bids") or data.get("b") or data.get("buy")
                    asks_raw = data.get("asks") or data.get("a") or data.get("sell")
                    book = books.setdefault(sym_common, _MexcOrderBookState())
                    if snapshot:
                        book.snapshot(bids_raw, asks_raw)
                    else:
                        book.update(bids_raw, asks_raw)
                    bids, asks = book.top_levels()
                    last_price = _extract_last_price(data)
                    store.upsert_order_book(
                        "mexc",
                        sym_common,
                        bids=bids or None,
                        asks=asks or None,
                        ts=time.time(),
                        last_price=last_price,
                    )
                    if bids and asks:
                        best_bid = bids[0][0]
                        best_ask = asks[0][0]
                        store.upsert_ticker(
                            Ticker(
                                exchange="mexc",
                                symbol=sym_common,
                                bid=best_bid,
                                ask=best_ask,
                                ts=time.time(),
                            )
                        )
                    continue
                # Handle other payloads: funding rate, ticker
                now = time.time()
                for sym_raw, payload in _iter_mexc_payloads(msg):
                    if not isinstance(payload, dict):
                        continue
                    sym_common = _from_mexc_symbol(sym_raw)
                    logger.info(
                        "WS PARSE exchange=%s native=%s -> common=%s",
                        "mexc",
                        sym_raw,
                        sym_common,
                    )
                    if not sym_common:
                        logger.warning(
                            "WS DROP reason=normalize_none exchange=%s native=%s payload=%s",
                            "mexc",
                            sym_raw,
                            raw_message[:500],
                        )
                        continue
                    if sym_common not in wanted_common:
                        continue
                    if _looks_like_depth(payload):
                        continue
                    rate = _extract_funding_rate(payload)
                    if rate is not None:
                        interval = _parse_interval(payload)
                        store.upsert_funding(
                            "mexc", sym_common, rate=rate, interval=interval, ts=now
                        )
                        continue
                    bid = _extract_bid(payload)
                    ask = _extract_ask(payload)
                    if bid > 0 and ask > 0:
                        store.upsert_ticker(
                            Ticker(exchange="mexc", symbol=sym_common, bid=bid, ask=ask, ts=now)
                        )
                        last_price = _extract_last_price(payload)
                        if last_price:
                            store.upsert_order_book(
                                "mexc",
                                sym_common,
                                last_price=last_price,
                                last_price_ts=now,
                            )
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)


async def _reconnect_ws():
    delay = WS_RECONNECT_INITIAL
    while True:
        try:
            async with websockets.connect(
                WS_ENDPOINT,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                extra_headers=MEXC_HEADERS,
                # Keep deflate compression enabled for legacy support.  Messages
                # should arrive uncompressed due to ``gzip": false`` in our
                # subscription requests, but the server may still use RFC 7692
                # per-frame compression (deflate) which the websockets library
                # can handle automatically.
                compression="deflate",
            ) as ws:
                delay = WS_RECONNECT_INITIAL
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


async def _send_mexc_subscriptions(
    ws, symbols: Sequence[tuple[str, str]]
) -> None:
    """
    Send subscription requests for ticker, order book and funding rate topics.

    Each request includes a ``method`` key and a ``param`` dict with the
    contract symbol.  A ``gzip`` flag set to ``False`` requests plaintext
    responses.  See: https://www.mexc.com/api-docs/futures/websocket-api
    """
    if not symbols:
        return
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
        # Subscribe to per-contract ticker (best bid/ask, last price).
        logger.debug("MEXC subscribe ticker -> %s (native=%s)", common, native)
        ticker_payload = {
            "method": "sub.ticker",
            "param": {"symbol": native},
            "gzip": False,
        }
        await _send_json_with_retry(ws, ticker_payload)
        await asyncio.sleep(WS_SUB_DELAY)
        # Subscribe to full order book depth (step0) for the symbol.
        logger.debug("MEXC subscribe depth -> %s", native)
        depth_payload = {
            "method": "sub.depth",
            "param": {"symbol": native},
            "gzip": False,
        }
        await _send_json_with_retry(ws, depth_payload)
        await asyncio.sleep(WS_SUB_DELAY)
        # Subscribe to funding rate updates.
        logger.debug("MEXC subscribe funding -> %s", native)
        funding_payload = {
            "method": "sub.funding.rate",
            "param": {"symbol": native},
            "gzip": False,
        }
        await _send_json_with_retry(ws, funding_payload)
        await asyncio.sleep(WS_SUB_DELAY)


async def _send_json_with_retry(ws, payload: dict) -> None:
    try:
        await ws.send(json.dumps(payload))
        logger.debug("MEXC WS send -> %s", json.dumps(payload)[:200])
    except Exception:
        logger.exception("Failed to send MEXC subscription", extra={"payload": payload})


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


def _is_mexc_ack(message: dict) -> bool:
    if not isinstance(message, dict):
        return False
    channel = message.get("channel")
    if isinstance(channel, str) and channel in {
        "rs.sub",
        "rs.unsub",
        "rs.ping",
        "rs.pong",
        "rs.query",
    }:
        return True
    if message.get("method") in {"sub", "unsub"} and message.get("success") is True:
        return True
    if message.get("success") is True and not message.get("data"):
        return True
    return False


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


def _decode_ws_message(message: str | bytes) -> dict | None:
    if isinstance(message, str):
        raw = message
    elif isinstance(message, (bytes, bytearray)):
        raw = _decode_ws_bytes(bytes(message))
    else:
        return None
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _is_ping(message: dict) -> bool:
    if not isinstance(message, dict):
        return False
    if "ping" in message:
        return True
    method = message.get("method")
    if isinstance(method, str) and method.lower() == "ping":
        return True
    return False


async def _reply_pong(ws, message: dict) -> None:
    try:
        if "ping" in message:
            await ws.send(json.dumps({"pong": message["ping"]}))
        else:
            await ws.send(json.dumps({"id": message.get("id", 0), "method": "pong"}))
    except Exception:
        pass


def _extract_depth_message(message: dict) -> tuple[bool, dict, str] | None:
    if not isinstance(message, dict):
        return None
    # Legacy depth update (depth.update method).  Format: {"method":"depth.update", "params":[snapshot,data,symbol]}
    method = message.get("method")
    if isinstance(method, str) and method.lower() == "depth.update":
        params = message.get("params")
        if not isinstance(params, list) or len(params) < 3:
            return None
        snapshot = bool(params[0])
        data = params[1] if isinstance(params[1], dict) else {}
        symbol = params[2]
        if not isinstance(symbol, str):
            return None
        return snapshot, data, symbol
    # New format: channel contains "depth"
    channel = message.get("channel") or message.get("topic")
    if isinstance(channel, str) and "depth" in channel.lower():
        data = message.get("data")
        if isinstance(data, dict):
            symbol = message.get("symbol") or data.get("symbol")
            if isinstance(symbol, str):
                # Treat as snapshot for "push.depth"/"push.depth.step" messages
                return True, data, symbol
    return None


def _extract_last_price(data: dict) -> float | None:
    for key in ("lastPrice", "last", "close", "price", "markPrice"):
        val = data.get(key)
        if val is None:
            continue
        try:
            price = float(val)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return None


def _iter_levels(source) -> Iterable[Tuple[float, float]]:
    if isinstance(source, dict):
        source = source.get("levels") or source.get("data") or source.get("list") or []
    if not isinstance(source, (list, tuple)):
        return []
    result: List[Tuple[float, float]] = []
    for entry in source:
        price, size = _parse_level(entry)
        if price is None or size is None:
            continue
        result.append((price, size))
    return result


def _looks_like_depth(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("bids") or payload.get("asks"):
        return True
    if payload.get("b") or payload.get("a"):
        return True
    if payload.get("buy") or payload.get("sell"):
        return True
    return False


def _extract_funding_rate(payload: dict) -> float | None:
    if not isinstance(payload, dict):
        return None
    for key in ("fundingRate", "funding_rate", "rate", "value"):
        val = payload.get(key)
        if val is None:
            continue
        try:
            rate = float(val)
        except (TypeError, ValueError):
            continue
        return rate
    return None


def _iter_mexc_payloads(message) -> Iterable[Tuple[str, dict]]:
    if not isinstance(message, dict):
        return []
    default_symbol: str | None = None
    sym_candidate = message.get("symbol") or message.get("s")
    if isinstance(sym_candidate, str) and sym_candidate:
        default_symbol = sym_candidate
    params = message.get("params")
    if isinstance(params, list):
        for item in reversed(params):
            if isinstance(item, str) and item:
                default_symbol = item
                break
    payload_candidates = []
    for key in ("data", "tick", "ticker", "tickers", "result", "payload"):
        if key in message:
            payload_candidates.append(message[key])
    if isinstance(params, list):
        payload_candidates.append(params)
    if not payload_candidates:
        payload_candidates.append(message)
    seen: set[tuple[str, int]] = set()
    items: list[tuple[str, dict]] = []
    for candidate in payload_candidates:
        for symbol, payload in _iter_payload_items(candidate, default_symbol):
            key = (symbol, id(payload))
            if key in seen:
                continue
            seen.add(key)
            items.append((symbol, payload))
    return items


def _iter_payload_items(payload, default_symbol: str | None) -> Iterable[tuple[str, dict]]:
    if payload is None:
        return []
    items: list[tuple[str, dict]] = []
    if isinstance(payload, dict):
        dict_values = list(payload.values())
        if dict_values and all(isinstance(v, dict) for v in dict_values):
            for key, value in payload.items():
                if not isinstance(value, dict):
                    continue
                symbol = _extract_symbol(value, key, default_symbol)
                if symbol:
                    items.append((symbol, value))
        else:
            symbol = _extract_symbol(payload, None, default_symbol)
            if symbol:
                items.append((symbol, payload))
        return items
    if isinstance(payload, list):
        for value in payload:
            if not isinstance(value, dict):
                continue
            symbol = _extract_symbol(value, None, default_symbol)
            if symbol:
                items.append((symbol, value))
        return items
    return items


def _extract_symbol(payload: dict, fallback_key: str | None, default_symbol: str | None) -> str | None:
    for key in ("symbol", "s", "instId", "contract", "pair", "market"):
        val = payload.get(key)
        if isinstance(val, str) and val:
            return val
    if fallback_key:
        return fallback_key
    return default_symbol


def _parse_level(level) -> Tuple[float | None, float | None]:
    price = size = None
    if isinstance(level, dict):
        for key in ("price", "p", "px", "bp", "ap"):
            val = level.get(key)
            if val is None:
                continue
            try:
                price = float(val)
                break
            except Exception:
                continue
        for key in ("size", "qty", "q", "v", "volume"):
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
    if size is None:
        return price, 0.0
    if size < 0:
        size = 0.0
    return price, size


@dataclass
class _MexcOrderBookState:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    def snapshot(self, bids, asks) -> None:
        self.bids.clear()
        self.asks.clear()
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)
    def update(self, bids, asks) -> None:
        self._apply(self.bids, bids)
        self._apply(self.asks, asks)
    def top_levels(self, depth: int = 5) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:depth]
        asks_sorted = sorted(self.asks.items(), key=lambda kv: kv[0])[:depth]
        return bids_sorted, asks_sorted
    def _apply(self, side: Dict[float, float], updates) -> None:
        if not updates:
            return
        for price, size in _iter_levels(updates):
            if size <= 0:
                side.pop(price, None)
            else:
                side[price] = size
        if len(side) > 200:
            if side is self.asks:
                ordered = sorted(side.items(), key=lambda kv: kv[0])
            else:
                ordered = sorted(side.items(), key=lambda kv: kv[0], reverse=True)
            trimmed = dict(ordered[:200])
            side.clear()
            side.update(trimmed)


async def authenticate_ws(ws: Any, creds: ApiCreds | None) -> None:
    """Placeholder for future authenticated MEXC channels."""
    del ws, creds
    return None