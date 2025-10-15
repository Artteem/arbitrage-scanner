from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Sequence, Tuple

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .discovery import discover_mexc_usdt_perp

WS_ENDPOINT = "wss://contract.mexc.com/ws"
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
WS_DEPTH_LEVELS = 50
MIN_SYMBOL_THRESHOLD = 1

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
        logger.exception("Failed to discover MEXC symbols")
        return []

    if not discovered:
        logger.error("MEXC discovery returned no USDT perpetual symbols; connector disabled")
        return []

    discovered_normalized: set[str] = set()
    for sym in discovered:
        normalized = _normalize_common_symbol(sym)
        if normalized:
            discovered_normalized.add(normalized)
    if not discovered_normalized:
        logger.error("MEXC discovery produced no usable symbols; connector disabled")
        return []

    if len(requested) < MIN_SYMBOL_THRESHOLD:
        requested = sorted(discovered_normalized)

    filtered = [symbol for symbol in requested if symbol in discovered_normalized]

    if not filtered:
        logger.warning(
            "Requested symbols are unavailable on MEXC; nothing to subscribe",
            extra={"requested": requested},
        )
        return []

    return filtered


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_mexc_symbol(symbol: Symbol) -> str:
    sym = str(symbol)
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
    subscribe = await _resolve_mexc_symbols(symbols)
    if not subscribe:
        logger.warning("No symbols resolved for MEXC connector; skipping startup")
        return

    while True:
        task = asyncio.create_task(_run_mexc_ws(store, tuple(subscribe)))

        try:
            await task
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("MEXC websocket worker crashed; restarting")
            await asyncio.sleep(WS_RECONNECT_INITIAL)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def _run_mexc_ws(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    wanted_exchange = [_to_mexc_symbol(sym) for sym in symbols if sym]
    wanted_exchange = [sym for sym in wanted_exchange if sym]
    if not wanted_exchange:
        return

    wanted_common = {_from_mexc_symbol(sym) for sym in wanted_exchange if sym}
    wanted_common = {sym for sym in wanted_common if sym}
    if not wanted_common:
        return

    books: Dict[str, _MexcOrderBookState] = {}

    async for ws in _reconnect_ws():
        try:
            await _send_mexc_subscriptions(ws, wanted_exchange)

            async for raw in ws:
                msg = _decode_ws_message(raw)
                if msg is None:
                    continue

                if _is_ping(msg):
                    await _reply_pong(ws, msg)
                    continue

                depth_payload = _extract_depth_message(msg)
                if depth_payload:
                    snapshot, data, sym_raw = depth_payload
                    if not isinstance(data, dict):
                        continue
                    sym_common = _from_mexc_symbol(sym_raw)
                    if not sym_common or sym_common not in wanted_common:
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

                now = time.time()
                for sym_raw, payload in _iter_mexc_payloads(msg):
                    if not isinstance(payload, dict):
                        continue

                    sym_common = _from_mexc_symbol(sym_raw)
                    if not sym_common or sym_common not in wanted_common:
                        continue

                    if _looks_like_depth(payload):
                        continue

                    rate = _extract_funding_rate(payload)
                    if rate is not None:
                        interval = _parse_interval(payload)
                        store.upsert_funding("mexc", sym_common, rate=rate, interval=interval, ts=now)
                        continue

                    bid = _extract_bid(payload)
                    ask = _extract_ask(payload)
                    if bid <= 0 or ask <= 0:
                        continue

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


async def _reconnect_ws():
    delay = WS_RECONNECT_INITIAL
    while True:
        try:
            async with websockets.connect(
                WS_ENDPOINT,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                extra_headers=MEXC_HEADERS,
            ) as ws:
                delay = WS_RECONNECT_INITIAL
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


async def _send_mexc_subscriptions(ws, symbols: Sequence[str]) -> None:
    if not symbols:
        return

    base_id = int(time.time() * 1_000)
    payloads: list[dict] = []
    req_id = base_id

    for method, extra in (
        ("sub.ticker", None),
        ("sub.depth", WS_DEPTH_LEVELS),
        ("sub.funding_rate", None),
    ):
        for sym in symbols:
            params: list[object] = [sym]
            if extra is not None:
                params.append(extra)
            payloads.append({"method": method, "params": params, "id": req_id})
            req_id += 1

    if not payloads:
        return

    message = json.dumps(payloads)
    try:
        await ws.send(message)
    except Exception:
        logger.exception("Failed to send batched MEXC subscriptions")


def _decode_ws_message(message: str | bytes) -> dict | None:
    if isinstance(message, str):
        raw = message
    elif isinstance(message, (bytes, bytearray)):
        raw = bytes(message).decode("utf-8", errors="ignore")
    else:
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

    channel = message.get("channel") or message.get("topic")
    if isinstance(channel, str) and "depth" in channel.lower():
        data = message.get("data")
        if isinstance(data, dict):
            symbol = message.get("symbol") or data.get("symbol")
            if isinstance(symbol, str):
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

