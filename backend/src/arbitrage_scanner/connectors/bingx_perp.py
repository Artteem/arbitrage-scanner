from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Dict, Iterable, List, Sequence, Tuple

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .bingx_utils import normalize_bingx_symbol
from .discovery import discover_bingx_usdt_perp

logger = logging.getLogger(__name__)

WS_ENDPOINTS = (
    "wss://open-api-ws.bingx.com/market?compress=false",
    "wss://open-api-swap.bingx.com/swap-market?compress=false",
)
WS_SUB_CHUNK = 250
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
MIN_SYMBOL_THRESHOLD = 5
FALLBACK_SYMBOLS: tuple[Symbol, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
_SUBSCRIPTION_LOG_LIMIT = 20
_WS_PAYLOAD_LOG_LIMIT = 20
_LOG_ONCE_TICKERS: set[Symbol] = set()
_WS_PAYLOAD_LOG_COUNT = 0


def _log_ws_subscriptions(kind: str, topics: Sequence[str]) -> None:
    if not topics:
        return

    preview = list(topics[:_SUBSCRIPTION_LOG_LIMIT])
    extra = len(topics) - len(preview)
    if extra > 0:
        logger.info(
            "BingX WS %s subscriptions: %s (+%d more)",
            kind,
            preview,
            extra,
        )
    else:
        logger.info("BingX WS %s subscriptions: %s", kind, preview)


def _log_ws_payload_received(symbol: Symbol) -> None:
    global _WS_PAYLOAD_LOG_COUNT
    if _WS_PAYLOAD_LOG_COUNT >= _WS_PAYLOAD_LOG_LIMIT:
        return
    _WS_PAYLOAD_LOG_COUNT += 1
    logger.info("BingX WS payload received for %s", symbol)


def _log_first_ticker(symbol: Symbol, bid: float, ask: float) -> None:
    target = symbol.upper()
    if target not in {"BTCUSDT", "ETHUSDT"}:
        return
    if target in _LOG_ONCE_TICKERS:
        return
    _LOG_ONCE_TICKERS.add(target)
    logger.info("BingX first ticker for %s: bid=%s ask=%s", target, bid, ask)


def _extract_price(item: dict, keys: Iterable[str]) -> float:
    for key in keys:
        val = item.get(key)
        if val is None:
            continue
        try:
            price = float(val)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return 0.0


def _normalize_common_symbol(symbol: Symbol) -> str:
    normalized = normalize_bingx_symbol(symbol)
    if normalized:
        return normalized
    sym = str(symbol).upper()
    return sym.replace("-", "").replace("_", "")


def _collect_wanted_common(symbols: Sequence[Symbol]) -> set[str]:
    wanted: set[str] = set()
    for symbol in symbols:
        if not symbol:
            continue
        normalized = normalize_bingx_symbol(symbol)
        if normalized:
            wanted.add(str(normalized))
            continue
        fallback = _normalize_common_symbol(str(symbol))
        if fallback:
            wanted.add(fallback)
    return wanted


def _chunk_bingx_symbols(symbols: Sequence[Symbol], size: int) -> Iterable[Sequence[Symbol]]:
    for idx in range(0, len(symbols), size):
        yield symbols[idx : idx + size]


def _to_bingx_symbol(symbol: Symbol) -> str:
    sym = str(symbol).upper()
    if "-" in sym:
        sym = sym.replace("-", "_")
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        base = sym[:-4]
        return f"{base}_USDT"
    if sym.endswith("USDC"):
        base = sym[:-4]
        return f"{base}_USDC"
    if sym.endswith("USD"):
        base = sym[:-3]
        return f"{base}_USD"
    return sym


def _to_bingx_ws_symbol(symbol: Symbol) -> str:
    sym = _normalize_common_symbol(symbol)
    for quote in ("USDT", "USDC", "USD", "BUSD", "FDUSD"):
        if sym.endswith(quote):
            base = sym[: -len(quote)]
            return f"{base}-{quote}"
    return sym


def _from_bingx_symbol(symbol: str | None) -> Symbol | None:
    return normalize_bingx_symbol(symbol)


async def run_bingx(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    subscribe = [sym for sym in dict.fromkeys(symbols) if sym]
    subscribe_all = False

    if len(subscribe) < MIN_SYMBOL_THRESHOLD:
        try:
            discovered = await discover_bingx_usdt_perp()
        except Exception:
            discovered = set()
        if discovered:
            subscribe = sorted(discovered)
        else:
            subscribe = list(FALLBACK_SYMBOLS)
            subscribe_all = True

    if not subscribe and not subscribe_all:
        return

    chunks = [tuple(chunk) for chunk in _chunk_bingx_symbols(subscribe, WS_SUB_CHUNK)]

    while True:
        tasks: list[asyncio.Task] = []

        if subscribe_all:
            tasks.append(asyncio.create_task(_run_bingx_all_tickers(store, subscribe)))

        for chunk in chunks:
            tasks.append(asyncio.create_task(_run_bingx_ws(store, chunk)))

        if not tasks:
            return

        try:
            await asyncio.gather(*tasks)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("BingX websocket workers crashed; restarting")
            await asyncio.sleep(WS_RECONNECT_INITIAL)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def _run_bingx_ws(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    wanted_common = _collect_wanted_common(symbols)
    if not wanted_common:
        return

    ws_symbols = sorted({_to_bingx_ws_symbol(sym) for sym in wanted_common if sym})
    if not ws_symbols:
        return

    async for ws in _reconnect_ws():
        try:
            await _send_ws_subscriptions(ws, ws_symbols)
            await _send_ws_depth_subscriptions(ws, ws_symbols)
            await _send_ws_funding_subscriptions(ws, ws_symbols)

            async for raw_msg in ws:
                msg = _decode_ws_message(raw_msg)
                if msg is None:
                    continue

                if _is_ws_ping(msg):
                    await _reply_ws_ping(ws, msg)
                    continue

                data_type = str(msg.get("dataType") or "").lower()
                now = time.time()

                for common_symbol, payload in _iter_ws_payloads(msg, wanted_common):
                    if not payload:
                        continue

                    if "fundingrate" in data_type:
                        rate = _extract_price(
                            payload,
                            (
                                "fundingRate",
                                "funding_rate",
                                "rate",
                                "value",
                            ),
                        )
                        interval = _parse_funding_interval(payload)
                        store.upsert_funding(
                            "bingx", common_symbol, rate=rate, interval=interval, ts=now
                        )
                        continue

                    if "depth" in data_type or (
                        isinstance(payload, dict)
                        and (payload.get("bids") or payload.get("asks"))
                    ):
                        bids, asks, last_price = _extract_depth_payload(payload)
                        if not bids and not asks and last_price is None:
                            continue
                        store.upsert_order_book(
                            "bingx",
                            common_symbol,
                            bids=bids or None,
                            asks=asks or None,
                            ts=now,
                            last_price=last_price,
                        )
                        continue

                    bid = _extract_price(
                        payload,
                        (
                            "bestBid",
                            "bestBidPrice",
                            "bid",
                            "bidPrice",
                            "bid1",
                            "bid1Price",
                            "bp",
                            "bidPx",
                            "bestBidPx",
                            "b",
                            "buyPrice",
                        ),
                    )
                    ask = _extract_price(
                        payload,
                        (
                            "bestAsk",
                            "bestAskPrice",
                            "ask",
                            "askPrice",
                            "ask1",
                            "ask1Price",
                            "ap",
                            "askPx",
                            "bestAskPx",
                            "a",
                            "sellPrice",
                        ),
                    )

                    if bid <= 0 or ask <= 0:
                        continue

                    store.upsert_ticker(
                        Ticker(
                            exchange="bingx",
                            symbol=common_symbol,
                            bid=bid,
                            ask=ask,
                            ts=now,
                        )
                    )
                    _log_first_ticker(common_symbol, bid, ask)

                    last_price = _extract_price(
                        payload,
                        (
                            "lastPrice",
                            "last",
                            "close",
                            "px",
                        ),
                    )
                    if last_price > 0:
                        store.upsert_order_book(
                            "bingx",
                            common_symbol,
                            last_price=last_price,
                            last_price_ts=now,
                        )
        except asyncio.CancelledError:
            raise
        except Exception:
            continue


async def _run_bingx_all_tickers(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    wanted_common = _collect_wanted_common(symbols)
    ws_symbols = ["ALL"]

    async for ws in _reconnect_ws():
        try:
            await _send_ws_subscriptions(ws, ws_symbols)

            async for raw_msg in ws:
                msg = _decode_ws_message(raw_msg)
                if msg is None:
                    continue

                if _is_ws_ping(msg):
                    await _reply_ws_ping(ws, msg)
                    continue

                now = time.time()
                for common_symbol, payload in _iter_ws_payloads(msg, wanted_common or None):
                    if not payload:
                        continue

                    bid = _extract_price(
                        payload,
                        (
                            "bestBid",
                            "bestBidPrice",
                            "bid",
                            "bidPrice",
                            "bid1",
                            "bid1Price",
                            "bp",
                            "bidPx",
                            "bestBidPx",
                            "b",
                            "buyPrice",
                        ),
                    )
                    ask = _extract_price(
                        payload,
                        (
                            "bestAsk",
                            "bestAskPrice",
                            "ask",
                            "askPrice",
                            "ask1",
                            "ask1Price",
                            "ap",
                            "askPx",
                            "bestAskPx",
                            "a",
                            "sellPrice",
                        ),
                    )

                    if bid <= 0 or ask <= 0:
                        continue

                    store.upsert_ticker(
                        Ticker(
                            exchange="bingx",
                            symbol=common_symbol,
                            bid=bid,
                            ask=ask,
                            ts=now,
                        )
                    )
                    _log_first_ticker(common_symbol, bid, ask)
        except asyncio.CancelledError:
            raise
        except Exception:
            continue


async def _reconnect_ws():
    idx = 0
    delay = WS_RECONNECT_INITIAL
    while True:
        endpoint = WS_ENDPOINTS[idx % len(WS_ENDPOINTS)]
        idx += 1
        try:
            async with websockets.connect(
                endpoint,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                compression=None,
            ) as ws:
                delay = WS_RECONNECT_INITIAL
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_MAX)


async def _send_ws_subscriptions(ws, symbols: Sequence[str]) -> None:
    if not symbols:
        return

    def _next_id() -> str:
        return str(int(time.time() * 1_000))
    topics = []
    for sym in symbols:
        normalized = sym if sym.upper() == "ALL" else sym.replace("_", "-")
        topics.append(f"swap/ticker:{normalized}")

    payload = {
        "id": _next_id(),
        "reqType": "sub",
        "dataType": topics,
    }

    _log_ws_subscriptions("ticker", topics)

    try:
        await ws.send(json.dumps(payload))
    except Exception:
        return


async def _send_ws_depth_subscriptions(ws, symbols: Sequence[str]) -> None:
    if not symbols:
        return

    def _next_id() -> str:
        return str(int(time.time() * 1_000))

    topics = [f"swap/depth5:{sym}" for sym in symbols if sym.upper() != "ALL"]
    if not topics:
        return

    payload = {
        "id": _next_id(),
        "reqType": "sub",
        "dataType": topics,
    }

    _log_ws_subscriptions("depth", topics)

    try:
        await ws.send(json.dumps(payload))
    except Exception:
        return


async def _send_ws_funding_subscriptions(ws, symbols: Sequence[str]) -> None:
    if not symbols:
        return

    def _next_id() -> str:
        return str(int(time.time() * 1_000))

    topics = [f"swap/fundingRate:{sym}" for sym in symbols if sym.upper() != "ALL"]
    if not topics:
        return

    payload = {
        "id": _next_id(),
        "reqType": "sub",
        "dataType": topics,
    }

    _log_ws_subscriptions("funding", topics)

    try:
        await ws.send(json.dumps(payload))
    except Exception:
        return


def _decode_ws_message(message: str | bytes) -> dict | None:
    if isinstance(message, (bytes, bytearray)):
        try:
            raw = message.decode("utf-8", errors="ignore")
        except Exception:
            return None
    else:
        raw = message

    try:
        return json.loads(raw)
    except Exception:
        return None


def _is_ws_ping(message: dict) -> bool:
    if not isinstance(message, dict):
        return False
    if "ping" in message:
        return True
    action = message.get("action")
    if isinstance(action, str) and action.lower() == "ping":
        return True
    return False


async def _reply_ws_ping(ws, message: dict) -> None:
    try:
        if "ping" in message:
            await ws.send(json.dumps({"pong": message["ping"]}))
        else:
            await ws.send(json.dumps({"id": message.get("id", "pong"), "action": "pong"}))
    except Exception:
        pass


def _iter_ws_payloads(
    message: dict,
    wanted_common: set[str] | None = None,
) -> Iterable[tuple[str, dict]]:
    if not isinstance(message, dict):
        logger.debug(
            "BingX WS message ignored: unexpected type %s", type(message).__name__
        )
        return []

    action = message.get("action")
    if isinstance(action, str):
        normalized = action.strip().lower()
        if normalized in {"subscribe", "sub", "unsubscribe", "unsub", "error"}:
            return []

    payload = message.get("data")
    if payload is None:
        for key in ("tickers", "items", "result"):
            cand = message.get(key)
            if cand is not None:
                payload = cand
                break
    default_symbol: str | None = None

    arg = message.get("arg")
    if isinstance(arg, dict):
        candidate = arg.get("instId") or arg.get("symbol") or arg.get("symbols")
        if isinstance(candidate, list):
            candidate = candidate[0] if candidate else None
        if isinstance(candidate, str):
            default_symbol = candidate

    topic_symbol = _extract_topic_symbol(message.get("dataType"))
    if topic_symbol:
        default_symbol = topic_symbol

    accepted: list[tuple[str, dict]] = []
    for raw_symbol, payload_item in _iter_payload_items(payload, default_symbol):
        if not isinstance(payload_item, dict):
            logger.debug(
                "BingX WS drop %r: payload is not a dict (got %s)",
                raw_symbol,
                type(payload_item).__name__,
            )
            continue

        if not raw_symbol:
            logger.debug(
                "BingX WS drop payload without symbol: keys=%s",
                list(payload_item.keys())[:5],
            )
            continue

        common_symbol = normalize_bingx_symbol(raw_symbol)
        if not common_symbol:
            logger.debug("BingX WS drop %r: unable to normalize symbol", raw_symbol)
            continue

        normalized_common = str(common_symbol)
        if wanted_common and normalized_common not in wanted_common:
            logger.debug(
                "BingX WS drop %s: symbol not requested", normalized_common
            )
            continue

        _log_ws_payload_received(normalized_common)
        accepted.append((normalized_common, payload_item))

    return accepted


def _extract_depth_payload(payload: dict) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]], float | None]:
    if not isinstance(payload, dict):
        return [], [], None

    containers: List[dict] = [payload]
    for key in ("depth", "depths", "orderbook", "book", "tick", "snapshot", "data"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            containers.append(nested)

    bids: List[Tuple[float, float]] = []
    asks: List[Tuple[float, float]] = []
    last_price: float | None = None

    for container in containers:
        if not isinstance(container, dict):
            continue

        bids_candidate = _collect_depth_levels(
            container,
            (
                "bids",
                "bid",
                "buy",
                "buys",
                "buyDepth",
                "buyLevels",
                "buyList",
                "bp",
            ),
        )
        asks_candidate = _collect_depth_levels(
            container,
            (
                "asks",
                "ask",
                "sell",
                "sells",
                "sellDepth",
                "sellLevels",
                "sellList",
                "ap",
            ),
        )

        if bids_candidate:
            bids = bids_candidate
        if asks_candidate:
            asks = asks_candidate
        if last_price is None:
            last_price = _extract_last_price(container)

    return bids[:20], asks[:20], last_price


def _collect_depth_levels(container: dict, keys: Sequence[str]) -> List[Tuple[float, float]]:
    for key in keys:
        if key not in container:
            continue
        levels = container.get(key)
        parsed = list(_iter_levels(levels))
        if parsed:
            return parsed
    return []


def _extract_last_price(container: dict) -> float | None:
    for key in (
        "lastPrice",
        "last_price",
        "last",
        "price",
        "close",
        "tradePrice",
        "markPrice",
    ):
        val = container.get(key)
        if val is None:
            continue
        try:
            price = float(val)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return None


def _parse_funding_interval(payload: dict) -> str:
    interval = payload.get("interval") or payload.get("fundingInterval")
    if isinstance(interval, (int, float)) and interval > 0:
        return f"{interval}h"
    if isinstance(interval, str) and interval:
        return interval
    return "8h"


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

    if not isinstance(payload, list):
        return []

    for value in payload:
        if not isinstance(value, dict):
            continue
        symbol = _extract_symbol(value, None, default_symbol)
        if symbol:
            items.append((symbol, value))

    return items


def _extract_symbol(payload: dict, fallback_key: str | None, default_symbol: str | None) -> str | None:
    for key in ("symbol", "instId", "s", "market", "pair"):
        val = payload.get(key)
        if isinstance(val, str) and val:
            return val

    if fallback_key:
        return fallback_key

    return default_symbol


def _extract_topic_symbol(data_type) -> str | None:
    if isinstance(data_type, (list, tuple, set)):
        for item in data_type:
            symbol = _extract_topic_symbol(item)
            if symbol:
                return symbol
        return None

    if isinstance(data_type, str) and data_type:
        segments: list[str] = []
        if ":" in data_type:
            segments.extend(part for part in data_type.split(":") if part)
        if not segments:
            segments = [data_type]

        for segment in segments:
            candidate = segment.strip()
            if not candidate:
                continue
            if "/" in candidate:
                candidate = candidate.split("/", maxsplit=1)[-1]
            if candidate.lower().startswith("swap/ticker") and ":" in candidate:
                candidate = candidate.split(":", maxsplit=1)[-1]
            if candidate.lower().startswith("ticker."):
                candidate = candidate.split(".", maxsplit=1)[-1]
            if "." in candidate and "-" in candidate.split(".")[-1]:
                candidate = candidate.split(".")[-1]
            candidate = candidate.replace("_", "-")
            if "-" in candidate:
                return candidate.upper()
    return None



