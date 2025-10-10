from __future__ import annotations

import asyncio
import json
import logging
import time
import zlib
from typing import Dict, Iterable, List, Sequence, Tuple

import httpx
import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .bingx_utils import normalize_bingx_symbol
from .discovery import discover_bingx_usdt_perp
from .utils import pick_timestamp, now_ts, iter_ws_messages

TICKERS_URLS: tuple[str, ...] = (
    "https://bingx.com/api/v3/contract/tickers",
    "https://open-api.bingx.com/openApi/swap/v2/market/getLatest",
    "https://open-api.bingx.com/openApi/swap/v3/market/getLatest",
)
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://bingx.com/",
    "Origin": "https://bingx.com",
}
POLL_INTERVAL = 1.5
HTTP_RELAX_INTERVAL = 6.0

WS_ENDPOINTS = (
    "wss://open-api-ws.bingx.com/market",
    "wss://open-api-ws.bingx.com/market?compress=false",
    "wss://open-api-swap.bingx.com/swap-market",
    "wss://open-api-swap.bingx.com/swap-market?compress=false",
)
WS_SUB_CHUNK = 80
WS_SUB_DELAY = 0.05
MIN_SYMBOL_THRESHOLD = 5

logger = logging.getLogger(__name__)


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


def _build_param_candidates(wanted_exchange: set[str] | None) -> list[dict[str, str] | None]:
    candidates: list[dict[str, str] | None] = []

    def _add(candidate: dict[str, str] | None) -> None:
        if candidate not in candidates:
            candidates.append(candidate)

    if wanted_exchange:
        joined = ",".join(sorted(wanted_exchange))
        _add({"symbols": joined})
        _add({"symbol": joined})
    _add({"symbol": "ALL"})
    _add(None)
    return candidates


async def run_bingx(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    subscribe = list(dict.fromkeys(symbols))
    http_fetch_all = False

    if len(subscribe) < MIN_SYMBOL_THRESHOLD:
        try:
            discovered = await discover_bingx_usdt_perp()
        except Exception:
            discovered = set()
        if discovered:
            subscribe = sorted(discovered)
        else:
            http_fetch_all = True

    if not subscribe and not http_fetch_all:
        return

    ws_ready = asyncio.Event()
    tasks = [
        asyncio.create_task(_poll_bingx_http(store, subscribe, ws_ready, http_fetch_all)),
        asyncio.create_task(_run_bingx_ws_tickers(store, subscribe, ws_ready)),
    ]

    if subscribe:
        tasks.append(asyncio.create_task(_run_bingx_orderbooks(store, subscribe)))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except Exception:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def _run_bingx_ws_tickers(
    store: TickerStore,
    symbols: Sequence[Symbol],
    ready_event: asyncio.Event | None = None,
) -> None:
    wanted_common = {_normalize_common_symbol(sym) for sym in symbols if sym}
    if not wanted_common:
        return

    ws_symbols = sorted({_to_bingx_ws_symbol(sym) for sym in wanted_common})
    if not ws_symbols:
        return

    async for ws in _reconnect_ws():
        try:
            for i in range(0, len(ws_symbols), WS_SUB_CHUNK):
                batch = ws_symbols[i : i + WS_SUB_CHUNK]
                await _send_ws_subscriptions(ws, batch)
                await asyncio.sleep(WS_SUB_DELAY)

            async for raw_msg in iter_ws_messages(
                ws,
                ping_interval=6.0,
                max_idle=20.0,
                name="bingx:ticker",
            ):
                msg = _decode_ws_message(raw_msg)
                if msg is None:
                    continue

                if _is_ws_ping(msg):
                    await _reply_ws_ping(ws, msg)
                    continue

                for raw_symbol, payload in _iter_ws_payloads(msg):
                    if not payload:
                        continue

                    exchange_symbol = _normalize_ws_symbol(raw_symbol)
                    common_symbol = _from_bingx_symbol(exchange_symbol)
                    if not common_symbol or common_symbol not in wanted_common:
                        continue

                    received_at = now_ts()
                    event_ts = pick_timestamp(
                        payload.get("time"),
                        payload.get("ts"),
                        payload.get("timestamp"),
                        payload.get("updateTime"),
                        msg.get("ts"),
                        msg.get("time"),
                        msg.get("timestamp"),
                        default=received_at,
                    )

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
                            ts=received_at,
                            event_ts=event_ts,
                        )
                    )
                    if ready_event and not ready_event.is_set():
                        ready_event.set()
        except asyncio.CancelledError:
            raise
        except RuntimeError as exc:
            logger.warning("BingX ticker stream stalled: %s", exc)
            await asyncio.sleep(1.0)
        except Exception:
            logger.exception("BingX ticker stream error", exc_info=True)
            await asyncio.sleep(2.0)


async def _run_bingx_orderbooks(
    store: TickerStore,
    symbols: Sequence[Symbol],
) -> None:
    wanted_common = {_normalize_common_symbol(sym) for sym in symbols if sym}
    if not wanted_common:
        return

    ws_symbols = sorted({_to_bingx_ws_symbol(sym) for sym in wanted_common})
    if not ws_symbols:
        return

    async for ws in _reconnect_ws():
        try:
            for i in range(0, len(ws_symbols), WS_SUB_CHUNK):
                batch = ws_symbols[i : i + WS_SUB_CHUNK]
                await _send_ws_depth_subscriptions(ws, batch)
                await asyncio.sleep(WS_SUB_DELAY)

            async for raw_msg in iter_ws_messages(
                ws,
                ping_interval=6.0,
                max_idle=20.0,
                name="bingx:depth",
            ):
                msg = _decode_ws_message(raw_msg)
                if msg is None:
                    continue

                if _is_ws_ping(msg):
                    await _reply_ws_ping(ws, msg)
                    continue

                for raw_symbol, payload in _iter_ws_payloads(msg):
                    if not isinstance(payload, dict):
                        continue

                    exchange_symbol = _normalize_ws_symbol(raw_symbol)
                    common_symbol = _from_bingx_symbol(exchange_symbol)
                    if not common_symbol or common_symbol not in wanted_common:
                        continue

                    received_at = now_ts()
                    event_ts = pick_timestamp(
                        payload.get("time"),
                        payload.get("ts"),
                        payload.get("timestamp"),
                        payload.get("updateTime"),
                        msg.get("ts"),
                        msg.get("time"),
                        msg.get("timestamp"),
                        default=received_at,
                    )

                    bids, asks, last_price = _extract_depth_payload(payload)
                    if not bids and not asks and last_price is None:
                        continue

                    store.upsert_order_book(
                        "bingx",
                        common_symbol,
                        bids=bids or None,
                        asks=asks or None,
                        ts=received_at,
                        last_price=last_price,
                        last_price_ts=event_ts if last_price is not None else None,
                    )

                    if bids and asks:
                        store.upsert_ticker(
                            Ticker(
                                exchange="bingx",
                                symbol=common_symbol,
                                bid=bids[0][0],
                                ask=asks[0][0],
                                ts=received_at,
                                event_ts=event_ts,
                            )
                        )
        except asyncio.CancelledError:
            raise
        except RuntimeError as exc:
            logger.warning("BingX depth stream stalled: %s", exc)
            await asyncio.sleep(1.0)
        except Exception:
            logger.exception("BingX depth stream error", exc_info=True)
            await asyncio.sleep(2.0)


async def _reconnect_ws():
    idx = 0
    while True:
        endpoint = WS_ENDPOINTS[idx % len(WS_ENDPOINTS)]
        idx += 1
        try:
            async with websockets.connect(
                endpoint,
                ping_interval=None,
                close_timeout=5,
                max_queue=None,
            ) as ws:
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)


async def _send_ws_subscriptions(ws, symbols: Sequence[str]) -> None:
    if not symbols:
        return

    def _next_id() -> str:
        return str(int(time.time() * 1_000))

    payloads = [
        {
            "id": _next_id(),
            "action": "subscribe",
            "params": {"channel": "ticker", "instId": list(symbols)},
        },
        {
            "id": _next_id(),
            "action": "subscribe",
            "params": {"channel": "ticker", "symbols": list(symbols)},
        },
    ]

    market_topics = [f"market.{sym}.ticker" for sym in symbols]
    payloads.append({"id": _next_id(), "reqType": "sub", "dataType": market_topics})

    swap_topics = [f"swap/ticker:{sym}" for sym in symbols]
    payloads.append({"id": _next_id(), "reqType": "sub", "dataType": swap_topics})
    payloads.extend(
        {"id": _next_id(), "reqType": "sub", "dataType": topic}
        for topic in swap_topics
    )

    for payload in payloads:
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            continue
        await asyncio.sleep(WS_SUB_DELAY)


async def _send_ws_depth_subscriptions(ws, symbols: Sequence[str]) -> None:
    if not symbols:
        return

    def _next_id() -> str:
        return str(int(time.time() * 1_000))

    depth_topics = [f"market.{sym}.depth5" for sym in symbols]
    swap_depth_topics = [f"swap/depth:{sym}" for sym in symbols]
    swap_depth5_topics = [f"swap/depth5:{sym}" for sym in symbols]

    payloads = [
        {
            "id": _next_id(),
            "action": "subscribe",
            "params": {"channel": "depth5", "instId": list(symbols)},
        },
        {
            "id": _next_id(),
            "action": "subscribe",
            "params": {"channel": "depth", "instId": list(symbols)},
        },
        {"id": _next_id(), "reqType": "sub", "dataType": depth_topics},
        {"id": _next_id(), "reqType": "sub", "dataType": swap_depth_topics},
        {"id": _next_id(), "reqType": "sub", "dataType": swap_depth5_topics},
    ]

    for topic in depth_topics + swap_depth_topics + swap_depth5_topics:
        payloads.append({"id": _next_id(), "reqType": "sub", "dataType": topic})

    for payload in payloads:
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            continue
        await asyncio.sleep(WS_SUB_DELAY)


def _decode_ws_message(message: str | bytes) -> dict | None:
    raw: str | None
    if isinstance(message, (bytes, bytearray)):
        raw = None
        for decoder in (
            lambda b: zlib.decompress(b, wbits=-zlib.MAX_WBITS),
            lambda b: zlib.decompress(b),
            lambda b: b.decode("utf-8"),
        ):
            try:
                data = decoder(message)
                if isinstance(data, bytes):
                    raw = data.decode("utf-8")
                else:
                    raw = data
                break
            except Exception:
                continue
        if raw is None:
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


def _iter_ws_payloads(message: dict) -> Iterable[tuple[str, dict]]:
    if not isinstance(message, dict):
        return []

    action = message.get("action")
    if isinstance(action, str):
        normalized = action.strip().lower()
        # Сообщения о подписке/ошибке не содержат рыночных данных и могут
        # безболезненно игнорироваться. BingX в последнее время стал
        # использовать значения вроде "snapshot"/"update" для тикеров, так что
        # фильтруем только явные служебные статусы.
        if normalized in {"subscribe", "sub", "unsubscribe", "unsub", "error"}:
            return []

    payload = message.get("data")
    if payload is None:
        # На всякий случай поддерживаем альтернативные ключи, которые BingX
        # возвращает в ряде эндпоинтов.
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

    return list(_iter_payload_items(payload, default_symbol))


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


def _normalize_ws_symbol(symbol: str) -> str:
    sym = symbol.strip().upper()
    sym = sym.replace("/", "-")
    if "_" in sym and "-" not in sym:
        sym = sym.replace("_", "-")
    return sym


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


async def _poll_bingx_http(
    store: TickerStore,
    symbols: Sequence[Symbol],
    ws_ready: asyncio.Event | None = None,
    fetch_all: bool = False,
) -> None:
    wanted_common = {_normalize_common_symbol(sym) for sym in symbols if sym}
    if not wanted_common and not fetch_all:
        return

    wanted_exchange = None if fetch_all else {_to_bingx_symbol(sym) for sym in wanted_common}
    param_candidates = _build_param_candidates(wanted_exchange)
    params_idx = 0
    url_idx = 0

    async with httpx.AsyncClient(timeout=15, headers=REQUEST_HEADERS) as client:
        while True:
            batch_received_at = now_ts()
            params = param_candidates[params_idx]
            url = TICKERS_URLS[url_idx]
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPStatusError:
                params_idx = (params_idx + 1) % len(param_candidates)
                url_idx = (url_idx + 1) % len(TICKERS_URLS)
                await asyncio.sleep(1.5)
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                url_idx = (url_idx + 1) % len(TICKERS_URLS)
                await asyncio.sleep(2.0)
                continue

            items: Iterable[dict] = []
            if isinstance(data, dict):
                for key in ("data", "result", "tickers", "items"):
                    value = data.get(key)
                    if isinstance(value, list):
                        items = value
                        break
                    if isinstance(value, dict):
                        items = value.values()
                        break
                else:
                    items = list(data.values()) if isinstance(data, dict) else []
            elif isinstance(data, list):
                items = data

            for raw in items:
                if not isinstance(raw, dict):
                    continue

                raw_symbol = raw.get("symbol") or raw.get("market") or raw.get("instId")
                if not raw_symbol:
                    continue

                normalized_exchange_symbol = _to_bingx_symbol(raw_symbol)
                if wanted_exchange is not None and normalized_exchange_symbol not in wanted_exchange:
                    normalized_common = _from_bingx_symbol(raw_symbol)
                    if not normalized_common or normalized_common not in wanted_common:
                        continue
                    normalized_exchange_symbol = _to_bingx_symbol(normalized_common)

                bid = _extract_price(
                    raw,
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
                    raw,
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

                common_symbol = _from_bingx_symbol(normalized_exchange_symbol)
                if wanted_common and (not common_symbol or common_symbol not in wanted_common):
                    continue

                event_ts = pick_timestamp(
                    raw.get("timestamp"),
                    raw.get("ts"),
                    raw.get("time"),
                    raw.get("updateTime"),
                    default=batch_received_at,
                )

                store.upsert_ticker(
                    Ticker(
                        exchange="bingx",
                        symbol=common_symbol,
                        bid=bid,
                        ask=ask,
                        ts=batch_received_at,
                        event_ts=event_ts,
                    )
                )

            if ws_ready and ws_ready.is_set():
                await asyncio.sleep(HTTP_RELAX_INTERVAL)
            else:
                await asyncio.sleep(POLL_INTERVAL)
