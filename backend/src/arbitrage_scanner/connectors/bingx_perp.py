from __future__ import annotations

import asyncio
import json
import time
import zlib
from typing import Iterable, Sequence

import httpx

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore

WS_URL = "wss://open-api.bingx.com/quote/ws"

WS_ENDPOINTS = (
    "wss://open-api-ws.bingx.com/market",
    "wss://open-api-ws.bingx.com/market?compress=false",
)
WS_SUB_CHUNK = 80
WS_SUB_DELAY = 0.05


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
    bids = item.get("bids")
    if isinstance(bids, list) and bids:
        first = bids[0]
        if isinstance(first, (list, tuple)) and first:
            try:
                price = float(first[0])
            except (TypeError, ValueError):
                price = 0.0
            if price > 0:
                return price
    asks = item.get("asks")
    if isinstance(asks, list) and asks:
        first = asks[0]
        if isinstance(first, (list, tuple)) and first:
            try:
                price = float(first[0])
            except (TypeError, ValueError):
                price = 0.0
            if price > 0:
                return price
    return 0.0


def _normalize_common_symbol(symbol: Symbol) -> str:
    sym = str(symbol).upper()
    normalized = _from_bingx_symbol(sym)
    if normalized:
        return normalized
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
    if not symbol:
        return None
    return symbol.replace("-", "").replace("_", "").upper()


async def run_bingx(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    if not symbols:
        return

    try:
        await _run_bingx_ws(store, symbols)
    except asyncio.CancelledError:
        raise
    except Exception:
        await _poll_bingx_http(store, symbols)


async def _run_bingx_ws(store: TickerStore, symbols: Sequence[Symbol]) -> None:
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

            async for raw_msg in ws:
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
                            ts=time.time(),
                        )
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)



async def _reconnect_ws():
    idx = 0
    while True:
        endpoint = WS_ENDPOINTS[idx % len(WS_ENDPOINTS)]
        idx += 1
        try:
            async with websockets.connect(
                endpoint, ping_interval=20, ping_timeout=20, close_timeout=5
            ) as ws:
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)


async def _send_ws_subscriptions(ws, symbols: Sequence[str]) -> None:
    if not symbols:
        return

    payloads = [
        {
            "id": str(int(time.time() * 1_000)),
            "action": "subscribe",
            "params": {"channel": "ticker", "instId": list(symbols)},
        },
        {
            "id": str(int(time.time() * 1_000)),
            "action": "subscribe",
            "params": {"channel": "ticker", "symbols": list(symbols)},
        },
    ]

    market_topics = [f"market.{sym}.ticker" for sym in symbols]
    payloads.append({"id": str(int(time.time() * 1_000)), "reqType": "sub", "dataType": market_topics})

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
    if isinstance(action, str) and action.lower() != "push":
        return []

    payload = message.get("data")
    default_symbol: str | None = None

    arg = message.get("arg")
    if isinstance(arg, dict):
        candidate = arg.get("instId") or arg.get("symbol") or arg.get("symbols")
        if isinstance(candidate, list):
            candidate = candidate[0] if candidate else None
        if isinstance(candidate, str):
            default_symbol = candidate

    return list(_iter_payload_items(payload, default_symbol))


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
    for key in ("symbol", "instId", "s", "market"):
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


async def _poll_bingx_http(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    wanted_common = {_normalize_common_symbol(sym) for sym in symbols if sym}
    if not wanted_common:
        return

    wanted_exchange = {_to_bingx_symbol(sym) for sym in wanted_common}
    param_candidates = _build_param_candidates(wanted_exchange)
    params_idx = 0

    async with httpx.AsyncClient(timeout=15, headers=REQUEST_HEADERS) as client:
        while True:
            now = time.time()
            params = param_candidates[params_idx]
            try:
                response = await client.get(TICKERS_URL, params=params)
                response.raise_for_status()
                data = response.json()
            except httpx.HTTPStatusError:
                params_idx = (params_idx + 1) % len(param_candidates)
                await asyncio.sleep(1.5)
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
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

                if await _handle_control_message(ws, message):
                    continue

                timestamp = time.time()
                for payload in _iter_ticker_payloads(message):
                    if not isinstance(payload, dict):
                        continue

                    raw_symbol = _extract_symbol_from_payload(payload, message)
                    if not raw_symbol:
                        continue

                    normalized_exchange_symbol = _to_bingx_symbol(raw_symbol)
                    if normalized_exchange_symbol not in wanted_exchange:
                        normalized_common = _from_bingx_symbol(raw_symbol)
                        if not normalized_common or normalized_common not in wanted_common:
                            continue
                        normalized_exchange_symbol = _to_bingx_symbol(normalized_common)

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
                        ),
                    )
                    if bid <= 0:
                        bid = _extract_price(payload, ("bidPrice1", "b"))

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
                        ),
                    )
                    if ask <= 0:
                        ask = _extract_price(payload, ("askPrice1", "a"))

                    if bid <= 0 or ask <= 0:
                        continue

                    common_symbol = _from_bingx_symbol(normalized_exchange_symbol)
                    if not common_symbol or common_symbol not in wanted_common:
                        continue

                    store.upsert_ticker(
                        Ticker(
                            exchange="bingx",
                            symbol=common_symbol,
                            bid=bid,
                            ask=ask,
                            ts=timestamp,
                        )
                    )
        except Exception:
            await asyncio.sleep(1.5)


async def _subscribe(ws: websockets.WebSocketClientProtocol, symbols: Sequence[str]) -> None:
    if not symbols:
        return

    for symbol in symbols:
        payload = {
            "event": "sub",
            "params": {
                "channel": "perpetual",
                "binary": False,
                "instId": symbol,
            },
        }
        await ws.send(json.dumps(payload))
        await asyncio.sleep(0.05)


async def _handle_control_message(
    ws: websockets.WebSocketClientProtocol, message: object
) -> bool:
    if not isinstance(message, dict):
        return False

    if "ping" in message:
        pong_payload: dict[str, object] = {"pong": message.get("ping")}
        await ws.send(json.dumps(pong_payload))
        return True

    event = str(message.get("event") or "").lower()
    if event in {"ping"}:
        reply = {"event": "pong"}
        if "ts" in message:
            reply["ts"] = message["ts"]
        await ws.send(json.dumps(reply))
        return True

    if event in {"sub", "subscribe", "pong"}:
        return True

    return False


def _iter_ticker_payloads(message: object) -> Iterable[dict]:
    if isinstance(message, dict):
        data = message.get("data")
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item
        elif isinstance(data, dict):
            yield data
        elif isinstance(message.get("tickers"), list):
            for item in message["tickers"]:
                if isinstance(item, dict):
                    yield item
        elif isinstance(message.get("items"), list):
            for item in message["items"]:
                if isinstance(item, dict):
                    yield item
        else:
            for value in message.values():
                if isinstance(value, dict):
                    yield value
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            yield item
    elif isinstance(message, list):
        for item in message:
            if isinstance(item, dict):
                yield item


def _extract_symbol_from_payload(payload: dict, envelope: object) -> str | None:
    candidates = [
        payload.get("symbol"),
        payload.get("instId"),
        payload.get("s"),
        payload.get("market"),
    ]
    if isinstance(envelope, dict):
        candidates.extend(
            [
                envelope.get("symbol"),
                envelope.get("instId"),
                envelope.get("s"),
                envelope.get("market"),
            ]
        )
        topic = envelope.get("topic") or envelope.get("channel")
        if isinstance(topic, str):
            for sep in ("/", ":", "."):
                if sep in topic:
                    part = topic.split(sep)[-1]
                    if part:
                        candidates.append(part)
    for candidate in candidates:
        if isinstance(candidate, str) and candidate:
            return candidate
    return None


async def _reconnect(url: str):
    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:
                yield ws
        except Exception:
            await asyncio.sleep(2.0)
