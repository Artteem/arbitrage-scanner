from __future__ import annotations

import asyncio
import json
import time
from typing import Iterable, Sequence

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore

WS_URL = "wss://open-api.bingx.com/quote/ws"


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


def _to_bingx_symbol(symbol: Symbol) -> str:
    sym = str(symbol).upper()
    if "-" in sym:
        sym = sym.replace("-", "_")
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        base = sym[:-4]
        return f"{base}_USDT"
    return sym


def _from_bingx_symbol(symbol: str | None) -> Symbol | None:
    if not symbol:
        return None
    return symbol.replace("-", "").replace("_", "").upper()


async def run_bingx(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    if not symbols:
        return

    wanted_common = {str(sym).upper() for sym in symbols}
    wanted_exchange = {_to_bingx_symbol(sym) for sym in wanted_common}

    async for ws in _reconnect(WS_URL):
        try:
            await _subscribe(ws, sorted(wanted_exchange))

            async for raw_msg in ws:
                try:
                    message = json.loads(raw_msg)
                except Exception:
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
