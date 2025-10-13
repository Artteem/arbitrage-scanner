from __future__ import annotations

import asyncio
import json
import time
import logging
from typing import Awaitable, Callable, Dict, Sequence

import websockets

from ..domain import Ticker, Symbol
from ..store import TickerStore
from .discovery import discover_binance_usdt_perp
from .utils import pick_timestamp, now_ts, iter_ws_messages

WS_ENDPOINT = "wss://fstream.binance.com/stream"

MIN_SYMBOL_THRESHOLD = 5

def _stream_name_book(sym: str) -> str:
    return f"{sym.lower()}@bookTicker"

def _stream_name_mark(sym: str) -> str:
    return f"{sym.lower()}@markPrice@1s"

def _stream_name_depth(sym: str) -> str:
    return f"{sym.lower()}@depth5@100ms"

def _stream_name_trade(sym: str) -> str:
    return f"{sym.lower()}@aggTrade"

CHUNK = 250  # безопасный размер пакета подписки

logger = logging.getLogger(__name__)

async def run_binance(store: TickerStore, symbols: Sequence[Symbol]):
    subscribe = list(dict.fromkeys(symbols))  # сохраняем порядок без дубликатов
    if len(subscribe) < MIN_SYMBOL_THRESHOLD:
        try:
            discovered = await discover_binance_usdt_perp()
        except Exception:
            discovered = set()
        if discovered:
            subscribe = sorted(discovered)

    if not subscribe:
        return

    async def _handle_book(d):
        # d: { s, b, a, ... }
        s = d.get("s"); b = d.get("b"); a = d.get("a")
        if s and b and a:
            received_at = now_ts()
            event_ts = pick_timestamp(
                d.get("E"),
                d.get("T"),
                d.get("eventTime"),
                default=received_at,
            )
            store.upsert_ticker(
                Ticker(
                    exchange="binance",
                    symbol=s,
                    bid=float(b),
                    ask=float(a),
                    ts=received_at,
                    event_ts=event_ts,
                )
            )

    async def _handle_depth(d):
        s = d.get("s") or d.get("symbol")
        bids = d.get("bids") or d.get("b") or []
        asks = d.get("asks") or d.get("a") or []
        if not s:
            return
        received_at = now_ts()
        event_ts = pick_timestamp(
            d.get("E"), d.get("T"), d.get("eventTime"), default=received_at
        )
        def _convert(levels):
            result = []
            for level in levels[:5]:
                if isinstance(level, (list, tuple)) and len(level) >= 2:
                    try:
                        price = float(level[0])
                        size = float(level[1])
                    except (TypeError, ValueError):
                        continue
                    if price > 0 and size > 0:
                        result.append((price, size))
            return result
        bids_conv = _convert(bids)
        asks_conv = _convert(asks)
        if bids_conv or asks_conv:
            store.upsert_order_book(
                "binance",
                s,
                bids=bids_conv or None,
                asks=asks_conv or None,
                ts=received_at,
            )
        if bids_conv and asks_conv:
            store.upsert_ticker(
                Ticker(
                    exchange="binance",
                    symbol=s,
                    bid=bids_conv[0][0],
                    ask=asks_conv[0][0],
                    ts=received_at,
                    event_ts=event_ts,
                )
            )

    async def _handle_mark(d):
        # d: { s, r, ... }
        s = d.get("s"); r = d.get("r")
        if s is not None and r is not None:
            try:
                rate = float(r)
            except Exception:
                rate = 0.0
            event_ts = pick_timestamp(
                d.get("E"), d.get("T"), d.get("eventTime"), default=now_ts()
            )
            store.upsert_funding("binance", s, rate=rate, interval="8h", ts=event_ts)

    async def _handle_trade(d):
        s = d.get("s") or d.get("symbol")
        p = d.get("p") or d.get("price")
        if not s or p is None:
            return
        try:
            price = float(p)
        except (TypeError, ValueError):
            return
        received_at = now_ts()
        event_ts = pick_timestamp(
            d.get("T"),
            d.get("E"),
            d.get("eventTime"),
            d.get("tradeTime"),
            default=received_at,
        )
        store.upsert_order_book(
            "binance",
            s,
            last_price=price,
            ts=received_at,
            last_price_ts=event_ts,
        )

    async def _consume_batch(batch: Sequence[Symbol]):
        if not batch:
            return

        handler_map: Dict[str, Callable[[dict], Awaitable[None]]] = {}
        for sym in batch:
            handler_map[_stream_name_book(sym)] = _handle_book
            handler_map[_stream_name_mark(sym)] = _handle_mark
            handler_map[_stream_name_depth(sym)] = _handle_depth
            handler_map[_stream_name_trade(sym)] = _handle_trade

        params = sorted(handler_map.keys())
        if not params:
            return

        label = f"{params[0]}..." if len(params) > 1 else params[0]

        def _infer_stream(event: str | None, symbol: str | None) -> str | None:
            if not event or not symbol:
                return None
            event_norm = event.lower()
            symbol_norm = symbol.lower()
            if event_norm == "bookticker":
                return f"{symbol_norm}@bookTicker"
            if event_norm == "markpriceupdate":
                return f"{symbol_norm}@markPrice@1s"
            if event_norm == "depthupdate":
                return f"{symbol_norm}@depth5@100ms"
            if event_norm == "aggtrade":
                return f"{symbol_norm}@aggTrade"
            return None

        async for ws in _reconnect(WS_ENDPOINT):
            sub_id = int(time.time())
            try:
                await ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": sub_id}))
                async for msg in iter_ws_messages(
                    ws,
                    ping_interval=5.0,
                    max_idle=20.0,
                    name=f"binance:{label}",
                ):
                    try:
                        data = json.loads(msg)
                    except Exception:
                        logger.exception("Failed to decode Binance payload", exc_info=True)
                        continue

                    if data.get("id") == sub_id and data.get("result") in (None, "success"):
                        continue

                    payload = data.get("data") if isinstance(data, dict) else None
                    stream = data.get("stream") if isinstance(data, dict) else None

                    if payload is None:
                        payload = data

                    if stream is None and isinstance(payload, dict):
                        stream = _infer_stream(payload.get("e"), payload.get("s") or payload.get("symbol"))

                    handler = handler_map.get(stream)
                    if handler and isinstance(payload, dict):
                        await handler(payload)
            except asyncio.CancelledError:
                raise
            except RuntimeError as exc:
                logger.warning("Binance stream %s idle: %s", label, exc)
            except Exception:
                logger.exception("Binance stream %s error", label)
            await asyncio.sleep(1)

    tasks = [
        asyncio.create_task(_consume_batch(subscribe[i : i + CHUNK]))
        for i in range(0, len(subscribe), CHUNK)
    ]
    await asyncio.gather(*tasks)

async def _reconnect(url: str):
    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=None,
                close_timeout=5,
                max_queue=None,
            ) as ws:
                yield ws
        except Exception:
            await asyncio.sleep(2)
            continue
