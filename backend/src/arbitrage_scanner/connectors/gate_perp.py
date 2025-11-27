from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Iterable, List, Sequence, Tuple

import websockets

from ..domain import Symbol, Ticker
from ..store import TickerStore

logger = logging.getLogger(__name__)

WS_ENDPOINT = "wss://fx-ws.gateio.ws/v4/ws/usdt"
PING_INTERVAL = 30.0


async def run_gate(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    contracts = _resolve_contracts(symbols)
    if not contracts:
        logger.warning("gate: no contracts to subscribe")
        return

    while True:
        try:
            async with websockets.connect(WS_ENDPOINT, ping_interval=None, ping_timeout=None) as ws:
                await _subscribe_all(ws, contracts)
                pong_state = {"ts": time.time()}
                ping_task = asyncio.create_task(_ping_loop(ws, pong_state))
                try:
                    async for raw in ws:
                        msg = _decode(raw)
                        if not msg:
                            continue
                        ch = str(msg.get("channel", "")).lower()
                        if ch == "futures.pong":
                            pong_state["ts"] = time.time()
                            continue
                        if ch == "futures.tickers":
                            await _handle_tickers(store, msg)
                        elif ch == "futures.obu":
                            await _handle_order_book(store, msg)
                        elif ch == "futures.book_ticker":
                            await _handle_best_ask_bid(store, msg)
                        else:
                            print(raw)
                except Exception as e:
                    print(e)
                finally:
                    print("Funally")
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("gate: websocket loop error, reconnecting", exc_info=exc)
            await asyncio.sleep(1.0)


async def _ping_loop(ws, pong_state: dict) -> None:
    while True:
        await asyncio.sleep(PING_INTERVAL)
        payload = {"time": int(time.time()), "channel": "futures.ping"}
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            print("Exception ping loop")
            raise
        if time.time() - pong_state.get("ts", 0) > PING_INTERVAL * 2:
            print("gate: missed pong")
            raise ConnectionError("gate: missed pong")


async def _subscribe_all(ws, contracts: Sequence[str]) -> None:
    ticker_sub = {
        "time": int(time.time()),
        "channel": "futures.tickers",
        "event": "subscribe",
        "payload": list(contracts),
    }
    await ws.send(json.dumps(ticker_sub))

    best_ab_book_sub = {
        "time": int(time.time()),
        "channel": "futures.book_ticker",
        "event": "subscribe",
        "payload": contracts,
    }
    await ws.send(json.dumps(best_ab_book_sub))

    order_book_sub = {
        "time": int(time.time()),
        "channel": "futures.obu",
        "event": "subscribe",
        "payload": [f"ob.{c}.400" for c in contracts],
    }
    await ws.send(json.dumps(order_book_sub))


def _decode(raw) -> dict | None:
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


def _handle_symbol_value(value) -> Symbol | None:
    if not value:
        return None
    return Symbol(str(value).replace("_", "").upper())


async def _handle_tickers(store: TickerStore, message: dict) -> None:
    result = message.get("result") or []
    items = result if isinstance(result, list) else [result]
    ts = message.get("time")
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = _handle_symbol_value(item.get("contract"))
        if not symbol:
            continue
        try:
            rate_val = float(item.get("funding_rate"))
        except (TypeError, ValueError):
            rate_val = None
        if rate_val is not None:
            store.upsert_funding(
                "gate",
                symbol,
                rate=rate_val,
                interval="8h",
                ts=ts,
            )

        last_price = item.get("mark_price")
        if last_price:
            store.set_last_price(
                "gate",
                symbol,
                last_price=last_price,
                ts=ts,
            )

async def _handle_order_book(store: TickerStore, message: dict) -> None:
    data = message.get("result") or {}
    if not isinstance(data, dict):
        return
    isFull = data.get("full") or False
    if not isFull:
        return
    symbol_array = data.get("s", "").split(".")
    if len(symbol_array) < 3:
        return
    symbol = _handle_symbol_value(symbol_array[1])
    if not symbol:
        return
    bids = _parse_levels(data.get("b"))
    asks = _parse_levels(data.get("a"))
    if not bids and not asks:
        return
    ts = _timestamp(data.get("t"))
    store.upsert_order_book("gate", symbol, bids=bids or None, asks=asks or None, ts=ts)


async def _handle_best_ask_bid(store: TickerStore, message: dict) -> None:
    data = message.get("result") or {}
    if not isinstance(data, dict):
        return
    symbol = _handle_symbol_value(data.get("s"))
    ask = _as_float(data.get('a', 0))
    bid = _as_float(data.get('b', 0))
    ts = message.get("time") or time.time()

    if ask > 0 and bid > 0:
        store.upsert_ticker(Ticker(exchange="gate", symbol=symbol, bid=bid, ask=ask, ts=ts))


def _parse_levels(levels: Iterable) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    if not levels:
        return out
    for entry in levels:
        try:
            price = float(entry[0])
            size = float(entry[1])
        except Exception:
            continue
        if price > 0 and size > 0:
            out.append((price, size))
    return out


def _resolve_contracts(symbols: Sequence[Symbol]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for sym in symbols:
        if not sym:
            continue
        native = _to_native(sym)
        if native in seen:
            continue
        seen.add(native)
        out.append(native)
    return out


def _to_native(symbol: Symbol) -> str:
    sym = str(symbol).strip().upper().replace("-", "_")
    if "_" not in sym and sym.endswith("USDT"):
        sym = f"{sym[:-4]}_USDT"
    return sym


def _timestamp(raw) -> float:
    try:
        val = float(raw)
        return val / 1000.0 if val > 1e12 else val
    except Exception:
        return time.time()


def _as_float(val) -> float:
    try:
        out = float(val)
        return out if out > 0 else 0.0
    except Exception:
        return 0.0
