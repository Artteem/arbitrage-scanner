from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import List, Sequence, Tuple, Set

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from ..domain import Symbol
from ..store import TickerStore

logger = logging.getLogger(__name__)

# Gate USDT-settled perpetuals WebSocket endpoint
WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"

# Как часто слать application-level ping (сек)
HEARTBEAT_INTERVAL = 20.0


def _symbol_to_contract(symbol: Symbol) -> str:
    """
    Конвертируем внутренний символ (например, BTCUSDT) в контракт Gate (BTC_USDT).
    """
    code = str(symbol).upper()
    if code.endswith("USDT"):
        base = code[:-4]
        return f"{base}_USDT"
    # Fallback – если случайно уже пришло BTC_USDT
    if "_" in code:
        return code
    return code


def _contract_to_symbol(contract: str) -> Symbol:
    """
    Обратное преобразование: BTC_USDT -> BTCUSDT.
    """
    return Symbol(contract.replace("_", ""))


def _as_float(val) -> float:
    try:
        out = float(val)
        return out if out > 0 else 0.0
    except Exception:
        return 0.0


async def _send_heartbeat(ws) -> None:
    """
    Application-layer ping. Протокольные ping/pong WebSocket’а
    библиотека websockets обрабатывает сама, но Gate ещё понимает futures.ping.
    """
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            msg = {
                "time": int(time.time()),
                "channel": "futures.ping",
            }
            await ws.send(json.dumps(msg))
        except Exception:
            # Вылетаем наверх — внешний цикл переподключится
            logger.warning("Gate WS heartbeat failed, closing connection")
            raise


async def _consume_book_ticker(
    store: TickerStore,
    ws,
    contracts: Set[str],
) -> None:
    """
    Читаем сообщения канала futures.book_ticker и обновляем стаканы в TickerStore.
    """
    while True:
        raw = await ws.recv()
        try:
            data = json.loads(raw)
        except Exception:
            logger.debug("Gate WS: failed to decode JSON: %s", raw)
            continue

        if data.get("channel") != "futures.book_ticker":
            # Может прилетать pong, ошибки и т.п. – игнорируем
            continue
        if data.get("event") != "update":
            continue

        result = data.get("result") or {}
        contract = result.get("s")
        if not contract or contract not in contracts:
            continue

        symbol = _contract_to_symbol(contract)

        bid = _as_float(result.get("b"))
        ask = _as_float(result.get("a"))
        bid_size = _as_float(result.get("B"))
        ask_size = _as_float(result.get("A"))

        ts_ms = result.get("t") or data.get("time_ms") or int(time.time() * 1000)
        ts = float(ts_ms) / 1000.0

        bids: List[Tuple[float, float]] = []
        asks: List[Tuple[float, float]] = []

        if bid > 0:
            bids.append((bid, bid_size))
        if ask > 0:
            asks.append((ask, ask_size))

        # Обновляем стакан
        try:
            store.upsert_order_book("gate", symbol, bids, asks, ts)
        except Exception:
            logger.exception("Gate WS: failed to upsert order book for %s", symbol)

        # Для спредов одновременно обновим last_price по mid
        mid = 0.0
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
        elif bid > 0:
            mid = bid
        elif ask > 0:
            mid = ask

        if mid > 0:
            try:
                store.set_last_price("gate", symbol, mid, ts)
            except Exception:
                logger.exception("Gate WS: failed to set last price for %s", symbol)


async def _ws_loop(store: TickerStore, contracts: Set[str]) -> None:
    """
    Основной цикл: подключаемся к WS, подписываемся и читаем обновления.
    При обрыве соединения — переподключаемся с экспоненциальным backoff.
    """
    if not contracts:
        logger.warning("Gate WS: no contracts to subscribe")
        return

    backoff = 3.0

    while True:
        try:
            logger.info(
                "Connecting to Gate WS (%s), %d contracts",
                WS_URL,
                len(contracts),
            )
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                # Подписка на best bid/ask
                sub = {
                    "time": int(time.time()),
                    "channel": "futures.book_ticker",
                    "event": "subscribe",
                    "payload": sorted(contracts),
                }
                await ws.send(json.dumps(sub))
                logger.info(
                    "Gate WS subscribed to futures.book_ticker: %s",
                    ", ".join(sorted(contracts)),
                )

                # Запускаем heartbeat параллельно с чтением
                heartbeat_task = asyncio.create_task(_send_heartbeat(ws))
                try:
                    await _consume_book_ticker(store, ws, contracts)
                finally:
                    heartbeat_task.cancel()
                    with contextlib.suppress(Exception):
                        await heartbeat_task

        except (ConnectionClosedError, ConnectionClosedOK) as exc:
            logger.warning("Gate WS connection closed: %s", exc)
        except Exception:
            logger.exception("Gate WS: unexpected error, will reconnect")

        logger.info("Gate WS: reconnecting in %.1f seconds", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60.0)


async def run_gate(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    """
    Точка входа коннектора Gate, используется в ConnectorSpec.run.

    * Преобразует список Symbol в список Gate-контрактов (BTC_USDT и т.п.).
    * Запускает бесконечный цикл WebSocket-подключения.
    """
    if not symbols:
        logger.warning(
            "run_gate: empty symbol list – nothing to subscribe on Gate",
        )
        return

    contracts: Set[str] = {_symbol_to_contract(sym) for sym in symbols}
    contracts = {c for c in contracts if c}  # на всякий случай

    if not contracts:
        logger.warning("run_gate: no Gate contracts resolved from symbols")
        return

    await _ws_loop(store, contracts)
