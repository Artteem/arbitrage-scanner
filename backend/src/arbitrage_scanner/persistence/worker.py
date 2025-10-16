"""Background persistence worker used by the ticker store."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from ..domain import ExchangeName, Symbol
from .database import BBOPoint, Database, FundingPoint
from .normalization import normalize_symbol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _BBOEvent:
    exchange: ExchangeName
    symbol: Symbol
    ts: int
    bid: float
    ask: float


@dataclass(frozen=True)
class _FundingEvent:
    exchange: ExchangeName
    symbol: Symbol
    ts: int
    rate: float
    interval: str


class DataPersistence:
    """Persist tick data and funding information to the database asynchronously."""

    def __init__(self, database: Database, *, max_queue: int = 5000) -> None:
        self._db = database
        self._queue: "asyncio.Queue[_BBOEvent | _FundingEvent | None]" = asyncio.Queue(max_queue)
        self._task: Optional[asyncio.Task[None]] = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is None:
            return
        await self._queue.put(None)
        await self._stopped.wait()
        task = self._task
        self._task = None
        if task:
            await task

    def submit_bbo(self, exchange: ExchangeName, symbol: Symbol, ts: float, bid: float, ask: float) -> None:
        if self._task is None:
            return
        event = _BBOEvent(exchange=exchange, symbol=symbol, ts=int(ts), bid=float(bid), ask=float(ask))
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Persistence queue for BBO events is full; dropping update for %s %s", exchange, symbol)

    def submit_funding(
        self,
        exchange: ExchangeName,
        symbol: Symbol,
        ts: float,
        rate: float,
        interval: str,
    ) -> None:
        if self._task is None:
            return
        event = _FundingEvent(exchange=exchange, symbol=symbol, ts=int(ts), rate=float(rate), interval=str(interval))
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Persistence queue for funding events is full; dropping update for %s %s", exchange, symbol)

    async def _run(self) -> None:
        try:
            while True:
                event = await self._queue.get()
                if event is None:
                    break
                try:
                    if isinstance(event, _BBOEvent):
                        await self._handle_bbo(event)
                    else:
                        await self._handle_funding(event)
                except Exception:
                    logger.exception("Failed to persist event: %s", event)
        finally:
            self._stopped.set()

    async def _handle_bbo(self, event: _BBOEvent) -> None:
        normalized = normalize_symbol(event.symbol)
        await self._db.ensure_alias(event.exchange, event.symbol)
        point = BBOPoint(ts=event.ts, bid=event.bid, ask=event.ask)
        await self._db.record_bbo(event.exchange, event.symbol, normalized, [point])

    async def _handle_funding(self, event: _FundingEvent) -> None:
        normalized = normalize_symbol(event.symbol)
        await self._db.ensure_alias(event.exchange, event.symbol)
        point = FundingPoint(ts=event.ts, rate=event.rate, interval=event.interval)
        await self._db.record_funding(event.exchange, event.symbol, normalized, [point])
