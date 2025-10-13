from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from typing import Iterable, Sequence

from ..engine.spread_calc import Row
from ..store import OrderBookData
from ..domain import ExchangeName, Symbol, Ticker
from .database import Database, SpreadSnapshotRecord, TickRecord

logger = logging.getLogger(__name__)


def _limit_levels(levels: Sequence[tuple[float, float]] | None, depth: int = 20) -> list[tuple[float, float]]:
    if not levels:
        return []
    limited: list[tuple[float, float]] = []
    for price, size in levels[:depth]:
        try:
            p = float(price)
            s = float(size)
        except (TypeError, ValueError):
            continue
        if math.isfinite(p) and math.isfinite(s) and p > 0 and s >= 0:
            limited.append((p, s))
    return limited


@dataclass
class _TickEvent:
    exchange: str
    symbol: str
    ts_ms: int
    bid: float
    ask: float


@dataclass
class _SpreadEvent:
    record: SpreadSnapshotRecord


class DataRecorder:
    """Buffers market data events and persists them in batches."""

    def __init__(self, database: Database, *, flush_interval: float = 1.0, max_batch: int = 500) -> None:
        self._database = database
        self._flush_interval = max(float(flush_interval), 0.1)
        self._max_batch = max(int(max_batch), 1)
        self._queue: asyncio.Queue[_TickEvent | _SpreadEvent] = asyncio.Queue(max_batch * 10)
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run(), name="data-recorder")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stopped.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    def record_tick(self, *, exchange: str, symbol: str, ts: float, bid: float, ask: float) -> None:
        if not self._database.is_connected:
            return
        try:
            ts_ms = int(max(ts, 0) * 1000)
        except Exception:
            return
        event = _TickEvent(exchange=str(exchange).lower(), symbol=str(symbol).upper(), ts_ms=ts_ms, bid=float(bid), ask=float(ask))
        self._put_nowait(event)

    def on_ticker(self, ticker: Ticker) -> None:
        self.record_tick(
            exchange=ticker.exchange,
            symbol=ticker.symbol,
            ts=ticker.ts,
            bid=ticker.bid,
            ask=ticker.ask,
        )

    def record_spread_row(self, row: Row, ts: float) -> None:
        if not self._database.is_connected:
            return
        try:
            ts_int = int(ts)
        except Exception:
            return
        record = self._snapshot_from_row(row, ts_int)
        if record is None:
            return
        event = _SpreadEvent(record=record)
        self._put_nowait(event)

    def _put_nowait(self, item: _TickEvent | _SpreadEvent) -> None:
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            logger.warning("Data recorder queue is full; dropping event")

    async def _run(self) -> None:
        tick_buffer: list[_TickEvent] = []
        spread_buffer: list[_SpreadEvent] = []
        try:
            while not self._stopped.is_set():
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=self._flush_interval)
                except asyncio.TimeoutError:
                    item = None
                if isinstance(item, _TickEvent):
                    tick_buffer.append(item)
                elif isinstance(item, _SpreadEvent):
                    spread_buffer.append(item)
                if tick_buffer and (len(tick_buffer) >= self._max_batch or item is None):
                    await self._flush_ticks(tick_buffer)
                    tick_buffer.clear()
                if spread_buffer and (len(spread_buffer) >= self._max_batch or item is None):
                    await self._flush_spreads(spread_buffer)
                    spread_buffer.clear()
        except asyncio.CancelledError:
            pass
        finally:
            if tick_buffer:
                await self._flush_ticks(tick_buffer)
            if spread_buffer:
                await self._flush_spreads(spread_buffer)

    async def _flush_ticks(self, events: Iterable[_TickEvent]) -> None:
        records = [
            TickRecord(
                exchange=ExchangeName(event.exchange),
                symbol=Symbol(event.symbol),
                ts_ms=event.ts_ms,
                bid=event.bid,
                ask=event.ask,
            )
            for event in events
        ]
        try:
            await self._database.insert_ticks(records)
        except Exception:
            logger.exception("Failed to persist tick batch", exc_info=True)

    async def _flush_spreads(self, events: Iterable[_SpreadEvent]) -> None:
        records = [event.record for event in events]
        try:
            await self._database.insert_spread_snapshots(records)
        except Exception:
            logger.exception("Failed to persist spread batch", exc_info=True)

    def _snapshot_from_row(self, row: Row, ts: int) -> SpreadSnapshotRecord | None:
        try:
            long_bid = float(row.price_long_bid)
            long_ask = float(row.price_long_ask)
            short_bid = float(row.price_short_bid)
            short_ask = float(row.price_short_ask)
            entry = float(row.entry_pct)
            exit_value = float(row.exit_pct)
        except (TypeError, ValueError):
            return None
        if not all(math.isfinite(x) for x in (long_bid, long_ask, short_bid, short_ask, entry, exit_value)):
            return None
        long_bids = _limit_levels(self._levels_from_orderbook(row.orderbook_long))
        long_asks = _limit_levels(self._levels_from_orderbook(row.orderbook_long, side="asks"))
        short_bids = _limit_levels(self._levels_from_orderbook(row.orderbook_short))
        short_asks = _limit_levels(self._levels_from_orderbook(row.orderbook_short, side="asks"))
        return SpreadSnapshotRecord(
            symbol=row.symbol,
            long_exchange=row.long_ex,
            short_exchange=row.short_ex,
            ts=ts,
            entry_pct=entry,
            exit_pct=exit_value,
            long_bid=long_bid,
            long_ask=long_ask,
            short_bid=short_bid,
            short_ask=short_ask,
            long_bids=long_bids,
            long_asks=long_asks,
            short_bids=short_bids,
            short_asks=short_asks,
        )

    def _levels_from_orderbook(
        self,
        orderbook: OrderBookData | None,
        side: str = "bids",
    ) -> Sequence[tuple[float, float]] | None:
        if orderbook is None:
            return None
        if side == "asks":
            return orderbook.asks
        return orderbook.bids


__all__ = ["DataRecorder"]
