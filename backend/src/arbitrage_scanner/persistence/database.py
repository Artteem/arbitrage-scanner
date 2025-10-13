from __future__ import annotations

import asyncio
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import aiosqlite

from ..domain import ExchangeName, Symbol


@dataclass(frozen=True)
class TickRecord:
    exchange: ExchangeName
    symbol: Symbol
    ts_ms: int
    bid: float
    ask: float


@dataclass(frozen=True)
class SpreadSnapshotRecord:
    symbol: Symbol
    long_exchange: ExchangeName
    short_exchange: ExchangeName
    ts: int
    entry_pct: float
    exit_pct: float
    long_bid: float
    long_ask: float
    short_bid: float
    short_ask: float
    long_bids: Sequence[tuple[float, float]]
    long_asks: Sequence[tuple[float, float]]
    short_bids: Sequence[tuple[float, float]]
    short_asks: Sequence[tuple[float, float]]


@dataclass(frozen=True)
class SpreadCandleRow:
    start_ts: int
    open: float
    high: float
    low: float
    close: float

    def to_dict(self) -> dict:
        return {
            "ts": self.start_ts,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
        }


class Database:
    """SQLite wrapper responsible for persisting market data."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._conn is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(self._path)
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await self._create_schema(conn)
        self._conn = conn

    async def close(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is not None:
            await conn.close()

    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    async def insert_ticks(self, records: Sequence[TickRecord]) -> None:
        if not records:
            return
        conn = await self._require_connection()
        payload = [
            (
                str(record.exchange).lower(),
                str(record.symbol).upper(),
                int(record.ts_ms),
                float(record.bid),
                float(record.ask),
            )
            for record in records
        ]
        async with self._lock:
            await conn.executemany(
                """
                INSERT OR REPLACE INTO ticks (exchange, symbol, ts_ms, bid, ask)
                VALUES (?, ?, ?, ?, ?)
                """,
                payload,
            )
            await conn.commit()

    async def insert_spread_snapshots(self, records: Sequence[SpreadSnapshotRecord]) -> None:
        if not records:
            return
        conn = await self._require_connection()
        payload = []
        for record in records:
            entry = (
                str(record.symbol).upper(),
                str(record.long_exchange).lower(),
                str(record.short_exchange).lower(),
                int(record.ts),
                float(record.entry_pct),
                float(record.exit_pct),
                float(record.long_bid),
                float(record.long_ask),
                float(record.short_bid),
                float(record.short_ask),
                json.dumps([[p, s] for p, s in record.long_bids]),
                json.dumps([[p, s] for p, s in record.long_asks]),
                json.dumps([[p, s] for p, s in record.short_bids]),
                json.dumps([[p, s] for p, s in record.short_asks]),
            )
            payload.append(entry)
        async with self._lock:
            await conn.executemany(
                """
                INSERT OR REPLACE INTO spread_snapshots (
                    symbol,
                    long_exchange,
                    short_exchange,
                    ts,
                    entry_pct,
                    exit_pct,
                    long_bid,
                    long_ask,
                    short_bid,
                    short_ask,
                    long_bids,
                    long_asks,
                    short_bids,
                    short_asks
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            await conn.commit()

    async def fetch_ticks(
        self,
        *,
        symbol: Symbol,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[TickRecord]:
        conn = await self._require_connection()
        query = """
            SELECT exchange, symbol, ts_ms, bid, ask
            FROM ticks
            WHERE symbol = ? AND ts_ms BETWEEN ? AND ?
            ORDER BY ts_ms ASC
        """
        cursor = await conn.execute(
            query,
            (str(symbol).upper(), int(start_ts_ms), int(end_ts_ms)),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        out: list[TickRecord] = []
        for exchange, symbol_value, ts_ms, bid, ask in rows:
            try:
                out.append(
                    TickRecord(
                        exchange=ExchangeName(str(exchange)),
                        symbol=Symbol(str(symbol_value)),
                        ts_ms=int(ts_ms),
                        bid=float(bid),
                        ask=float(ask),
                    )
                )
            except Exception:
                continue
        return out

    async def fetch_spread_candles(
        self,
        *,
        metric: str,
        symbol: Symbol,
        long_exchange: ExchangeName,
        short_exchange: ExchangeName,
        timeframe_seconds: int,
        start_ts: int,
    ) -> list[SpreadCandleRow]:
        conn = await self._require_connection()
        query = """
            SELECT ts, entry_pct, exit_pct
            FROM spread_snapshots
            WHERE symbol = ?
              AND long_exchange = ?
              AND short_exchange = ?
              AND ts >= ?
            ORDER BY ts ASC
        """
        cursor = await conn.execute(
            query,
            (
                str(symbol).upper(),
                str(long_exchange).lower(),
                str(short_exchange).lower(),
                int(start_ts),
            ),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        value_index = 1 if metric == "entry" else 2
        return self._aggregate_rows(rows, value_index, timeframe_seconds)

    async def _create_schema(self, conn: aiosqlite.Connection) -> None:
        await conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS ticks (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                bid REAL NOT NULL,
                ask REAL NOT NULL,
                PRIMARY KEY (exchange, symbol, ts_ms)
            );

            CREATE TABLE IF NOT EXISTS spread_snapshots (
                symbol TEXT NOT NULL,
                long_exchange TEXT NOT NULL,
                short_exchange TEXT NOT NULL,
                ts INTEGER NOT NULL,
                entry_pct REAL NOT NULL,
                exit_pct REAL NOT NULL,
                long_bid REAL NOT NULL,
                long_ask REAL NOT NULL,
                short_bid REAL NOT NULL,
                short_ask REAL NOT NULL,
                long_bids TEXT NOT NULL,
                long_asks TEXT NOT NULL,
                short_bids TEXT NOT NULL,
                short_asks TEXT NOT NULL,
                PRIMARY KEY (symbol, long_exchange, short_exchange, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts
            ON ticks(symbol, ts_ms);

            CREATE INDEX IF NOT EXISTS idx_spread_snapshots_symbol_ts
            ON spread_snapshots(symbol, long_exchange, short_exchange, ts);
            """
        )
        await conn.commit()

    async def _require_connection(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database connection has not been established")
        return self._conn

    def _aggregate_rows(
        self,
        rows: Sequence[Sequence[float]],
        value_index: int,
        timeframe_seconds: int,
    ) -> list[SpreadCandleRow]:
        timeframe = max(int(timeframe_seconds), 1)
        candles: list[SpreadCandleRow] = []
        current: SpreadCandleRow | None = None
        for row in rows:
            if len(row) < 3:
                continue
            ts_raw = row[0]
            value_raw = row[value_index]
            if ts_raw is None or value_raw is None:
                continue
            try:
                ts = int(ts_raw)
                value = float(value_raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                continue
            bucket = (ts // timeframe) * timeframe
            if current and current.start_ts == bucket:
                if value > current.high:
                    current = SpreadCandleRow(
                        start_ts=current.start_ts,
                        open=current.open,
                        high=value,
                        low=current.low,
                        close=value,
                    )
                elif value < current.low:
                    current = SpreadCandleRow(
                        start_ts=current.start_ts,
                        open=current.open,
                        high=current.high,
                        low=value,
                        close=value,
                    )
                else:
                    current = SpreadCandleRow(
                        start_ts=current.start_ts,
                        open=current.open,
                        high=current.high,
                        low=current.low,
                        close=value,
                    )
                candles[-1] = current
            else:
                current = SpreadCandleRow(bucket, value, value, value, value)
                candles.append(current)
        return candles


__all__ = [
    "Database",
    "SpreadCandleRow",
    "SpreadSnapshotRecord",
    "TickRecord",
]
