"""Async persistence layer backed by SQLite."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import aiosqlite

from ..domain import ExchangeName, Symbol
from .normalization import NormalizedSymbol, normalize_symbol, split_symbol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BBOPoint:
    ts: int
    bid: float
    ask: float


@dataclass(frozen=True)
class FundingPoint:
    ts: int
    rate: float
    interval: str


class Database:
    """Manages a SQLite database with normalized market data."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._conn is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(str(self._path))
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")

    async def initialize(self) -> None:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS normalized_contracts (
                normalized_symbol TEXT PRIMARY KEY,
                base_asset TEXT NOT NULL,
                quote_asset TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS contract_aliases (
                exchange TEXT NOT NULL,
                raw_symbol TEXT NOT NULL,
                normalized_symbol TEXT NOT NULL,
                PRIMARY KEY (exchange, raw_symbol),
                FOREIGN KEY (normalized_symbol) REFERENCES normalized_contracts(normalized_symbol)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS taker_fees (
                exchange TEXT PRIMARY KEY,
                fee REAL NOT NULL,
                ts INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS bbo_history (
                exchange TEXT NOT NULL,
                raw_symbol TEXT NOT NULL,
                normalized_symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                bid REAL NOT NULL,
                ask REAL NOT NULL,
                PRIMARY KEY (exchange, raw_symbol, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_bbo_normalized_ts
                ON bbo_history(normalized_symbol, ts);

            CREATE TABLE IF NOT EXISTS funding_history (
                exchange TEXT NOT NULL,
                raw_symbol TEXT NOT NULL,
                normalized_symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                rate REAL NOT NULL,
                interval TEXT NOT NULL,
                PRIMARY KEY (exchange, raw_symbol, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_funding_normalized_ts
                ON funding_history(normalized_symbol, ts);
            """
        )
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def ensure_contract(self, symbol: NormalizedSymbol) -> None:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        async with self._lock:
            await self._conn.execute(
                """
                INSERT OR IGNORE INTO normalized_contracts (normalized_symbol, base_asset, quote_asset)
                VALUES (?, ?, ?)
                """,
                (symbol.value, symbol.base, symbol.quote),
            )
            await self._conn.commit()

    async def ensure_alias(self, exchange: ExchangeName, raw_symbol: Symbol) -> str:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        normalized = split_symbol(raw_symbol)
        await self.ensure_contract(normalized)
        async with self._lock:
            await self._conn.execute(
                """
                INSERT OR IGNORE INTO contract_aliases (exchange, raw_symbol, normalized_symbol)
                VALUES (?, ?, ?)
                """,
                (exchange, raw_symbol, normalized.value),
            )
            await self._conn.commit()
        return normalized.value

    async def record_taker_fee(self, exchange: ExchangeName, fee: float, ts: int) -> None:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        async with self._lock:
            await self._conn.execute(
                """
                INSERT INTO taker_fees (exchange, fee, ts)
                VALUES (?, ?, ?)
                ON CONFLICT(exchange) DO UPDATE SET fee=excluded.fee, ts=excluded.ts
                """,
                (exchange, float(fee), int(ts)),
            )
            await self._conn.commit()

    async def record_bbo(
        self,
        exchange: ExchangeName,
        raw_symbol: Symbol,
        normalized_symbol: str | None,
        points: Iterable[BBOPoint],
    ) -> None:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        points_list = list(points)
        if not points_list:
            return
        if not normalized_symbol:
            normalized_symbol = normalize_symbol(raw_symbol)
        payload = [
            (exchange, raw_symbol, normalized_symbol, int(p.ts), float(p.bid), float(p.ask))
            for p in points_list
        ]
        async with self._lock:
            await self._conn.executemany(
                """
                INSERT OR IGNORE INTO bbo_history (exchange, raw_symbol, normalized_symbol, ts, bid, ask)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            await self._conn.commit()

    async def record_single_bbo(
        self,
        exchange: ExchangeName,
        raw_symbol: Symbol,
        ts: int,
        bid: float,
        ask: float,
    ) -> None:
        normalized = normalize_symbol(raw_symbol)
        await self.ensure_alias(exchange, raw_symbol)
        await self.record_bbo(
            exchange,
            raw_symbol,
            normalized,
            [BBOPoint(ts=ts, bid=bid, ask=ask)],
        )

    async def record_funding(
        self,
        exchange: ExchangeName,
        raw_symbol: Symbol,
        normalized_symbol: str | None,
        points: Iterable[FundingPoint],
    ) -> None:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        payload = [
            (exchange, raw_symbol, normalized_symbol or normalize_symbol(raw_symbol), int(p.ts), float(p.rate), str(p.interval))
            for p in points
        ]
        if not payload:
            return
        async with self._lock:
            await self._conn.executemany(
                """
                INSERT OR REPLACE INTO funding_history (exchange, raw_symbol, normalized_symbol, ts, rate, interval)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            await self._conn.commit()

    async def record_single_funding(
        self,
        exchange: ExchangeName,
        raw_symbol: Symbol,
        ts: int,
        rate: float,
        interval: str,
    ) -> None:
        normalized = normalize_symbol(raw_symbol)
        await self.ensure_alias(exchange, raw_symbol)
        await self.record_funding(
            exchange,
            raw_symbol,
            normalized,
            [FundingPoint(ts=ts, rate=rate, interval=interval)],
        )

    async def resolve_normalized(self, exchange: ExchangeName, raw_symbol: Symbol) -> str:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        query = "SELECT normalized_symbol FROM contract_aliases WHERE exchange=? AND raw_symbol=?"
        async with self._conn.execute(query, (exchange, raw_symbol)) as cursor:
            row = await cursor.fetchone()
        if row:
            return str(row[0])
        normalized = await self.ensure_alias(exchange, raw_symbol)
        return normalized

    async def load_bbo_points(
        self,
        exchange: ExchangeName,
        normalized_symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> list[BBOPoint]:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        query = (
            "SELECT ts, bid, ask FROM bbo_history "
            "WHERE exchange=? AND normalized_symbol=? AND ts BETWEEN ? AND ? "
            "ORDER BY ts ASC"
        )
        async with self._conn.execute(query, (exchange, normalized_symbol, start_ts, end_ts)) as cursor:
            rows = await cursor.fetchall()
        return [BBOPoint(ts=int(ts), bid=float(bid), ask=float(ask)) for ts, bid, ask in rows]

    async def load_funding_points(
        self,
        exchange: ExchangeName,
        normalized_symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> list[FundingPoint]:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        query = (
            "SELECT ts, rate, interval FROM funding_history "
            "WHERE exchange=? AND normalized_symbol=? AND ts BETWEEN ? AND ? "
            "ORDER BY ts ASC"
        )
        async with self._conn.execute(query, (exchange, normalized_symbol, start_ts, end_ts)) as cursor:
            rows = await cursor.fetchall()
        return [FundingPoint(ts=int(ts), rate=float(rate), interval=str(interval)) for ts, rate, interval in rows]

    async def list_exchange_aliases(self, exchange: ExchangeName) -> dict[str, str]:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        query = "SELECT raw_symbol, normalized_symbol FROM contract_aliases WHERE exchange=?"
        async with self._conn.execute(query, (exchange,)) as cursor:
            rows = await cursor.fetchall()
        return {str(raw): str(norm) for raw, norm in rows}

    async def list_contracts(self) -> dict[str, NormalizedSymbol]:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        query = "SELECT normalized_symbol, base_asset, quote_asset FROM normalized_contracts"
        async with self._conn.execute(query) as cursor:
            rows = await cursor.fetchall()
        return {
            str(symbol): NormalizedSymbol(str(symbol), str(base), str(quote))
            for symbol, base, quote in rows
        }

    async def bulk_register_aliases(
        self,
        exchange: ExchangeName,
        raw_symbols: Iterable[Symbol],
    ) -> dict[Symbol, str]:
        mapping: dict[Symbol, str] = {}
        for sym in raw_symbols:
            normalized = split_symbol(sym)
            await self.ensure_contract(normalized)
            async with self._lock:
                await self._conn.execute(
                    """
                    INSERT OR IGNORE INTO contract_aliases (exchange, raw_symbol, normalized_symbol)
                    VALUES (?, ?, ?)
                    """,
                    (exchange, sym, normalized.value),
                )
                await self._conn.commit()
            mapping[sym] = normalized.value
        return mapping

    async def bulk_record_bbo(
        self,
        exchange: ExchangeName,
        entries: Iterable[tuple[Symbol, Sequence[BBOPoint]]],
    ) -> None:
        buffer: list[tuple[str, str, str, int, float, float]] = []
        for raw_symbol, points in entries:
            normalized = normalize_symbol(raw_symbol)
            await self.ensure_alias(exchange, raw_symbol)
            for point in points:
                buffer.append(
                    (
                        exchange,
                        raw_symbol,
                        normalized,
                        int(point.ts),
                        float(point.bid),
                        float(point.ask),
                    )
                )
        if not buffer:
            return
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        async with self._lock:
            await self._conn.executemany(
                """
                INSERT OR IGNORE INTO bbo_history (exchange, raw_symbol, normalized_symbol, ts, bid, ask)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                buffer,
            )
            await self._conn.commit()

    async def bulk_record_funding(
        self,
        exchange: ExchangeName,
        entries: Iterable[tuple[Symbol, Sequence[FundingPoint]]],
    ) -> None:
        buffer: list[tuple[str, str, str, int, float, str]] = []
        for raw_symbol, points in entries:
            normalized = normalize_symbol(raw_symbol)
            await self.ensure_alias(exchange, raw_symbol)
            for point in points:
                buffer.append(
                    (
                        exchange,
                        raw_symbol,
                        normalized,
                        int(point.ts),
                        float(point.rate),
                        str(point.interval),
                    )
                )
        if not buffer:
            return
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        async with self._lock:
            await self._conn.executemany(
                """
                INSERT OR REPLACE INTO funding_history (exchange, raw_symbol, normalized_symbol, ts, rate, interval)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                buffer,
            )
            await self._conn.commit()
