from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence

from ..domain import ExchangeName, Symbol


@dataclass
class Candle:
    """Simple OHLC candle stored in memory."""

    start_ts: int
    open: float
    high: float
    low: float
    close: float

    def update(self, value: float) -> None:
        if value > self.high:
            self.high = value
        if value < self.low:
            self.low = value
        self.close = value

    def to_dict(self) -> dict:
        return {
            "ts": self.start_ts,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
        }


class SpreadHistory:
    """Keeps OHLC history for spreads for several timeframes."""

    def __init__(self, timeframes: Iterable[int], max_candles: int = 600) -> None:
        normalized = sorted({int(tf) for tf in timeframes if int(tf) > 0})
        if not normalized:
            raise ValueError("At least one positive timeframe is required")
        self._timeframes: tuple[int, ...] = tuple(normalized)
        self._max_candles = int(max_candles)
        if self._max_candles <= 0:
            raise ValueError("max_candles must be positive")
        self._entries: Dict[str, Dict[int, List[Candle]]] = {}
        self._exits: Dict[str, Dict[int, List[Candle]]] = {}

    @property
    def timeframes(self) -> tuple[int, ...]:
        return self._timeframes

    def add_point(
        self,
        *,
        symbol: Symbol,
        long_exchange: ExchangeName,
        short_exchange: ExchangeName,
        entry_value: float,
        exit_value: float,
        ts: float,
    ) -> None:
        key = self._key(symbol, long_exchange, short_exchange)
        self._add_value(self._entries, key, ts, float(entry_value))
        self._add_value(self._exits, key, ts, float(exit_value))

    def get_candles(
        self,
        metric: str,
        *,
        symbol: Symbol,
        long_exchange: ExchangeName,
        short_exchange: ExchangeName,
        timeframe: int,
    ) -> Sequence[Candle]:
        tf = int(timeframe)
        if tf not in self._timeframes:
            raise KeyError(f"Unsupported timeframe: {tf}")
        storage = self._entries if metric == "entry" else self._exits
        key = self._key(symbol, long_exchange, short_exchange)
        by_tf = storage.get(key)
        if not by_tf:
            return []
        candles = by_tf.get(tf)
        if not candles:
            return []
        return list(candles)

    def _add_value(
        self,
        storage: Dict[str, Dict[int, List[Candle]]],
        key: str,
        ts: float,
        value: float,
    ) -> None:
        per_key = storage.setdefault(key, {})
        for tf in self._timeframes:
            period_start = int(ts // tf) * tf
            candles = per_key.setdefault(tf, [])
            if candles and candles[-1].start_ts == period_start:
                candles[-1].update(value)
            else:
                candles.append(Candle(period_start, value, value, value, value))
                if len(candles) > self._max_candles:
                    del candles[: len(candles) - self._max_candles]

    @staticmethod
    def _key(symbol: Symbol, long_exchange: ExchangeName, short_exchange: ExchangeName) -> str:
        return f"{symbol}:{long_exchange}:{short_exchange}".lower()
