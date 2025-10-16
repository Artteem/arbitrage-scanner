from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from typing import Iterable, Sequence, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from ..engine.spread_history import Candle
from .repositories import get_latest_quote_before, get_quotes_history


@dataclass(slots=True)
class SpreadSeries:
    entry: Sequence[Candle]
    exit: Sequence[Candle]


async def load_spread_candles_from_quotes(
    session: AsyncSession,
    *,
    long_contract_id: int,
    short_contract_id: int,
    timeframe_seconds: int,
    start: datetime,
    end: datetime,
) -> SpreadSeries:
    """Build OHLC candles for entry/exit spreads using stored quotes."""

    if end <= start:
        return SpreadSeries(entry=(), exit=())

    long_quotes = await get_quotes_history(session, long_contract_id, start, end)
    short_quotes = await get_quotes_history(session, short_contract_id, start, end)

    if not long_quotes and not short_quotes:
        return SpreadSeries(entry=(), exit=())

    long_prev = await get_latest_quote_before(session, long_contract_id, start)
    short_prev = await get_latest_quote_before(session, short_contract_id, start)

    points: list[tuple[float, float, float]] = []

    def _quote_tuple(quote, side: str) -> tuple[datetime, str, object]:
        return quote.timestamp, side, quote

    events: list[tuple[datetime, str, object]] = []
    if long_prev is not None:
        events.append(_quote_tuple(long_prev, "long"))
    if short_prev is not None:
        events.append(_quote_tuple(short_prev, "short"))
    events.extend(_quote_tuple(q, "long") for q in long_quotes)
    events.extend(_quote_tuple(q, "short") for q in short_quotes)
    events.sort(key=lambda item: (item[0], item[1]))

    current_long = long_prev
    current_short = short_prev

    start_ts = start.timestamp()
    if current_long is not None and current_short is not None:
        maybe_point = _compute_point(current_long, current_short, start_ts)
        if maybe_point is not None:
            points.append(maybe_point)

    for ts_dt, side, quote in events:
        if ts_dt < start:
            if side == "long":
                current_long = quote
            else:
                current_short = quote
            continue
        if side == "long":
            current_long = quote
        else:
            current_short = quote
        if current_long is None or current_short is None:
            continue
        maybe_point = _compute_point(current_long, current_short, ts_dt.timestamp())
        if maybe_point is not None:
            points.append(maybe_point)

    if not points:
        return SpreadSeries(entry=(), exit=())

    entry_candles, exit_candles = _build_candles(points, timeframe_seconds, start.timestamp())
    return SpreadSeries(entry=entry_candles, exit=exit_candles)


def _compute_point(long_quote, short_quote, ts: float) -> tuple[float, float, float] | None:
    try:
        long_bid = float(long_quote.bid)
        long_ask = float(long_quote.ask)
        short_bid = float(short_quote.bid)
        short_ask = float(short_quote.ask)
    except (TypeError, ValueError):
        return None

    if min(long_bid, long_ask, short_bid, short_ask) <= 0:
        return None

    entry = _spread_entry(short_bid, long_ask)
    exit_ = _spread_exit(long_bid, short_ask)
    if not (math.isfinite(entry) and math.isfinite(exit_)):
        return None
    return (ts, entry, exit_)


def _build_candles(
    points: Iterable[Tuple[float, float, float]],
    timeframe_seconds: int,
    min_ts: float,
) -> tuple[list[Candle], list[Candle]]:
    entry_map: dict[int, Candle] = {}
    exit_map: dict[int, Candle] = {}
    tf = max(int(timeframe_seconds), 1)
    min_bucket = int(min_ts // tf) * tf

    for ts, entry, exit_ in points:
        bucket = int(ts // tf) * tf
        if bucket < min_bucket:
            continue
        entry_candle = entry_map.get(bucket)
        if entry_candle is None:
            entry_candle = Candle(bucket, entry, entry, entry, entry)
            entry_map[bucket] = entry_candle
        else:
            entry_candle.update(entry)

        exit_candle = exit_map.get(bucket)
        if exit_candle is None:
            exit_candle = Candle(bucket, exit_, exit_, exit_, exit_)
            exit_map[bucket] = exit_candle
        else:
            exit_candle.update(exit_)

    entry_series = [entry_map[key] for key in sorted(entry_map.keys())]
    exit_series = [exit_map[key] for key in sorted(exit_map.keys())]
    return entry_series, exit_series


def _spread_entry(short_bid: float, long_ask: float) -> float:
    mid = (short_bid + long_ask) / 2.0
    return (short_bid - long_ask) / mid * 100.0


def _spread_exit(long_bid: float, short_ask: float) -> float:
    mid = (long_bid + short_ask) / 2.0
    return (long_bid - short_ask) / mid * 100.0

