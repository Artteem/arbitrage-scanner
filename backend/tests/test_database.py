import pytest

from arbitrage_scanner.persistence import Database, SpreadSnapshotRecord, TickRecord


@pytest.mark.asyncio
async def test_insert_and_fetch_ticks(tmp_path):
    db = Database(tmp_path / "test.sqlite3")
    await db.connect()
    records = [
        TickRecord(exchange="binance", symbol="BTCUSDT", ts_ms=1000, bid=10.0, ask=10.5),
        TickRecord(exchange="binance", symbol="BTCUSDT", ts_ms=2000, bid=10.2, ask=10.7),
        TickRecord(exchange="bybit", symbol="BTCUSDT", ts_ms=2000, bid=10.1, ask=10.6),
    ]
    await db.insert_ticks(records)
    fetched = await db.fetch_ticks(symbol="BTCUSDT", start_ts_ms=0, end_ts_ms=5000)
    assert len(fetched) == 3
    assert fetched[0].bid == pytest.approx(10.0)
    await db.close()


@pytest.mark.asyncio
async def test_fetch_spread_candles(tmp_path):
    db = Database(tmp_path / "spreads.sqlite3")
    await db.connect()
    snapshots = [
        SpreadSnapshotRecord(
            symbol="BTCUSDT",
            long_exchange="binance",
            short_exchange="bybit",
            ts=60,
            entry_pct=1.0,
            exit_pct=0.5,
            long_bid=10.1,
            long_ask=10.3,
            short_bid=10.4,
            short_ask=10.6,
            long_bids=[],
            long_asks=[],
            short_bids=[],
            short_asks=[],
        ),
        SpreadSnapshotRecord(
            symbol="BTCUSDT",
            long_exchange="binance",
            short_exchange="bybit",
            ts=90,
            entry_pct=1.5,
            exit_pct=0.7,
            long_bid=10.2,
            long_ask=10.4,
            short_bid=10.5,
            short_ask=10.7,
            long_bids=[],
            long_asks=[],
            short_bids=[],
            short_asks=[],
        ),
        SpreadSnapshotRecord(
            symbol="BTCUSDT",
            long_exchange="binance",
            short_exchange="bybit",
            ts=160,
            entry_pct=0.5,
            exit_pct=0.2,
            long_bid=10.0,
            long_ask=10.2,
            short_bid=10.3,
            short_ask=10.5,
            long_bids=[],
            long_asks=[],
            short_bids=[],
            short_asks=[],
        ),
    ]
    await db.insert_spread_snapshots(snapshots)
    candles = await db.fetch_spread_candles(
        metric="entry",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe_seconds=60,
        start_ts=0,
    )
    assert len(candles) == 2
    first = candles[0]
    assert first.start_ts == 60
    assert first.open == pytest.approx(1.0)
    assert first.close == pytest.approx(1.5)
    assert first.high == pytest.approx(1.5)
    assert first.low == pytest.approx(1.0)
    second = candles[1]
    assert second.start_ts == 120
    assert second.open == pytest.approx(0.5)
    await db.close()
