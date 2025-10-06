import math

import httpx
import pytest

from arbitrage_scanner.exchanges.history import fetch_spread_history, _spread_pct, SpreadCandle
from arbitrage_scanner.engine.spread_history import SpreadHistory


@pytest.mark.asyncio
async def test_fetch_spread_history_and_merge():
    base_ts_ms = 1_700_000_000_000
    step_ms = 300_000

    binance_payload = [
        [base_ts_ms, "100.0", "101.0", "99.5", "100.5"],
        [base_ts_ms + step_ms, "101.0", "102.0", "100.0", "101.5"],
    ]

    bybit_payload = {
        "retCode": 0,
        "result": {
            "list": [
                [base_ts_ms, "100.5", "101.5", "100.0", "101.0"],
                [base_ts_ms + step_ms, "101.2", "102.2", "100.5", "102.0"],
            ]
        },
    }

    state = {"binance": 0, "bybit": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "fapi.binance.com":
            if state["binance"] == 0:
                state["binance"] += 1
                return httpx.Response(200, json=binance_payload)
            return httpx.Response(200, json=[])
        if host == "api.bybit.com":
            if state["bybit"] == 0:
                state["bybit"] += 1
                return httpx.Response(200, json=bybit_payload)
            return httpx.Response(200, json={"result": {"list": []}})
        raise AssertionError(f"Unexpected host {host}")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        entry, exit_ = await fetch_spread_history(
            symbol="BTCUSDT",
            long_exchange="binance",
            short_exchange="bybit",
            timeframe_seconds=300,
            lookback_days=1,
            client=client,
        )

    assert len(entry) == 2
    assert len(exit_) == 2
    assert entry[0].start_ts == base_ts_ms // 1000
    assert exit_[1].start_ts == (base_ts_ms + step_ms) // 1000

    expected_entry_close = _spread_pct(101.0, 100.5)
    expected_exit_close = _spread_pct(101.5, 102.0)
    assert math.isclose(entry[0].close, expected_entry_close, rel_tol=1e-9)
    assert math.isclose(exit_[1].close, expected_exit_close, rel_tol=1e-9)

    history = SpreadHistory(timeframes=(300,), max_candles=100)
    history.merge_external(
        "entry",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=300,
        candles=entry,
    )
    history.merge_external(
        "exit",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=300,
        candles=exit_,
    )

    stored_entry = history.get_candles(
        "entry",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=300,
    )
    stored_exit = history.get_candles(
        "exit",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=300,
    )

    assert len(stored_entry) == 2
    assert stored_entry[0].start_ts == entry[0].start_ts
    assert math.isclose(stored_entry[0].close, expected_entry_close, rel_tol=1e-9)
    assert len(stored_exit) == 2
    assert math.isclose(stored_exit[1].close, expected_exit_close, rel_tol=1e-9)

    updated_entry = [SpreadCandle(start_ts=entry[0].start_ts, open=0.1, high=0.2, low=0.0, close=0.15)]
    history.merge_external(
        "entry",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=300,
        candles=updated_entry,
    )
    merged_entry = history.get_candles(
        "entry",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=300,
    )
    assert len(merged_entry) == 2
    assert math.isclose(merged_entry[0].close, 0.15, rel_tol=1e-9)
