from arbitrage_scanner.engine.spread_history import SpreadHistory


def create_history():
    return SpreadHistory(timeframes=(60, 300), max_candles=5)


def test_single_point_creates_candle():
    history = create_history()
    history.add_point(
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        entry_value=1.2,
        exit_value=-0.4,
        ts=65,
    )
    candles = history.get_candles(
        "entry",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=60,
    )
    assert len(candles) == 1
    candle = candles[0]
    assert candle.open == candle.close == 1.2
    assert candle.high == candle.low == 1.2


def test_updates_within_same_bucket():
    history = create_history()
    history.add_point(
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        entry_value=1.0,
        exit_value=0.5,
        ts=10,
    )
    history.add_point(
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        entry_value=2.0,
        exit_value=-1.0,
        ts=30,
    )
    history.add_point(
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        entry_value=0.5,
        exit_value=1.0,
        ts=59,
    )
    candles = history.get_candles(
        "entry",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=60,
    )
    assert len(candles) == 1
    candle = candles[0]
    assert candle.open == 1.0
    assert candle.close == 0.5
    assert candle.high == 2.0
    assert candle.low == 0.5

    exit_candles = history.get_candles(
        "exit",
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=60,
    )
    assert len(exit_candles) == 1
    exit_candle = exit_candles[0]
    assert exit_candle.open == 0.5
    assert exit_candle.close == 1.0
    assert exit_candle.high == 1.0
    assert exit_candle.low == -1.0


def test_new_period_creates_new_candles_and_limited_length():
    history = create_history()
    base_ts = 0
    for i in range(7):
        history.add_point(
            symbol="ETHUSDT",
            long_exchange="binance",
            short_exchange="bybit",
            entry_value=float(i),
            exit_value=float(-i),
            ts=base_ts + i * 60,
        )
    candles = history.get_candles(
        "entry",
        symbol="ETHUSDT",
        long_exchange="binance",
        short_exchange="bybit",
        timeframe=60,
    )
    assert len(candles) == 5
    assert candles[0].start_ts == 2 * 60
    assert candles[-1].close == 6.0
