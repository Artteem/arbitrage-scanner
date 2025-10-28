from arbitrage_scanner.domain import Ticker
from arbitrage_scanner.store import TickerStore
from arbitrage_scanner.engine.spread_calc import compute_rows


def test_store_collects_all_exchange_quotes():
    store = TickerStore()
    now = 1_700_000_000.0
    entries = [
        ("binance", "BTC/USDT", 30000.0, 30000.5),
        ("bybit", "BTC-USDT", 30010.0, 30010.5),
        ("mexc", "BTC_USDT", 30005.0, 30005.5),
        ("bingx", "BTC-USDT", 30008.0, 30008.5),
        ("gate", "BTC_USDT", 30007.0, 30007.5),
    ]
    for exchange, symbol, bid, ask in entries:
        store.upsert_ticker(Ticker(exchange=exchange, symbol=symbol, bid=bid, ask=ask, ts=now))

    rows = compute_rows(
        store,
        symbols=["BTCUSDT"],
        exchanges=["binance", "bybit", "mexc", "bingx", "gate"],
        taker_fees={exchange: 0.0005 for exchange, *_ in entries},
    )

    exchanges_present = {(row.long_ex, row.short_ex) for row in rows}
    assert ("mexc", "binance") in exchanges_present
    assert ("bingx", "bybit") in exchanges_present
    assert ("gate", "mexc") in exchanges_present
    assert all(row.symbol == "BTCUSDT" for row in rows)
