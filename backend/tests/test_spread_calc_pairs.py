from arbitrage_scanner.domain import Ticker
from arbitrage_scanner.engine.spread_calc import compute_rows
from arbitrage_scanner.store import TickerStore


def _make_ticker(exchange: str, bid: float, ask: float) -> Ticker:
    return Ticker(exchange=exchange, symbol="BTCUSDT", bid=bid, ask=ask)


def test_compute_rows_includes_all_exchange_pairs():
    store = TickerStore()
    store.upsert_ticker(_make_ticker("binance", bid=100.0, ask=101.0))
    store.upsert_ticker(_make_ticker("bybit", bid=100.5, ask=101.5))
    store.upsert_ticker(_make_ticker("bingx", bid=100.8, ask=101.8))

    rows = compute_rows(
        store,
        symbols=["BTCUSDT"],
        exchanges=["binance", "bybit", "bingx"],
    )

    assert {row.long_ex for row in rows} == {"binance", "bybit", "bingx"}
    assert {row.short_ex for row in rows} == {"binance", "bybit", "bingx"}
    # Every ordered pair should be present: 3 exchanges -> 6 permutations.
    assert {(row.long_ex, row.short_ex) for row in rows} == {
        ("binance", "bybit"),
        ("binance", "bingx"),
        ("bybit", "binance"),
        ("bybit", "bingx"),
        ("bingx", "binance"),
        ("bingx", "bybit"),
    }
    assert len(rows) == 6


