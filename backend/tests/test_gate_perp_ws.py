from arbitrage_scanner.connectors.gate_perp import (
    _handle_orderbook,
    _handle_tickers,
)
from arbitrage_scanner.store import TickerStore


def test_gate_handle_tickers_updates_store():
    store = TickerStore()
    payload = {
        "contract": "BTC_USDT",
        "best_bid": "30000",
        "best_ask": "30010",
        "funding_rate": "0.0001",
        "last": "30005",
    }

    _handle_tickers(store, payload)

    ticker = store.get_ticker("gate", "BTCUSDT")
    assert ticker is not None
    assert ticker.bid == 30000
    assert ticker.ask == 30010

    funding = store.get_funding("gate", "BTCUSDT")
    assert funding is not None
    assert funding.rate == 0.0001

    order_book = store.get_order_book("gate", "BTCUSDT")
    assert order_book is not None
    assert order_book.last_price == 30005


def test_gate_handle_orderbook_updates_depth():
    store = TickerStore()
    payload = {
        "contract": "ETH_USDT",
        "bids": [["2000", "5"], ["1999.5", "3"]],
        "asks": [["2001", "4"], ["2001.5", "2"]],
        "last": "2000.5",
    }

    _handle_orderbook(store, payload)

    order_book = store.get_order_book("gate", "ETHUSDT")
    assert order_book is not None
    assert order_book.bids[0] == (2000.0, 5.0)
    assert order_book.asks[0] == (2001.0, 4.0)
    assert order_book.last_price == 2000.5
