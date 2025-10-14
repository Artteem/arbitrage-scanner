from arbitrage_scanner.connectors.bingx_perp import (
    _iter_ws_payloads,
    _parse_funding_interval,
)


def test_iter_ws_payloads_parses_ticker_message():
    wanted = {"BTCUSDT"}
    message = {
        "dataType": "swap/ticker:BTC-USDT",
        "data": {
            "symbol": "BTC-USDT",
            "bestBid": "30000",
            "bestAsk": "30010",
        },
    }

    items = list(_iter_ws_payloads(message, wanted))

    assert items
    symbol, payload = items[0]
    assert symbol == "BTCUSDT"
    assert payload["bestBid"] == "30000"


def test_iter_ws_payloads_parses_funding_message():
    wanted = {"ETHUSDT"}
    message = {
        "dataType": "swap/fundingRate:ETH-USDT",
        "data": {
            "symbol": "ETH-USDT",
            "fundingRate": "0.0001",
            "fundingInterval": 8,
        },
    }

    items = list(_iter_ws_payloads(message, wanted))

    assert items
    symbol, payload = items[0]
    assert symbol == "ETHUSDT"
    assert payload["fundingRate"] == "0.0001"


def test_parse_funding_interval_formats_numeric():
    assert _parse_funding_interval({"fundingInterval": 8}) == "8h"
    assert _parse_funding_interval({"interval": "4h"}) == "4h"
    assert _parse_funding_interval({}) == "8h"
