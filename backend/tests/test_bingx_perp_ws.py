from arbitrage_scanner.connectors.bingx_perp import (
    _extract_price,
    _from_bingx_symbol,
    _iter_ws_payloads,
)


def test_iter_ws_payloads_handles_snapshot_action():
    message = {
        "action": "snapshot",
        "arg": {"instId": "BTC-USDT"},
        "data": [
            {
                "symbol": "BTC-USDT",
                "bestBid": "63000",
                "bestAsk": "63010",
            }
        ],
    }

    items = list(_iter_ws_payloads(message))

    assert items == [
        (
            "BTCUSDT",
            {
                "symbol": "BTC-USDT",
                "bestBid": "63000",
                "bestAsk": "63010",
            },
        )
    ]


def test_iter_ws_payloads_skips_subscription_ack():
    message = {
        "action": "subscribe",
        "data": "success",
        "arg": {"instId": "BTC-USDT"},
    }

    assert list(_iter_ws_payloads(message)) == []


def test_iter_ws_payloads_supports_alternative_keys():
    message = {
        "action": "update",
        "tickers": [
            {
                "market": "ETH-USDT",
                "bid": "3500",
                "ask": "3501",
            }
        ],
    }

    items = list(_iter_ws_payloads(message))

    assert items == [
        (
            "ETHUSDT",
            {
                "market": "ETH-USDT",
                "bid": "3500",
                "ask": "3501",
            },
        )
    ]


def test_iter_ws_payloads_uses_data_type_symbol():
    message = {
        "dataType": "swap/ticker:BTC-USDT",
        "data": {
            "bp": "63000",
            "ap": "63010",
        },
    }

    items = list(_iter_ws_payloads(message))

    assert items == [
        (
            "BTCUSDT",
            {
                "bp": "63000",
                "ap": "63010",
            },
        )
    ]


def test_iter_ws_payloads_handles_list_data_type():
    message = {
        "dataType": ["swap/ticker:ETH-USDT"],
        "data": {
            "pair": "ETH-USDT",
            "bid1Price": "3500",
            "ask1Price": "3501",
        },
    }

    items = list(_iter_ws_payloads(message))

    assert items == [
        (
            "ETHUSDT",
            {
                "pair": "ETH-USDT",
                "bid1Price": "3500",
                "ask1Price": "3501",
            },
        )
    ]


def test_extract_price_supports_short_keys():
    payload = {"b": "123.45", "a": "123.55"}

    bid = _extract_price(payload, ("bestBid", "bid", "bp", "b"))
    ask = _extract_price(payload, ("bestAsk", "ask", "ap", "a"))

    assert bid == 123.45
    assert ask == 123.55


def test_from_bingx_symbol_strips_suffixes():
    assert _from_bingx_symbol("BTCUSDT_UMCBL") == "BTCUSDT"
    assert _from_bingx_symbol("eth-usdt-perp") == "ETHUSDT"
