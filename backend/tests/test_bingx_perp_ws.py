from arbitrage_scanner.connectors.bingx_perp import (
    _build_param_candidates,
    _extract_price,
    _extract_symbol,
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
            "BTC-USDT",
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
            "ETH-USDT",
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
            "BTC-USDT",
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
            "ETH-USDT",
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


def test_build_param_candidates_includes_hyphen_variants():
    candidates = _build_param_candidates({"BTC_USDT", "ETH_USDT"})

    assert {"symbol": "BTC_USDT,ETH_USDT"} in candidates
    assert {"symbol": "BTC-USDT,ETH-USDT"} in candidates
    assert {"symbol": "ALL"} in candidates


def test_extract_symbol_supports_symbol_name():
    payload = {"symbolName": "XRP-USDT"}

    assert _extract_symbol(payload, None, None) == "XRP-USDT"
