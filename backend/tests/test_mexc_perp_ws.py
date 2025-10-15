import gzip
import json

from arbitrage_scanner.connectors.mexc_perp import (
    _decode_ws_message,
    _extract_ask,
    _extract_bid,
    _iter_mexc_payloads,
    _parse_interval,
)


def test_iter_mexc_payloads_handles_channel_array():
    message = {
        "channel": "push.ticker",
        "data": [
            {
                "symbol": "BTC_USDT",
                "bid1": "30000",
                "ask1": "30010",
            }
        ],
    }

    items = list(_iter_mexc_payloads(message))

    assert items == [
        (
            "BTC_USDT",
            {
                "symbol": "BTC_USDT",
                "bid1": "30000",
                "ask1": "30010",
            },
        )
    ]


def test_iter_mexc_payloads_supports_params_message():
    message = {
        "method": "ticker.update",
        "params": [
            {
                "symbol": "ETH_USDT",
                "bestBid": "2100",
                "bestAsk": "2101",
            },
            True,
            "ETH_USDT",
        ],
    }

    items = list(_iter_mexc_payloads(message))

    assert items == [
        (
            "ETH_USDT",
            {
                "symbol": "ETH_USDT",
                "bestBid": "2100",
                "bestAsk": "2101",
            },
        )
    ]


def test_iter_mexc_payloads_extracts_funding_payload():
    message = {
        "channel": "push.funding_rate",
        "data": {
            "symbol": "DOGE_USDT",
            "fundingRate": "0.0001",
            "fundingInterval": 8,
        },
    }

    items = list(_iter_mexc_payloads(message))

    assert items == [
        (
            "DOGE_USDT",
            {
                "symbol": "DOGE_USDT",
                "fundingRate": "0.0001",
                "fundingInterval": 8,
            },
        )
    ]


def test_iter_mexc_payloads_skips_ack_messages():
    message = {"id": 1, "code": 0, "msg": "ok"}

    assert list(_iter_mexc_payloads(message)) == []


def test_extract_bid_ask_support_multiple_keys():
    payload = {
        "bestBidPrice": "123.45",
        "bestAskPrice": "123.56",
    }

    assert _extract_bid(payload) == 123.45
    assert _extract_ask(payload) == 123.56


def test_parse_interval_formats_hours():
    assert _parse_interval({"fundingInterval": 8}) == "8h"
    assert _parse_interval({"interval": "4h"}) == "4h"
    assert _parse_interval({}) == "8h"


def test_decode_ws_message_handles_gzip_bytes():
    payload = {"channel": "push.ticker", "data": {"symbol": "BTC_USDT"}}
    blob = json.dumps(payload).encode("utf-8")
    compressed = gzip.compress(blob)

    decoded = _decode_ws_message(compressed)

    assert decoded == payload
