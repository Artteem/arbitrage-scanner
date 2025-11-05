import gzip
import json

from arbitrage_scanner.connectors.bingx_perp import (
    _decode_ws_message,
    _iter_ws_payloads,
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


def test_decode_ws_message_handles_compressed_bytes():
    payload = {"dataType": "swap/ticker:BTC-USDT", "data": {"symbol": "BTC-USDT"}}
    blob = json.dumps(payload).encode("utf-8")
    compressed = gzip.compress(blob)

    decoded = _decode_ws_message(compressed)

    assert decoded == payload
