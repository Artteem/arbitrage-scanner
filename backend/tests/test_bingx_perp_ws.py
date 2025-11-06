import gzip
import json

from arbitrage_scanner.connectors.bingx_perp import (
    _BingxWsClient,
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


class _DummyStore:
    def upsert_order_book(self, *args, **kwargs):
        return None

    def upsert_ticker(self, *args, **kwargs):
        return None


def test_ws_client_drops_unsupported_topics():
    store = _DummyStore()
    client = _BingxWsClient(
        store,
        ticker_pairs=[("BTCUSDT", "BTC-USDT")],
        depth_symbols=["BTC-USDT"],
        filter_symbols=None,
    )

    depth_id = next(
        sub_id for sub_id in client._active_sub_ids if "sub_depth" in sub_id
    )

    error_message = {
        "code": 80015,
        "msg": "symbol:BTC-USDT is not supported in websocket",
        "id": depth_id,
        "dataType": "",
    }

    client._handle_bingx_message(error_message)

    assert depth_id not in client._active_sub_ids
    assert all(
        str(payload.get("id")) != depth_id
        for payload in client._active_subs.values()
        if isinstance(payload, dict)
    )
    # ticker subscription should remain intact
    assert any(key.startswith("ticker:") for key in client._active_subs)
