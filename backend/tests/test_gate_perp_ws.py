import asyncio
import gzip
import json

from arbitrage_scanner.connectors.gate_perp import (
    WS_ORDERBOOK_DEPTH,
    WS_ORDERBOOK_INTERVAL,
    _decode_ws_message,
    _handle_orderbook,
    _handle_ping,
    _handle_tickers,
    _perform_initial_ping,
    _send_subscriptions,
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


def test_gate_decode_ws_message_handles_gzip():
    payload = {"channel": "futures.tickers", "result": {"contract": "BTC_USDT"}}
    blob = json.dumps(payload).encode("utf-8")
    compressed = gzip.compress(blob)

    decoded = _decode_ws_message(compressed)

    assert decoded == payload


def test_gate_send_subscriptions_uses_documented_orderbook_payload():
    class DummyWS:
        def __init__(self):
            self.sent = []

        async def send(self, data):
            self.sent.append(json.loads(data))

    ws = DummyWS()
    asyncio.run(_send_subscriptions(ws, [("BTCUSDT", "BTC_USDT")]))

    depth_messages = [
        message
        for message in ws.sent
        if message.get("channel") == "futures.order_book"
    ]
    assert depth_messages
    depth_payload = depth_messages[0]["payload"]
    assert depth_payload == [
        "BTC_USDT",
        str(WS_ORDERBOOK_DEPTH),
        WS_ORDERBOOK_INTERVAL,
    ]


def test_gate_initial_ping_subscribes_and_resolves_ack():
    class DummyWS:
        def __init__(self, responses):
            self.sent = []
            self._responses = responses

        async def send(self, data):
            self.sent.append(json.loads(data))

        async def recv(self):
            if not self._responses:
                raise RuntimeError("no responses queued")
            return json.dumps(self._responses.pop(0))

    responses = [
        {"channel": "futures.ping", "event": "subscribe", "result": "success"}
    ]
    ws = DummyWS(responses)

    assert asyncio.run(_perform_initial_ping(ws)) is True
    assert ws.sent
    assert ws.sent[0]["channel"] == "futures.ping"
    assert ws.sent[0]["event"] == "subscribe"


def test_gate_handle_ping_replies_to_update_messages():
    class DummyWS:
        def __init__(self):
            self.sent = []

        async def send(self, data):
            self.sent.append(json.loads(data))

    ws = DummyWS()
    message = {"channel": "futures.ping", "event": "update"}

    handled = asyncio.run(_handle_ping(ws, message))

    assert handled is True
    assert ws.sent
    assert ws.sent[0]["event"] == "pong"
