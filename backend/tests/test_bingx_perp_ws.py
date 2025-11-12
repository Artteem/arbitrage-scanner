import asyncio
import gzip
import json

from arbitrage_scanner.connectors.bingx_perp import (
    _decode_ws_message,
    _iter_ws_payloads,
)
from arbitrage_scanner.connectors import bingx_perp
from arbitrage_scanner.store import TickerStore


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


def test_run_bingx_uses_fallback_when_symbol_map_fails(monkeypatch):
    created_clients: list[object] = []

    class DummyClient:
        def __init__(
            self,
            store: TickerStore,
            *,
            ticker_pairs,
            depth_symbols,
            filter_symbols,
        ) -> None:
            self.ticker_pairs = list(ticker_pairs)
            self.depth_symbols = list(depth_symbols)
            self.filter_symbols = filter_symbols
            created_clients.append(self)

        async def run(self) -> None:
            return

        async def stop(self) -> None:
            return

    async def failing_symbol_map():
        raise RuntimeError("boom")

    async def empty_discovery():
        return set()

    monkeypatch.setattr(bingx_perp, "_load_bingx_ws_symbol_map", failing_symbol_map)
    monkeypatch.setattr(bingx_perp, "discover_bingx_usdt_perp", empty_discovery)
    monkeypatch.setattr(bingx_perp, "_BingxWsClient", DummyClient)
    monkeypatch.setattr(bingx_perp, "_BINGX_WS_SYMBOL_CACHE", None, raising=False)
    monkeypatch.setattr(bingx_perp, "_BINGX_WS_SYMBOL_CACHE_TS", 0.0, raising=False)

    store = TickerStore()

    asyncio.run(bingx_perp.run_bingx(store, symbols=[]))

    assert created_clients, "websocket client should be created when falling back"
    # Ensure fallback symbols were used to create subscriptions
    normalized_pairs = {
        common for client in created_clients for common, _ in client.ticker_pairs
    }
    assert {
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
    }.issubset(normalized_pairs)
