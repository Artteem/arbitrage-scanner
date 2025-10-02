import asyncio
import contextlib

import httpx

from arbitrage_scanner.connectors import bingx_perp
from arbitrage_scanner.connectors.bingx_perp import (
    _build_param_candidates,
    _extract_price,
    _extract_symbol,
    _from_bingx_symbol,
    _iter_ws_payloads,
)
from arbitrage_scanner.store import TickerStore


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


def test_param_candidates_prioritize_all_queries():
    candidates = _build_param_candidates({"BTC_USDT"})

    assert candidates[0] == {"symbol": "ALL"}
    assert candidates[1] == {"pair": "ALL"}
    assert candidates[-1] is None


def test_poll_bingx_http_rotates_params_on_errors(monkeypatch):
    class _DummyResponse:
        def __init__(self, payload):
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class _DummyClient:
        def __init__(self, seen_pair_event):
            self.calls: list[tuple[str, dict | None]] = []
            self._attempt = 0
            self._seen_pair = seen_pair_event

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, params=None):
            self.calls.append((url, params))
            self._attempt += 1
            if self._attempt == 1:
                request = httpx.Request("GET", url)
                raise httpx.ConnectError("boom", request=request)
            if params == {"pair": "ALL"}:
                self._seen_pair.set()
            return _DummyResponse(
                {"data": [{"symbol": "BTC-USDT", "bestBid": "100", "bestAsk": "101"}]}
            )

    async def _run() -> tuple[TickerStore, _DummyClient]:
        seen_pair = asyncio.Event()
        dummy_client = _DummyClient(seen_pair)
        monkeypatch.setattr(bingx_perp.httpx, "AsyncClient", lambda *a, **kw: dummy_client)
        monkeypatch.setattr(bingx_perp, "TICKERS_URLS", ("https://example.com",))
        monkeypatch.setattr(bingx_perp, "POLL_INTERVAL", 0.01)
        monkeypatch.setattr(bingx_perp, "HTTP_RELAX_INTERVAL", 0.02)

        original_sleep = asyncio.sleep

        async def _fast_sleep(delay: float):
            await original_sleep(0)

        monkeypatch.setattr(bingx_perp.asyncio, "sleep", _fast_sleep)

        store = TickerStore()

        task = asyncio.create_task(
            bingx_perp._poll_bingx_http(store, ["BTCUSDT"], ws_ready=None, fetch_all=False)
        )
        await asyncio.wait_for(seen_pair.wait(), timeout=0.5)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        return store, dummy_client

    store, dummy_client = asyncio.run(_run())

    params_used = [params for (_, params) in dummy_client.calls]
    assert {"symbol": "ALL"} in params_used
    assert {"pair": "ALL"} in params_used

    ticker = store.get_ticker("bingx", "BTCUSDT")
    assert ticker is not None
    assert ticker.bid == 100.0
