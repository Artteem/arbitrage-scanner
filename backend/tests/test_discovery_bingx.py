import asyncio

import httpx

from arbitrage_scanner.connectors import discovery
from arbitrage_scanner.connectors.bingx_utils import normalize_bingx_symbol


class _DummyResponse:
    def __init__(self, url: str, payload, status: int = 200):
        self._payload = payload
        self.status_code = status
        self._request = httpx.Request("GET", url)
        self._response = httpx.Response(status_code=status, request=self._request)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("error", request=self._request, response=self._response)

    def json(self):
        return self._payload


class _DummyClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None):
        self.calls.append((url, params))
        if not self._responses:
            raise RuntimeError("No more responses queued")
        payload, status = self._responses.pop(0)
        return _DummyResponse(url, payload, status=status)


def _make_success_payload():
    return {
        "data": [
            {"symbol": "BTC-USDT"},
            {"market": "ETH-USDT"},
            {"instId": "LTC-USDC"},
        ]
    }


def test_discover_bingx_usdt_perp(monkeypatch):
    client = _DummyClient(responses=[(_make_success_payload(), 200)])

    class _Factory:
        def __call__(self, *args, **kwargs):
            return client

    monkeypatch.setattr(discovery.httpx, "AsyncClient", _Factory())

    symbols = asyncio.run(discovery.discover_bingx_usdt_perp())

    assert symbols == {"BTCUSDT", "ETHUSDT"}
    assert client.calls


def test_discover_bingx_usdt_perp_falls_back(monkeypatch):
    error_response = ({}, 403)
    success_response = (_make_success_payload(), 200)
    client = _DummyClient(responses=[error_response, success_response])

    class _Factory:
        def __call__(self, *args, **kwargs):
            return client

    monkeypatch.setattr(discovery.httpx, "AsyncClient", _Factory())

    symbols = asyncio.run(discovery.discover_bingx_usdt_perp())

    assert symbols == {"BTCUSDT", "ETHUSDT"}
    assert len(client.calls) >= 2


def test_normalize_bingx_symbol_removes_suffixes():
    assert normalize_bingx_symbol("btc-usdt-umcbl") == "BTCUSDT"
    assert normalize_bingx_symbol("LTCUSDC_PERP") == "LTCUSDC"
