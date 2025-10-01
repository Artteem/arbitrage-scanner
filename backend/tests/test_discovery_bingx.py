import pytest

from arbitrage_scanner.connectors import discovery


class _DummyResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {
            "data": [
                {"symbol": "BTC-USDT"},
                {"market": "ETH-USDT"},
                {"instId": "LTC-USDC"},
            ]
        }


class _DummyClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None):
        assert url == discovery.BINGX_TICKERS
        assert params == {"symbol": "ALL"}
        return _DummyResponse()


@pytest.mark.asyncio
async def test_discover_bingx_usdt_perp(monkeypatch):
    monkeypatch.setattr(discovery.httpx, "AsyncClient", _DummyClient)

    symbols = await discovery.discover_bingx_usdt_perp()

    assert symbols == {"BTCUSDT", "ETHUSDT"}
