import pytest

from arbitrage_scanner.connectors import discovery
from arbitrage_scanner.connectors.bingx_utils import normalize_bingx_symbol


class _DummyResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {
            "data": [
                {"symbol": "BTC-USDT", "quoteAsset": "usdt"},
                {"market": "ETH-USDT", "quoteAsset": "USDT"},
                {"instId": "LTC-USDC", "quoteAsset": "USDC"},
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
        assert url == discovery.BINGX_CONTRACTS
        assert params is None
        return _DummyResponse()


@pytest.mark.asyncio
async def test_discover_bingx_usdt_perp(monkeypatch):
    monkeypatch.setattr(discovery.httpx, "AsyncClient", _DummyClient)

    symbols = await discovery.discover_bingx_usdt_perp()

    assert symbols == {"BTCUSDT", "ETHUSDT"}


def test_normalize_bingx_symbol_removes_suffixes():
    assert normalize_bingx_symbol("btc-usdt-umcbl") == "BTCUSDT"
    assert normalize_bingx_symbol("LTCUSDC_PERP") == "LTCUSDC"
