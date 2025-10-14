import pytest

from arbitrage_scanner.connectors import discovery


class _DummyResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return [
            {"contract": "BTC_USDT", "state": "open", "in_delisting": False},
            {"contract": "ETH_USDT_20240628", "state": "open"},
            {"contract": "XRP_USDT", "state": "closed"},
            {"name": "SOL_USDT", "state": "trading"},
            {"contract": None},
            {"contract": "ADA_USDT", "in_delisting": True},
            {"symbol": "DOT_USDT", "state": "live"},
        ]


class _DummyClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None):
        assert url == discovery.GATE_CONTRACTS
        assert params is None
        return _DummyResponse()


@pytest.mark.asyncio
async def test_discover_gate_usdt_perp(monkeypatch):
    monkeypatch.setattr(discovery.httpx, "AsyncClient", _DummyClient)

    symbols = await discovery.discover_gate_usdt_perp()

    assert symbols == {"BTCUSDT", "SOLUSDT", "DOTUSDT"}
