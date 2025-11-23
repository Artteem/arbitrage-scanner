from __future__ import annotations

import asyncio
import json
import time
from typing import Sequence

import websockets
from ..domain import Symbol, Ticker
from ..store import TickerStore

from .mexc_rest import get_mexc_contracts


class MexcPerpConnector:
    def __init__(
        self,
        ws_endpoint: str = "wss://contract.mexc.com/edge",
        ping_interval: int = 10,
        ping_timeout: int = 5,
    ) -> None:
        self._endpoint = ws_endpoint
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._ws = None
        self._accumulator = ""
        self._store: TickerStore | None = None

    """
    run_mexc

    runs main connector cycle:
    - initializes websocket to user defined endpoint
    - configures ping-timer for socket by mexc protocol
    - reconnects to websocket if needed
    - setup all mexc message type handlers
    - handlers are implemented in a different class
    """
    async def run_mexc(self, store: TickerStore, symbols: Sequence[Symbol]):
        self._store = store
        self._is_running = True
        while self._is_running:
            ws = None
            keepalive = None
            try:
                ws = await self.get_connection()
                await self.init_connection(ws)
                keepalive = asyncio.create_task(self.keep_alive(ws))
                await self.handle_data(ws, store, symbols)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Reconnect loop handles errors; real implementation should log details.
                await asyncio.sleep(1)
            finally:
                if keepalive:
                    keepalive.cancel()
                    await asyncio.gather(keepalive, return_exceptions=True)
                if ws:
                    try:
                        await ws.close()
                    except Exception:
                        pass

    async def get_connection(self):
        # check if ws conenction object already exist (not null)
        # check if connection alive
        # if connection exists and alive - return current connection object
        # if connection does not exists or not alive - create ws connection to self._endpoint? save and return
        if self._ws and not getattr(self._ws, "closed", True):
            return self._ws
        self._ws = await websockets.connect(
            self._endpoint, ping_interval=self._ping_interval, ping_timeout=self._ping_timeout
        )
        return self._ws

    async def init_connection(self, ws):
        resolved = await self._resolve_mexc_symbols()
        await self.sub_on_tickers(ws, resolved)
        await self.sub_on_depth(ws, resolved)

    async def keep_alive(self, ws):
        raise NotImplementedError

    async def handle_data(self, ws, store: TickerStore, symbols: Sequence[Symbol]):
        async for raw in ws:
            for message in self._split_messages(raw):
                if message is None:
                    continue
                msg_type = self._detect_message_type(message)
                if msg_type == "pong":
                    await self._handle_pong(message)
                elif msg_type == "sub.ticker":
                    await self._handle_ticker(message, store)
                elif msg_type == "sub.depth":
                    await self._handle_depth(message, store)
                else:
                    continue

    async def sub_on_tickers(self, ws, symbols: Sequence[str]):
        for native in symbols:
            payload = {"method": "sub.ticker", "param": {"symbol": native}, "gzip": False}
            await ws.send(json.dumps(payload))

    async def sub_on_depth(self, ws, symbols: Sequence[str]):
        for native in symbols:
            payload = {"method": "sub.depth", "param": {"symbol": native}, "gzip": False}
            await ws.send(json.dumps(payload))

    def _as_float(self, value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    
    def _extract_bid(self, item) -> float:
        for key in ("bid1", "bestBidPrice", "bestBid"):
            val = item.get(key)
            bid = self._as_float(val)
            if bid > 0:
                return bid
        return 0.0


    def _extract_ask(self, item) -> float:
        for key in ("ask1", "bestAskPrice", "bestAsk"):
            val = item.get(key)
            ask = self._as_float(val)
            if ask > 0:
                return ask
        return 0.0

    def _split_messages(self, raw):
        if raw is None:
            return []
        if isinstance(raw, (bytes, bytearray)):
            try:
                text = raw.decode("utf-8")
            except Exception:
                return []
        else:
            text = str(raw)
        combined = (self._accumulator or "") + text
        if not combined:
            return []

        parts = combined.split("}{")
        segments: list[str] = []
        for idx, part in enumerate(parts):
            segment = part
            if idx > 0:
                segment = "{" + segment
            if idx < len(parts) - 1:
                segment = segment + "}"
            segments.append(segment)

        if not segments:
            return []

        # Handle potentially incomplete last segment
        tail = segments[-1]
        try:
            json.loads(tail)
            self._accumulator = ""
        except Exception:
            self._accumulator = tail
            segments = segments[:-1]

        messages: list[dict] = []
        for segment in segments:
            try:
                messages.append(json.loads(segment))
            except Exception:
                continue
        return messages

    def _detect_message_type(self, message: dict) -> str | None:
        if not isinstance(message, dict):
            return None
        method = str(message.get("method") or "").lower()
        channel = str(message.get("channel") or "").lower()
        if "pong" in message or method == "pong" or channel == "rs.pong":
            return "pong"
        if "depth" in method or "depth" in channel:
            return "sub.depth"
        if "ticker" in method or "ticker" in channel:
            return "sub.ticker"
        return None

    async def _handle_ticker(self, message: dict, store: TickerStore):
        #print("sub.ticker", json.dumps(message))
        payload = message.get("data") if isinstance(message, dict) else None
        if not isinstance(payload, dict):
            payload = message if isinstance(message, dict) else None
        if not isinstance(payload, dict):
            return

        symbol_raw = payload.get("symbol") or payload.get("s")
        symbol = Symbol(str(symbol_raw).replace("_", ""))
        if not symbol_raw:
            return

        bid = self._extract_bid(payload)
        ask = self._extract_ask(payload)
        if bid <= 0 or ask <= 0:
            return

        ts_raw = payload.get("ts") or payload.get("timestamp") or payload.get("time")
        try:
            ts_val = float(ts_raw)
            if ts_val > 1e12:
                ts_val /= 1000.0
        except (TypeError, ValueError):
            ts_val = time.time()

        store.upsert_ticker(
            Ticker(
                exchange="mexc",
                symbol=symbol,
                bid=bid,
                ask=ask,
                ts=ts_val
            )
        )

        last_price = payload.get("lastPrice")
        if last_price:
            store.set_last_price(
                "mexc",
                symbol,
                last_price=last_price,
                ts=ts_val,
            )

        try:
            rate_val = float(payload.get("fundingRate"))
        except (TypeError, ValueError):
            rate_val = None
        if rate_val is not None:
            store.upsert_funding(
                "mexc",
                symbol,
                rate=rate_val,
                interval="8h",
                ts=ts_val,
            )

    async def _handle_depth(self, message: dict, store: TickerStore):
        # extract asks and bids and upsert order book
        data = message.get("data") if isinstance(message, dict) else None
        if not isinstance(data, dict):
            return

        symbol_raw = data.get("symbol") or message.get("symbol")
        if not symbol_raw:
            return
        symbol = Symbol(str(symbol_raw).replace("_", ""))

        bids_raw = data.get("bids") or data.get("b") or data.get("buy")
        asks_raw = data.get("asks") or data.get("a") or data.get("sell")

        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []

        for entry in bids_raw or []:
            try:
                price = float(entry[0])
                size = float(entry[1])
                if price > 0 and size > 0:
                    bids.append((price, size))
            except Exception:
                continue

        for entry in asks_raw or []:
            try:
                price = float(entry[0])
                size = float(entry[1])
                if price > 0 and size > 0:
                    asks.append((price, size))
            except Exception:
                continue

        if not bids and not asks:
            return

        ts = time.time()
        store.upsert_order_book(
            "mexc",
            symbol,
            bids=bids or None,
            asks=asks or None,
            ts=ts,
        )

    async def _handle_pong(self, message: dict):
        print("pong", json.dumps(message))

    async def _resolve_mexc_symbols(self) -> list[str]:
        contracts = await get_mexc_contracts()
        symbols: list[str] = []
        for contract in contracts:
            if getattr(contract, "contract_type", "").lower() != "perp":
                continue
            if str(getattr(contract, "quote_asset", "")).upper() != "USDT":
                continue
            native = str(getattr(contract, "original_symbol", "")).upper()
            if not native:
                continue
            if native not in symbols:
                symbols.append(native)
        return symbols
