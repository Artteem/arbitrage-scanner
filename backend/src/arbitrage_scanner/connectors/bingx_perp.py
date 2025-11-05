from __future__ import annotations

import asyncio, json, gzip, io, time, random
import logging
from typing import Any, Dict, Iterable, List, Sequence, Tuple
import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError
from ..domain import Symbol, Ticker
from ..store import TickerStore
from .bingx_utils import normalize_bingx_symbol
from .credentials import ApiCreds
from .discovery import discover_bingx_usdt_perp

logger = logging.getLogger(__name__)

WS_ENDPOINTS = (
    "wss://open-api.bingx.com/market",
    "wss://open-api-swap.bingx.com/standard/market?compress=false",
    "wss://open-api-ws.bingx.com/market?compress=false",
)
MAX_TOPICS_PER_CONN = 100
MAX_TOPICS_PER_SUB_MSG = 30
WS_SUB_DELAY = 0.1
HEARTBEAT_INTERVAL = 20.0
WS_RECONNECT_INITIAL = 1.0
WS_RECONNECT_MAX = 60.0
MIN_SYMBOL_THRESHOLD = 1
FALLBACK_SYMBOLS: tuple[Symbol, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
_SUBSCRIPTION_LOG_LIMIT = 20
_WS_PAYLOAD_LOG_LIMIT = 20
_LOG_ONCE_TICKERS: set[Symbol] = set()
_WS_PAYLOAD_LOG_COUNT = 0
BINGX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Origin": "https://bingx.com",
    "Referer": "https://bingx.com/",
    "Accept": "application/json, text/plain, */*",
}

def _is_bingx_ack(msg: dict) -> bool:
    """Return True if the message is an acknowledgement frame."""
    return (
        isinstance(msg, dict)
        and msg.get("code") == 0
        and "data" not in msg
        and "dataType" not in msg
    )
async def _resolve_bingx_symbols(symbols: Sequence[Symbol]) -> tuple[list[Symbol], bool]:
    requested: list[Symbol] = []
    seen: set[str] = set()
    for symbol in symbols:
        if not symbol:
            continue
        normalized = _normalize_common_symbol(symbol)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        requested.append(normalized)
    discovered: set[str] = set()
    try:
        discovered = await discover_bingx_usdt_perp()
    except Exception:
        discovered = set()
    if discovered:
        discovered_normalized = {_normalize_common_symbol(sym) for sym in discovered}
        filtered: list[Symbol] = []
        used: set[str] = set()
        for symbol in requested:
            if symbol in discovered_normalized and symbol not in used:
                filtered.append(symbol)
                used.add(symbol)
        if filtered:
            return filtered, False
        return sorted(discovered_normalized), False
    if not requested:
        return list(FALLBACK_SYMBOLS), True
    return requested, False
def _log_ws_subscriptions(kind: str, topics: Sequence[str]) -> None:
    if not topics:
        return
    preview = list(topics[:_SUBSCRIPTION_LOG_LIMIT])
    extra = len(topics) - len(preview)
    if extra > 0:
        logger.info(
            "BingX WS %s subscriptions: %s (+%d more)",
            kind,
            preview,
            extra,
        )
    else:
        logger.info("BingX WS %s subscriptions: %s", kind, preview)
def _log_ws_payload_received(symbol: Symbol) -> None:
    global _WS_PAYLOAD_LOG_COUNT
    if _WS_PAYLOAD_LOG_COUNT >= _WS_PAYLOAD_LOG_LIMIT:
        return
    _WS_PAYLOAD_LOG_COUNT += 1
    logger.info("BingX WS payload received for %s", symbol)
def _log_first_ticker(symbol: Symbol, bid: float, ask: float) -> None:
    target = symbol.upper()
    if target not in {"BTCUSDT", "ETHUSDT"}:
        return
    if target in _LOG_ONCE_TICKERS:
        return
    _LOG_ONCE_TICKERS.add(target)
    logger.info("BingX first ticker for %s: bid=%s ask=%s", target, bid, ask)
def _extract_price(item: dict, keys: Iterable[str]) -> float:
    for key in keys:
        val = item.get(key)
        if val is None:
            continue
        try:
            price = float(val)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return 0.0
def _normalize_common_symbol(symbol: Symbol) -> str:
    normalized = normalize_bingx_symbol(symbol)
    if normalized:
        return normalized
    sym = str(symbol).upper()
    return sym.replace("-", "").replace("_", "")
def _collect_wanted_common(symbols: Sequence[Symbol]) -> set[str]:
    wanted: set[str] = set()
    for symbol in symbols:
        if not symbol:
            continue
        normalized = normalize_bingx_symbol(symbol)
        if normalized:
            wanted.add(str(normalized))
            continue
        fallback = _normalize_common_symbol(str(symbol))
        if fallback:
            wanted.add(fallback)
    return wanted
def _chunk_bingx_symbols(symbols: Sequence[Symbol], size: int) -> Iterable[Sequence[Symbol]]:
    for idx in range(0, len(symbols), size):
        yield symbols[idx : idx + size]
def _chunk_list(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]
def _to_bingx_symbol(symbol: Symbol) -> str:
    sym = str(symbol).upper()
    if "-" in sym:
        sym = sym.replace("-", "_")
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        base = sym[:-4]
        return f"{base}_USDT"
    if sym.endswith("USDC"):
        base = sym[:-4]
        return f"{base}_USDC"
    if sym.endswith("USD"):
        base = sym[:-3]
        return f"{base}_USD"
    return sym


def _to_bingx_ws_symbol(symbol: Symbol) -> str:
    sym = _normalize_common_symbol(symbol)
    for quote in ("USDT", "USDC", "USD", "BUSD", "FDUSD"):
        if sym.endswith(quote):
	@@ -215,10 +221,9 @@ def _from_bingx_symbol(symbol: str | None) -> str | None:


async def run_bingx(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    # 1. Получаем список символов, ИГНОРИРУЯ флаг subscribe_all
    subscribe, _ = await _resolve_bingx_symbols(symbols) # [CI: 79]

    # 2. Проверяем, что список символов не пуст
    if not subscribe:
        logger.warning("No symbols resolved for BingX connector; skipping startup")
        return
    chunks = [
        tuple(chunk) for chunk in _chunk_bingx_symbols(subscribe, MAX_TOPICS_PER_CONN)
    ]

    # 3. Блок 'if subscribe_all:' ПОЛНОСТЬЮ УДАЛЕН.
    #    Оставили только цикл по 'chunks'.

    while True:
        tasks: list[asyncio.Task] = []
        clients: list[_BingxWsClient] = []

        try:
            # 4. Цикл for chunk in chunks: теперь единственный, кто создает клиенты
            for chunk in chunks:
                wanted_common = _collect_wanted_common(chunk)
                if not wanted_common:
                    continue
                symbol_pairs: list[tuple[str, str]] = []
                for sym in sorted(wanted_common):
                    native = _to_bingx_ws_symbol(sym)
                    if not native:
                        continue
                    symbol_pairs.append((sym, native))
                if not symbol_pairs:
                    continue
                native_symbols = [
                    native for _, native in symbol_pairs if native.upper() != 'ALL'
                ]
                client = _BingxWsClient(
                    store,
                    ticker_pairs=symbol_pairs,
                    depth_symbols=native_symbols,
                    funding_symbols=native_symbols,
                    filter_symbols=wanted_common,
                )
                clients.append(client)
                tasks.append(asyncio.create_task(client.run()))
            if not tasks:
                return
            await asyncio.gather(*tasks)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('BingX websocket workers crashed; restarting')
            await asyncio.sleep(WS_RECONNECT_INITIAL)
        finally:
            for client in clients:
                await client.stop()
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
class _BingxWsClient:
    def __init__(
        self,
        store: TickerStore,
        *,
        ticker_pairs: Sequence[tuple[str, str]],
        depth_symbols: Sequence[str],
        funding_symbols: Sequence[str],
        filter_symbols: Iterable[str] | None,
    ) -> None:
        self.log = logger
        self._store = store
        self._ticker_pairs = list(ticker_pairs)
        self._depth_symbols = list(depth_symbols)
        self._funding_symbols = list(funding_symbols)
        self._running = True
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._rx_task: asyncio.Task | None = None
        self._hb_task: asyncio.Task | None = None
        self._last_rx_ts = 0.0
        self._active_subs: Dict[str, Dict[str, Any]] = {}
        self.ws_url = WS_ENDPOINTS[0]
        if filter_symbols:
            wanted = {str(sym) for sym in filter_symbols if sym}
            self._wanted_common = wanted or None
        else:
            wanted = {
                common for common, _ in self._ticker_pairs if common and common != 'ALL'
            }
            self._wanted_common = wanted or None
        self._prepare_subscriptions()
    async def run(self) -> None:
        try:
            await self._run_ws_forever()
        finally:
            await self._close_ws()
    async def stop(self) -> None:
        self._running = False
        await self._close_ws()
    async def _run_ws_forever(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                await self._open_ws()
                await self._resubscribe_all()
                backoff = 1.0
                await self._join_ws_tasks()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning('BingX WS disconnected: %s', repr(exc))
            finally:
                await self._close_ws()
            if not self._running:
                break
            await asyncio.sleep(backoff + random.uniform(0, 0.75))
            backoff = min(backoff * 2, 30.0)
    async def _open_ws(self) -> None:
        last_error: Exception | None = None
        for endpoint in WS_ENDPOINTS:
            try:
                self.ws_url = endpoint
                self.log.info('Connecting to BingX WS endpoint %s', endpoint)
                self._ws = await websockets.connect(
                    endpoint,
                    ping_interval=None,
                    ping_timeout=None,
                    max_size=32 * 1024 * 1024,
                    compression=None,
                    close_timeout=3,
                    read_limit=2**20,
                    max_queue=1024,
                    extra_headers=BINGX_HEADERS,
                )
                self._rx_task = asyncio.create_task(self._ws_reader())
                self._hb_task = asyncio.create_task(self._ws_heartbeat())
                return
            except Exception as exc:
                last_error = exc
                self.log.warning(
                    'Failed to connect to BingX endpoint %s: %s', endpoint, repr(exc)
                )
        if last_error:
            raise last_error
    async def _close_ws(self) -> None:
        tasks = [task for task in (self._rx_task, self._hb_task) if task is not None]
        self._rx_task = None
        self._hb_task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
    async def _join_ws_tasks(self) -> None:
        tasks = [task for task in (self._rx_task, self._hb_task) if task is not None]
        if not tasks:
            return
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._rx_task = None
            self._hb_task = None
    async def _resubscribe_all(self) -> None:
        if not self._ws:
            return
        for key, payload in self._active_subs.items():
            try:
                await self._ws.send(json.dumps(payload))
                self.log.info(
                    'BingX WS subscribe send %s -> %s',
                    key,
                    json.dumps(payload)[:200],
                )
                await asyncio.sleep(0.01)
            except Exception as exc:
                self.log.error('Resubscribe failed for %s: %s', key, exc)

    async def _ws_reader(self) -> None:
        assert self._ws is not None
        try:
            while self._running:
                raw = await self._ws.recv()
                self._log_raw_frame(raw)
                txt = self._maybe_gunzip(raw)
                self._last_rx_ts = time.time()

                msg = None
                if txt:
                    try:
                        msg = json.loads(txt)
                    except Exception:
                        self.log.debug('BingX WS TXT: %s', txt[:200])

                sample_source = msg if msg is not None else txt
                if sample_source is not None and int(time.time()) % 10 == 0:
                    self.log.debug('WS RX sample: %s', str(sample_source)[:300])

                if isinstance(msg, dict):
                    if 'ping' in msg:
                        pong = {'pong': msg.get('ping')}
                        await self._ws.send(json.dumps(pong))
                        continue
                    if (
                        msg.get('event') == 'ping'
                        or msg.get('op') == 'ping'
                        or msg.get('reqType') == 'ping'
                    ):
                        pong = {
                            'event': 'pong',
                            'op': 'pong',
                            'reqType': 'pong',
                            'ts': msg.get('ts') or int(time.time() * 1000),
                        }
                        await self._ws.send(json.dumps(pong))
                        continue
                    if msg.get('event') == 'pong' or 'pong' in msg:
                        continue
                self._handle_bingx_message(msg if msg is not None else txt)
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, ConnectionClosedError):
            raise
        except Exception as exc:
            self.log.exception('BingX WS reader failure: %s', exc)
            raise

    async def _ws_heartbeat(self) -> None:
        WS_PING_EVERY = 20
        APP_PING_EVERY = 40
        last_app_ping = 0.0

        try:
            while self._running:
                if not self._ws:
                    await asyncio.sleep(WS_PING_EVERY)
                    continue
                try:
                    pong_waiter = await self._ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=10)
                except Exception as exc:
                    self.log.error('bingx heartbeat failed: %s', repr(exc))
                    raise

                now = time.time()
                if now - last_app_ping > APP_PING_EVERY:
                    try:
                        await self._ws.send(
                            json.dumps({'event': 'ping', 'ts': int(now * 1000)})
                        )
                        last_app_ping = now
                    except Exception:
                        pass

                await asyncio.sleep(WS_PING_EVERY)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise

    def _prepare_subscriptions(self) -> None:
        if self._ticker_pairs:
            seen: set[str] = set()
            for common, native in self._ticker_pairs:
                if not native:
                    continue
                if str(native).upper() == 'ALL':
                    continue
                normalized = native.replace('_', '-')
                if normalized in seen:
                    continue
                seen.add(normalized)
                payload = {
                    'operation': 'subscribe',
                    'channel': 'swap/ticker',
                    'symbol': normalized,
                }
                _log_ws_subscriptions('ticker', [normalized])
                self.log.info('BingX subscribe ticker -> %s (native=%s)', common, normalized)
                key = f'ticker:{common or normalized}'
                self._remember_sub(key, payload)

        if self._depth_symbols:
            seen_depth: set[str] = set()
            for sym in self._depth_symbols:
                if not sym or sym.upper() == 'ALL':
                    continue
                topic = sym.replace('_', '-')
                if topic in seen_depth:
                    continue
                seen_depth.add(topic)
                payload = {
                    'operation': 'subscribe',
                    'channel': 'swap/depth5',
                    'symbol': topic,
                }
                _log_ws_subscriptions('depth', [topic])
                self.log.info('BingX subscribe depth -> %s', topic)
                self._remember_sub(f'depth:{topic}', payload)

        if self._funding_symbols:
            seen_funding: set[str] = set()
            for sym in self._funding_symbols:
                if not sym or sym.upper() == 'ALL':
                    continue
                topic = sym.replace('_', '-')
                if topic in seen_funding:
                    continue
                seen_funding.add(topic)
                payload = {
                    'operation': 'subscribe',
                    'channel': 'swap/fundingRate',
                    'symbol': topic,
                }
                _log_ws_subscriptions('funding', [topic])
                self.log.info('BingX subscribe funding -> %s', topic)
                self._remember_sub(f'funding:{topic}', payload)

    def _remember_sub(self, key: str, payload: Dict[str, Any]) -> None:
        self._active_subs[key] = payload
    def _log_raw_frame(self, raw: Any) -> None:
        if isinstance(raw, (bytes, bytearray)):
            buf = bytes(raw[:512])
            try:
                text_preview = buf.decode('utf-8', 'replace')
            except Exception:
                text_preview = ''
            if text_preview.strip():
                self.log.debug('WS RX raw (first 512b): %s', text_preview)
            else:
                self.log.debug('WS RX raw (first 512b hex): %s', buf.hex())
        else:
            self.log.debug('WS RX raw (first 512b): %s', str(raw)[:512])
    def _maybe_gunzip(self, payload: Any) -> str:
        if isinstance(payload, (bytes, bytearray)):
            b = bytes(payload)
            if len(b) >= 2 and b[0] == 0x1F and b[1] == 0x8B:
                try:
                    with gzip.GzipFile(fileobj=io.BytesIO(b)) as gz:
                        return gz.read().decode('utf-8', 'replace')
                except Exception:
                    try:
                        return b.decode('utf-8', 'replace')
                    except Exception:
                        return ''
            else:
                try:
                    return b.decode('utf-8', 'replace')
                except Exception:
                    return ''
        if isinstance(payload, str):
            return payload
        try:
            return json.dumps(payload, ensure_ascii=False)
        except Exception:
            return ''
    def _handle_bingx_message(self, message: Any) -> None:
        if isinstance(message, list):
            for item in message:
                self._handle_bingx_message(item)
            return
        if isinstance(message, str):
            try:
                parsed = json.loads(message)
            except Exception:
                self.log.debug('BingX WS TXT: %s', message[:200])
                return
            self._handle_bingx_message(parsed)
            return
        if not isinstance(message, dict):
            return
        if _is_bingx_ack(message):
            self.log.info('BingX WS ack: %s', str(message)[:500])
            return
        code = message.get('code') if isinstance(message.get('code'), (int, float)) else None
        if code and code != 0:
            channel = message.get('dataType') or message.get('channel')
            self.log.error(
                'BingX WS error (channel=%s): %s', channel, str(message)[:500]
            )
            return
        data_type = str(message.get('dataType') or '').lower()
        now = time.time()
        for common_symbol, payload in _iter_ws_payloads(message, self._wanted_common):
            if not payload:
                continue

            if 'fundingrate' in data_type:
                rate = _extract_price(
                    payload,
                    (
                        'fundingRate',
                        'funding_rate',
                        'rate',
                        'value',
                    ),
                )
                interval = _parse_funding_interval(payload)
                self._store.upsert_funding(
                    'bingx', common_symbol, rate=rate, interval=interval, ts=now
                )
                continue

            if 'depth' in data_type or (
                isinstance(payload, dict)
                and (payload.get('bids') or payload.get('asks'))
            ):
                bids, asks, last_price = _extract_depth_payload(payload)
                if not bids and not asks and last_price is None:
                    continue
                self._store.upsert_order_book(
                    'bingx',
                    common_symbol,
                    bids=bids or None,
                    asks=asks or None,
                    ts=now,
                    last_price=last_price,
                )
                continue

            bid = _extract_price(
                payload,
                (
                    'bestBid',
                    'bestBidPrice',
                    'bid',
                    'bidPrice',
                    'bid1',
                    'bid1Price',
                    'bp',
                    'bidPx',
                    'bestBidPx',
                    'b',
                    'buyPrice',
                ),
            )
            ask = _extract_price(
                payload,
                (
                    'bestAsk',
                    'bestAskPrice',
                    'ask',
                    'askPrice',
                    'ask1',
                    'ask1Price',
                    'ap',
                    'askPx',
                    'bestAskPx',
                    'a',
                    'sellPrice',
                ),
            )
            if bid <= 0 or ask <= 0:
                continue
            self._store.upsert_ticker(
                Ticker(
                    exchange='bingx',
                    symbol=common_symbol,
                    bid=bid,
                    ask=ask,
                    ts=now,
                )
            )
            _log_first_ticker(common_symbol, bid, ask)

            last_price = _extract_price(
                payload,
                (
                    'lastPrice',
                    'last',
                    'close',
                    'px',
                ),
            )
            if last_price > 0:
                self._store.upsert_order_book(
                    'bingx',
                    common_symbol,
                    last_price=last_price,
                    last_price_ts=now,
                )
def _iter_ws_payloads(
    message: dict,
    wanted_common: set[str] | None = None,
) -> Iterable[tuple[str, dict]]:
    if not isinstance(message, dict):
        logger.debug(
            "BingX WS message ignored: unexpected type %s", type(message).__name__
        )
        return []
    action = message.get("action")
    if isinstance(action, str):
        normalized = action.strip().lower()
        if normalized in {"subscribe", "sub", "unsubscribe", "unsub", "error"}:
            return []
    payload = message.get("data")
    if payload is None:
        for key in ("tickers", "items", "result"):
            cand = message.get(key)
            if cand is not None:
                payload = cand
                break
    default_symbol: str | None = None

    arg = message.get("arg")
    if isinstance(arg, dict):
        candidate = arg.get("instId") or arg.get("symbol") or arg.get("symbols")
        if isinstance(candidate, list):
            candidate = candidate[0] if candidate else None
        if isinstance(candidate, str):
            default_symbol = candidate
    topic_symbol = _extract_topic_symbol(message.get("dataType"))
    if topic_symbol:
        default_symbol = topic_symbol

    accepted: list[tuple[str, dict]] = []
    for raw_symbol, payload_item in _iter_payload_items(payload, default_symbol):
        if not isinstance(payload_item, dict):
            logger.debug(
                "BingX WS drop %r: payload is not a dict (got %s)",
                raw_symbol,
                type(payload_item).__name__,
            )
            continue

        if not raw_symbol:
            logger.debug(
                "BingX WS drop payload without symbol: keys=%s",
                list(payload_item.keys())[:5],
            )
            continue
        common_symbol = _from_bingx_symbol(raw_symbol)
        logger.info(
            "WS PARSE exchange=%s native=%s -> common=%s",
            "bingx",
            raw_symbol,
            common_symbol,
        )
        if not common_symbol:
            logger.warning(
                "WS DROP reason=normalize_none exchange=%s native=%s payload=%s",
                "bingx",
                raw_symbol,
                str(message)[:500],
            )
            continue
        normalized_common = str(common_symbol)
        if wanted_common and normalized_common not in wanted_common:
            logger.debug(
                "BingX WS drop %s: symbol not requested", normalized_common
            )
            continue
        _log_ws_payload_received(normalized_common)
        accepted.append((normalized_common, payload_item))
    return accepted
def _extract_depth_payload(payload: dict) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]], float | None]:
    if not isinstance(payload, dict):
        return [], [], None
    containers: List[dict] = [payload]
    for key in ("depth", "depths", "orderbook", "book", "tick", "snapshot", "data"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            containers.append(nested)
    bids: List[Tuple[float, float]] = []
    asks: List[Tuple[float, float]] = []
    last_price: float | None = None
    for container in containers:
        if not isinstance(container, dict):
            continue

        bids_candidate = _collect_depth_levels(
            container,
            (
                "bids",
                "bid",
                "buy",
                "buys",
                "buyDepth",
                "buyLevels",
                "buyList",
                "bp",
            ),
        )
        asks_candidate = _collect_depth_levels(
            container,
            (
                "asks",
                "ask",
                "sell",
                "sells",
                "sellDepth",
                "sellLevels",
                "sellList",
                "ap",
            ),
        )
        if bids_candidate:
            bids = bids_candidate
        if asks_candidate:
            asks = asks_candidate
        if last_price is None:
            last_price = _extract_last_price(container)
    return bids[:20], asks[:20], last_price
def _collect_depth_levels(container: dict, keys: Sequence[str]) -> List[Tuple[float, float]]:
    for key in keys:
        if key not in container:
            continue
        levels = container.get(key)
        parsed = list(_iter_levels(levels))
        if parsed:
            return parsed
    return []

def _extract_last_price(container: dict) -> float | None:
    for key in (
        "lastPrice",
        "last_price",
        "last",
        "price",
        "close",
        "tradePrice",
        "markPrice",
    ):
        val = container.get(key)
        if val is None:
            continue
        try:
            price = float(val)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return None
def _decode_ws_message(message: str | bytes | bytearray) -> dict | None:
    text: str | None
    if isinstance(message, str):
        text = message
    elif isinstance(message, (bytes, bytearray)):
        text = _decode_ws_text(bytes(message))
    else:
        return None
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None
def _decode_ws_text(data: bytes) -> str | None:
    if not data:
        return None
    if len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B:
        try:
            return gzip.decompress(data).decode('utf-8')
        except Exception:
            return None
    try:
        return data.decode('utf-8')
    except Exception:
        return None

def _parse_funding_interval(payload: dict) -> str:
    interval = payload.get("interval") or payload.get("fundingInterval")
    if isinstance(interval, (int, float)) and interval > 0:
        return f"{interval}h"
    if isinstance(interval, str) and interval:
        return interval
    return "8h"


def _iter_levels(source) -> Iterable[Tuple[float, float]]:
    if isinstance(source, dict):
        source = source.get("levels") or source.get("data") or source.get("list") or []
    if not isinstance(source, (list, tuple)):
        return []
    result: List[Tuple[float, float]] = []
    for entry in source:
        price, size = _parse_level(entry)
        if price is None or size is None:
            continue
        result.append((price, size))
    return result


def _parse_level(level) -> Tuple[float | None, float | None]:
    price = size = None
    if isinstance(level, dict):
        for key in ("price", "p", "px", "bp", "ap"):
            val = level.get(key)
            if val is None:
                continue
            try:
                price = float(val)
                break
            except Exception:
                continue
        for key in ("size", "qty", "q", "v", "volume"):
            val = level.get(key)
            if val is None:
                continue
            try:
                size = float(val)
                break
            except Exception:
                continue
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        try:
            price = float(level[0])
        except Exception:
            price = None
        try:
            size = float(level[1])
        except Exception:
            size = None
    if price is None or price <= 0:
        return None, None
    if size is None:
        return price, 0.0
    if size < 0:
        size = 0.0
    return price, size


def _iter_payload_items(payload, default_symbol: str | None) -> Iterable[tuple[str, dict]]:
    if payload is None:
        return []

    items: list[tuple[str, dict]] = []

    if isinstance(payload, dict):
        dict_values = list(payload.values())
        if dict_values and all(isinstance(v, dict) for v in dict_values):
            for key, value in payload.items():
                if not isinstance(value, dict):
                    continue
                symbol = _extract_symbol(value, key, default_symbol)
                if symbol:
                    items.append((symbol, value))
        else:
            symbol = _extract_symbol(payload, None, default_symbol)
            if symbol:
                items.append((symbol, payload))
        return items

    if not isinstance(payload, list):
        return []

    for value in payload:
        if not isinstance(value, dict):
            continue
        symbol = _extract_symbol(value, None, default_symbol)
        if symbol:
            items.append((symbol, value))

    return items


def _extract_symbol(payload: dict, fallback_key: str | None, default_symbol: str | None) -> str | None:
    for key in ("symbol", "instId", "s", "market", "pair"):
        val = payload.get(key)
        if isinstance(val, str) and val:
            return val

    if fallback_key:
        return fallback_key

    return default_symbol


def _extract_topic_symbol(data_type) -> str | None:
    if isinstance(data_type, (list, tuple, set)):
        for item in data_type:
            symbol = _extract_topic_symbol(item)
            if symbol:
                return symbol
        return None
    if isinstance(data_type, dict):
        return _extract_topic_symbol(
            data_type.get("symbol")
            or data_type.get("instId")
            or data_type.get("pair")
        )
    if isinstance(data_type, str) and data_type:
        segments: list[str] = []
        if ":" in data_type:
            segments.extend(part for part in data_type.split(":") if part)
        if not segments:
            segments = [data_type]

        for segment in segments:
            candidate = segment.strip()
            if not candidate:
                continue
            if "/" in candidate:
                candidate = candidate.split("/", maxsplit=1)[-1]
            if candidate.lower().startswith("swap/ticker") and ":" in candidate:
                candidate = candidate.split(":", maxsplit=1)[-1]
            if candidate.lower().startswith("ticker."):
                candidate = candidate.split(".", maxsplit=1)[-1]
            if "." in candidate and "-" in candidate.split(".")[-1]:
                candidate = candidate.split(".")[-1]
            candidate = candidate.replace("_", "-")
            if "-" in candidate:
                return candidate.upper()
        return data_type
    return None


async def authenticate_ws(ws: Any, creds: ApiCreds | None) -> None:
    """Placeholder for future authenticated BingX channels."""
    del ws, creds
    return None
