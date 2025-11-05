from __future__ import annotations

import asyncio, json, gzip, io, time, random
import zlib
import logging
import re
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

from ..domain import Symbol, Ticker
from ..store import TickerStore
from .bingx_utils import normalize_bingx_symbol
from .credentials import ApiCreds
from .discovery import discover_bingx_usdt_perp

logger = logging.getLogger(__name__)

# ИСПРАВЛЕНИЕ 1: Использовать ЕДИНСТВЕННЫЙ правильный WS endpoint для Perpetual Swap
WS_ENDPOINTS = (
    "wss://open-api-swap.bingx.com/swap-market",
)
MAX_TOPICS_PER_CONN = 100
# MAX_TOPICS_PER_SUB_MSG = 10 # <-- УДАЛЕНО: BingX не поддерживает пакеты
WS_SUB_DELAY = 0.05 # <-- УМЕНЬШЕНО: Ускоряем подписку
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
    if not isinstance(msg, dict):
        return False
    # Успешный ответ на подписку
    if "reqType" in msg and msg.get("code") == 0 and "id" in msg:
        return True
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
        logger.debug(
            "BingX WS %s subscriptions: %s (+%d more)",
            kind,
            preview,
            extra,
        )
    else:
        logger.debug("BingX WS %s subscriptions: %s", kind, preview)


def _log_ws_payload_received(symbol: Symbol) -> None:
    global _WS_PAYLOAD_LOG_COUNT
    if _WS_PAYLOAD_LOG_COUNT >= _WS_PAYLOAD_LOG_LIMIT:
        return
    _WS_PAYLOAD_LOG_COUNT += 1
    logger.debug("BingX WS payload received for %s", symbol)


def _log_first_ticker(symbol: Symbol, bid: float, ask: float) -> None:
    target = symbol.upper()
    if target not in {"BTCUSDT", "ETHUSDT"}:
        return
    if target in _LOG_ONCE_TICKERS:
        return
    _LOG_ONCE_TICKERS.add(target)
    logger.debug("BingX first ticker for %s: bid=%s ask=%s", target, bid, ask)


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
        # Дополнительная проверка на ASCII
        if not re.fullmatch(r'[A-Z0-9]+', normalized):
            return ""
        return normalized
    sym = str(symbol).upper()
    sym = sym.replace("-", "").replace("_", "")
    # Дополнительная проверка на ASCII
    if not re.fullmatch(r'[A-Z0-9]+', sym):
        return ""
    return sym


def _collect_wanted_common(symbols: Sequence[Symbol]) -> set[str]:
    wanted: set[str] = set()
    for symbol in symbols:
        if not symbol:
            continue
        normalized = normalize_bingx_symbol(symbol)
        if normalized:
            if not re.fullmatch(r'[A-Z0-9]+', normalized):
                continue
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
    sym = str(symbol).upper().replace("-", "_")
    if "_" in sym:
        return sym
    if sym.endswith("USDT"):
        return f"{sym[:-4]}_USDT"
    if sym.endswith("USDC"):
        return f"{sym[:-4]}_USDC"
    if sym.endswith("USD"):
        return f"{sym[:-3]}_USD"
    return sym


def _to_bingx_ws_symbol(symbol: Symbol) -> str | None:
    # ИСПРАВЛЕНИЕ: приводим к формату BTC-USDT
    sym = _normalize_common_symbol(symbol)

    # ПРОВЕРКА: Если символ "мусорный" (как 币安人生USDT), _normalize_common_symbol вернет ""
    if not sym:
        logger.warning("BingX WS skipping invalid non-ASCII symbol: %s", symbol)
        return None # <--- НЕ подписываемся

    for quote in ("USDT", "USDC", "USD", "BUSD", "FDUSD"):
        if sym.endswith(quote):
            base = sym[: -len(quote)]
            if not base: # Защита от символа вида "USDT"
                return None
            return f"{base}-{quote}"
    
    logger.warning("BingX WS could not format symbol to WS standard: %s", symbol)
    return None # <--- Не подписываемся, если не смогли распознать


def _from_bingx_symbol(symbol: str | None) -> str | None:
    """
    Мягкая нормализация входящих имён инструментов из BingX.
    Сначала пробуем штатный normalize_bingx_symbol, если он вернул None — делаем безопасный фолбэк.
    """
    s = normalize_bingx_symbol(symbol)
    if s:
        return s
    if not symbol:
        return None
    return str(symbol).replace("-", "").replace("_", "").upper()



async def run_bingx(store: TickerStore, symbols: Sequence[Symbol]) -> None:
    # ИСПРАВЛЕНИЕ: Убираем логику 'subscribe_all', она была некорректной
    subscribe, _ = await _resolve_bingx_symbols(symbols)

    if not subscribe:
        logger.warning("No symbols resolved for BingX connector; skipping startup")
        return

    chunks = [
        tuple(chunk) for chunk in _chunk_bingx_symbols(subscribe, MAX_TOPICS_PER_CONN)
    ]

    while True:
        tasks: list[asyncio.Task] = []
        clients: list[_BingxWsClient] = []

        try:
            # Блок 'if subscribe_all:' УДАЛЕН
            for chunk in chunks:
                wanted_common = _collect_wanted_common(chunk)
                if not wanted_common:
                    continue

                symbol_pairs: list[tuple[str, str]] = []
                for sym in sorted(wanted_common):
                    native = _to_bingx_ws_symbol(sym)
                    
                    # ИСПРАВЛЕНИЕ: Проверяем, что _to_bingx_ws_symbol вернул валидный символ
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
                await asyncio.sleep(WS_SUB_DELAY) # Добавляем задержку между подписками
            except Exception as exc:
                self.log.error('Resubscribe failed for %s: %s', key, exc)
                raise # Если подписка не удалась, нужно переподключиться

    async def _ws_reader(self) -> None:
        assert self._ws is not None
        try:
            while self._running:
                raw = await self._ws.recv()
                self._log_raw_frame(raw)
                txt = self._maybe_gunzip(raw)
                self._last_rx_ts = time.time()

                # ИСПРАВЛЕНИЕ 3: Обработка Ping/Pong
                if txt == 'Ping':
                    await self._ws.send('Pong')
                    self.log.debug("BingX WS Pong sent")
                    continue

                msg = None
                if txt:
                    try:
                        msg = json.loads(txt)
                    except Exception:
                        if txt != 'Pong': # Не логируем наши собственные ответы
                            self.log.debug('BingX WS TXT: %s', txt[:200])

                sample_source = msg if msg is not None else txt
                if sample_source is not None and int(time.time()) % 10 == 0:
                    self.log.debug('WS RX sample: %s', str(sample_source)[:300])

                if isinstance(msg, dict):
                    # Старая (документация v1) обработка ping/pong, оставляем на всякий случай
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
        # Убираем отправку ping'ов от клиента, т.к. сервер
        # сам присылает "Ping", а мы на него отвечаем "Pong"
        try:
            while self._running:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                if not self._ws or self._ws.closed:
                    raise ConnectionError("BingX websocket closed")
                now = time.time()
                if (now - self._last_rx_ts) > (HEARTBEAT_INTERVAL * 2.5):
                    self.log.warning("BingX WS timeout: no message received in %s sec", int(now - self._last_rx_ts))
                    raise ConnectionError("BingX WS timeout")
        except asyncio.CancelledError:
            raise
        except Exception:
            raise

    def _prepare_subscriptions(self) -> None:
        # ИСПРАВЛЕНИЕ 2: Используем новый формат (БЕЗ ПАКЕТОВ)
        
        # 1. Tickers
        if self._ticker_pairs:
            seen: set[str] = set()
            for common, native in self._ticker_pairs:
                if not native: continue
                if str(native).upper() == 'ALL': continue
                normalized = native.replace('_', '-') # Документация использует BTC-USDT
                if normalized in seen: continue
                seen.add(normalized)
                
                topic = f"{normalized}@ticker"
                payload = {
                    "id": f"sub_ticker_{normalized}_{int(time.time())}",
                    "reqType": "sub",
                    "dataType": topic
                }
                self.log.info('BingX adding ticker sub -> %s (native=%s)', common, topic)
                self._remember_sub(f'ticker:{normalized}', payload)

        # 2. Depth (Стаканы)
        if self._depth_symbols:
            seen_depth: set[str] = set()
            for sym in self._depth_symbols:
                if not sym or sym.upper() == 'ALL': continue
                topic_symbol = sym.replace('_', '-')
                if topic_symbol in seen_depth: continue
                seen_depth.add(topic_symbol)

                topic = f"{topic_symbol}@depth5"
                payload = {
                    "id": f"sub_depth_{topic_symbol}_{int(time.time())}",
                    "reqType": "sub",
                    "dataType": topic
                }
                self.log.info('BingX adding depth sub -> %s', topic)
                self._remember_sub(f'depth:{topic_symbol}', payload)

        # 3. Funding (Фандинг)
        if self._funding_symbols:
            seen_funding: set[str] = set()
            for sym in self._funding_symbols:
                if not sym or sym.upper() == 'ALL': continue
                topic_symbol = sym.replace('_', '-')
                if topic_symbol in seen_funding: continue
                seen_funding.add(topic_symbol)
                
                topic = f"{topic_symbol}@fundingRate"
                payload = {
                    "id": f"sub_funding_{topic_symbol}_{int(time.time())}",
                    "reqType": "sub",
                    "dataType": topic
                }
                self.log.info('BingX adding funding sub -> %s', topic)
                self._remember_sub(f'funding:{topic_symbol}', payload)


    def _remember_sub(self, key: str, payload: Dict[str, Any]) -> None:
        self._active_subs[key] = payload

    def _log_raw_frame(self, raw: Any) -> None:
        if not self.log.isEnabledFor(logging.DEBUG):
            return

        if isinstance(raw, (bytes, bytearray)):
            self.log.debug('WS RX raw frame (%d bytes)', len(raw))
            return

        self.log.debug('WS RX raw frame: %s', str(raw)[:512])

    def _maybe_gunzip(self, payload: Any) -> str:
        if isinstance(payload, (bytes, bytearray)):
            b = bytes(payload)
            if len(b) < 2:
                return ""
    
            # Попытка GZIP
            if b[0] == 0x1F and b[1] == 0x8B:
                try:
                    with gzip.GzipFile(fileobj=io.BytesIO(b)) as gz:
                        return gz.read().decode('utf-8', 'replace')
                except Exception:
                    pass # Ошибка GZIP, попробуем zlib или текст
    
            # Попытка ZLIB (для BingX)
            try:
                # 15 - базовое окно, 32 - автоопределение (zlib ИЛИ gzip)
                return zlib.decompress(b, 15 + 32).decode('utf-8', 'replace')
            except Exception:
                pass # Ошибка ZLIB, попробуем текст
    
            # Попытка как обычный текст
            try:
                return b.decode('utf-8', 'replace')
            except Exception:
                return '' # Не удалось расшифровать
    
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
                # self.log.debug('BingX WS TXT: %s', message[:200])
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
            
            # В v2 'dataType' содержит и тип, и символ (напр., "btc-usdt@ticker")
            # 'data' содержит сам payload
            is_funding = 'fundingrate' in data_type
            is_depth = 'depth' in data_type
            
            if is_funding:
                rate = _extract_price(
                    payload,
                    (
                        'fundingRate', # 'f' в доках, но оставим старые ключи
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

            if is_depth or (
                isinstance(payload, dict)
                and (payload.get('bids') or payload.get('asks') or payload.get('b') or payload.get('a'))
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

            # ИСПРАВЛЕНИЕ 4: Добавляем 'b' (bid) и 'a' (ask) из документации
            bid = _extract_price(
                payload,
                (
                    'b', # <--- ИЗ ДОКУМЕНТАЦИИ V2 (Best Bid)
                    'bestBid',
                    'bestBidPrice',
                    'bid',
                    'bidPrice',
                    'bid1',
                    'bid1Price',
                    'bp',
                    'bidPx',
                    'bestBidPx',
                    'buyPrice',
                ),
            )
            ask = _extract_price(
                payload,
                (
                    'a', # <--- ИЗ ДОКУМЕНТАЦИИ V2 (Best Ask)
                    'bestAsk',
                    'bestAskPrice',
                    'ask',
                    'askPrice',
                    'ask1',
                    'ask1Price',
                    'ap',
                    'askPx',
                    'bestAskPx',
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
            
            # ИСПРАВЛЕНИЕ 4: 'c' (Close/LastPrice)
            last_price = _extract_price(
                payload,
                (
                    'c', # <--- ИЗ ДОКУМЕНТАЦИИ V2 (Last Price)
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
    
    # В v2 'data' это и есть payload, 'dataType' содержит символ
    if payload is None:
        payload = message.get("data")

    # Если 'data' нет, но есть 'dataType', то 'message' - это сам payload
    if payload is None and "dataType" in message:
        payload = message


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
    
    # 'payload' может быть списком в некоторых ответах, но в V2 (ticker/depth) это обычно dict
    for raw_symbol, payload_item in _iter_payload_items(payload, default_symbol):
        if not isinstance(payload_item, dict):
            logger.debug(
                "BingX WS drop %r: payload is not a dict (got %s)",
                raw_symbol,
                type(payload_item).__name__,
            )
            continue
        
        # В v2 'data' это и есть payload, а 'dataType' (уже в default_symbol) несёт символ
        if not raw_symbol:
            raw_symbol = _extract_symbol(payload_item, None, default_symbol)

        if not raw_symbol:
            logger.debug(
                "BingX WS drop payload without symbol: keys=%s",
                list(payload_item.keys())[:5],
            )
            continue

        common_symbol = _from_bingx_symbol(raw_symbol)
        logger.debug(
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
        
        # ИСПРАВЛЕНИЕ 4: Добавляем 'b' (bids) и 'a' (asks) из v2
        bids_candidate = _collect_depth_levels(
            container,
            (
                "b", # <--- ИЗ ДОКУМЕНТАЦИИ V2 (Bids)
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
                "a", # <--- ИЗ ДОКУМЕНТАЦИИ V2 (Asks)
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
        "c", # <--- ИЗ ДОКУМЕНТАЦИИ V2
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
        # В v2 стаканы приходят как [цена, кол-во]
        pass
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
    if size is None or size < 0:
        size = 0.0
    return price, size


def _iter_payload_items(payload, default_symbol: str | None) -> Iterable[tuple[str, dict]]:
    if payload is None:
        return []

    items: list[tuple[str, dict]] = []

    if isinstance(payload, dict):
        dict_values = list(payload.values())
        # Убираем эту эвристику, т.к. в v2 'data' - это один объект
        # if dict_values and all(isinstance(v, dict) for v in dict_values):
        #     for key, value in payload.items():
        #         if not isinstance(value, dict):
        #             continue
        #         symbol = _extract_symbol(value, key, default_symbol)
        #         if symbol:
        #             items.append((symbol, value))
        # else:
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
    # ИСПРАВЛЕНИЕ 4: 's' - ключ символа в v2
    for key in ("s", "symbol", "instId", "market", "pair"):
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
        candidate = data_type.strip()
        
        # ИСПРАВЛЕНИЕ 2: Новый формат 'BTC-USDT@ticker'
        if "@" in candidate:
            candidate = candidate.split("@", maxsplit=1)[0]
            
        if "/" in candidate:
            candidate = candidate.split("/", maxsplit=1)[-1]
        if candidate.lower().startswith("swap/ticker") and ":" in candidate:
            candidate = candidate.split(":", maxsplit=1)[-1]
        if candidate.lower().startswith("ticker."):
            candidate = candidate.split(".", maxsplit=1)[-1]
        if "." in candidate and "-" in candidate.split(".")[-1]:
            candidate = candidate.split(".")[-1]
        
        candidate = candidate.replace("_", "-")
        if "-" in candidate: # Формат BingX (BTC-USDT)
            return candidate.upper()
        return data_type
    return None


async def authenticate_ws(ws: Any, creds: ApiCreds | None) -> None:
    """Placeholder for future authenticated BingX channels."""
    del ws, creds
    return None
