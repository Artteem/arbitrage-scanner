from __future__ import annotations
from dataclasses import dataclass
from typing import List, Iterable, Mapping, Sequence
from ..domain import ExchangeName, Symbol
from ..store import OrderBookData, TickerStore

# ТЕЙКЕР-комиссии по биржам (в долях, не в процентах).
# Берём по умолчанию консервативные значения; при необходимости можно вынести в .env.
DEFAULT_TAKER_FEES: dict[ExchangeName, float] = {
    "binance": 0.0005,  # 0.05%
    "bybit":   0.0006,  # 0.06%
    "mexc":   0.0006,  # 0.06%
    "bingx":  0.0005,  # 0.05%
}

MAX_TICKER_SKEW_SECONDS = 0.5

@dataclass
class Row:
    symbol: Symbol
    long_ex: ExchangeName
    short_ex: ExchangeName
    entry_pct: float
    exit_pct: float
    funding_long: float
    funding_short: float
    funding_interval_long: str
    funding_interval_short: str
    funding_spread: float  # short - long
    commission_pct_total: float  # сумма комиссий за ВЕСЬ цикл (4 рыночные сделки)
    price_long_ask: float
    price_short_bid: float
    price_long_bid: float
    price_short_ask: float
    orderbook_long: OrderBookData | None = None
    orderbook_short: OrderBookData | None = None
    skew_seconds: float = 0.0
    skewed: bool = False
    latency_long: float | None = None
    latency_short: float | None = None

    def as_dict(self) -> dict:
        # ВАЖНО: не ломаем фронт. Отдаём и ключ "commission" (как использовалось в UI),
        # и "commission_total_pct" для совместимости.
        return {
            "symbol": self.symbol,
            "long_exchange": self.long_ex,
            "short_exchange": self.short_ex,
            "entry_pct": round(self.entry_pct, 4),
            "exit_pct": round(self.exit_pct, 4),
            "funding_long": self.funding_long,
            "funding_short": self.funding_short,
            "funding_interval_long": self.funding_interval_long,
            "funding_interval_short": self.funding_interval_short,
            "funding_spread": round(self.funding_spread, 6),

            # Комиссия «полного круга» — 4 рыночные сделки: 
            #   вход:   long_ex BUY (taker) + short_ex SELL (taker)
            #   выход:  long_ex SELL (taker) + short_ex BUY  (taker)
            # Итого: 2 * (fee_long + fee_short), в процентах.
            "commission": round(self.commission_pct_total, 4),
            "commission_total_pct": round(self.commission_pct_total, 4),

            "price_long_ask": self.price_long_ask,
            "price_short_bid": self.price_short_bid,
            "price_long_bid": self.price_long_bid,
            "price_short_ask": self.price_short_ask,
            "orderbook_long": self.orderbook_long.to_dict() if self.orderbook_long else None,
            "orderbook_short": self.orderbook_short.to_dict() if self.orderbook_short else None,
            "skew_seconds": round(self.skew_seconds, 6),
            "skewed": self.skewed,
            "latency_long": self.latency_long,
            "latency_short": self.latency_short,
        }

def _entry(bid_short: float, ask_long: float) -> float:
    # (bid(B) - ask(A)) / mid * 100
    return (bid_short - ask_long) / ((bid_short + ask_long) / 2.0) * 100.0

def _exit(bid_long: float, ask_short: float) -> float:
    # (bid(A) - ask(B)) / mid * 100
    return (bid_long - ask_short) / ((bid_long + ask_short) / 2.0) * 100.0

def _commission_total_pct(
    long_ex: ExchangeName,
    short_ex: ExchangeName,
    fees: Mapping[ExchangeName, float],
) -> float:
    fl = float(fees.get(long_ex, 0.001))   # дефолт на случай отсутствия в словаре
    fs = float(fees.get(short_ex, 0.001))
    return (2.0 * (fl + fs)) * 100.0

def compute_rows(
    store: TickerStore,
    symbols: Iterable[Symbol],
    exchanges: Iterable[ExchangeName],
    taker_fees: Mapping[ExchangeName, float] = DEFAULT_TAKER_FEES,
) -> List[Row]:
    rows: List[Row] = []

    ordered_symbols: List[Symbol] = []
    seen_symbols: set[Symbol] = set()

    for sym in symbols:
        sym_str = Symbol(str(sym))
        if sym_str in seen_symbols:
            continue
        ordered_symbols.append(sym_str)
        seen_symbols.add(sym_str)

    for sym in store.symbols():
        if sym in seen_symbols:
            continue
        ordered_symbols.append(sym)
        seen_symbols.add(sym)

    ordered_exchanges: List[ExchangeName] = list(exchanges)
    seen_exchanges: set[ExchangeName] = set(ordered_exchanges)
    for ex in store.exchanges():
        if ex in seen_exchanges:
            continue
        ordered_exchanges.append(ex)
        seen_exchanges.add(ex)

    exchange_index: dict[ExchangeName, int] = {
        ex: idx for idx, ex in enumerate(ordered_exchanges)
    }

    def _exchange_sort_key(ex: ExchangeName) -> tuple[int, ExchangeName]:
        return (exchange_index.get(ex, len(exchange_index)), ex)

    for sym in ordered_symbols:
        data_by_symbol = store.by_symbol(sym)
        if not data_by_symbol:
            continue

        present: Sequence[ExchangeName] = sorted(data_by_symbol.keys(), key=_exchange_sort_key)
        if len(present) < 2:
            continue

        for long_ex in present:
            long_payload = data_by_symbol.get(long_ex)
            if not long_payload:
                continue
            long_t = long_payload.get("ticker")
            if not long_t:
                continue
            long_ob = long_payload.get("order_book")

            for short_ex in present:
                if long_ex == short_ex:
                    continue

                short_payload = data_by_symbol.get(short_ex)
                if not short_payload:
                    continue
                short_t = short_payload.get("ticker")
                if not short_t:
                    continue
                short_ob = short_payload.get("order_book")

                skew_seconds = abs(long_t.ts - short_t.ts)
                skewed = skew_seconds > MAX_TICKER_SKEW_SECONDS

                long_latency = long_t.latency
                short_latency = short_t.latency

                fl = long_payload.get("funding")
                fs = short_payload.get("funding")

                entry = _entry(short_t.bid, long_t.ask)
                exitv = _exit(long_t.bid, short_t.ask)

                comm_total = _commission_total_pct(long_ex, short_ex, taker_fees)

                rows.append(
                    Row(
                        symbol=sym,
                        long_ex=long_ex,
                        short_ex=short_ex,
                        entry_pct=entry,
                        exit_pct=exitv,
                        funding_long=(fl.rate if fl else 0.0),
                        funding_short=(fs.rate if fs else 0.0),
                        funding_interval_long=(fl.interval if fl else ""),
                        funding_interval_short=(fs.interval if fs else ""),
                        funding_spread=((fs.rate if fs else 0.0) - (fl.rate if fl else 0.0)),
                        commission_pct_total=comm_total,
                        price_long_ask=long_t.ask,
                        price_short_bid=short_t.bid,
                        price_long_bid=long_t.bid,
                        price_short_ask=short_t.ask,
                        orderbook_long=long_ob,
                        orderbook_short=short_ob,
                        skew_seconds=skew_seconds,
                        skewed=skewed,
                        latency_long=long_latency,
                        latency_short=short_latency,
                    )
                )

    rows.sort(key=lambda r: r.entry_pct, reverse=True)
    return rows
