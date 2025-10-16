from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Mapping, Sequence
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
    notional: float | None = None
    liquidity_warning: bool = False
    entry_price_long_avg: float | None = None
    entry_price_short_avg: float | None = None
    exit_price_long_avg: float | None = None
    exit_price_short_avg: float | None = None

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
            "notional": self.notional,
            "liquidity_warning": self.liquidity_warning,
            "entry_price_long_avg": self.entry_price_long_avg or self.price_long_ask,
            "entry_price_short_avg": self.entry_price_short_avg or self.price_short_bid,
            "exit_price_long_avg": self.exit_price_long_avg or self.price_long_bid,
            "exit_price_short_avg": self.exit_price_short_avg or self.price_short_ask,
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


def _avg_price_for_notional(
    orderbook: OrderBookData | None,
    notional: float,
    *,
    side: str,
) -> float | None:
    if orderbook is None or notional <= 0:
        return None
    levels = orderbook.asks if side == "buy" else orderbook.bids
    if not levels:
        return None
    if side == "buy":
        iterable = sorted(levels, key=lambda level: level[0])
    else:
        iterable = sorted(levels, key=lambda level: level[0], reverse=True)
    remaining = float(notional)
    total_qty = 0.0
    total_notional = 0.0
    for price, size in iterable:
        if price <= 0 or size <= 0:
            continue
        level_notional = price * size
        if level_notional >= remaining:
            qty = remaining / price
            total_qty += qty
            total_notional += remaining
            remaining = 0.0
            break
        total_qty += size
        total_notional += level_notional
        remaining -= level_notional
        if remaining <= 0:
            break
    if remaining > 1e-9:
        return None
    if total_qty <= 0:
        return None
    return total_notional / total_qty


def compute_rows(
    store: TickerStore,
    symbols: Iterable[Symbol],
    exchanges: Iterable[ExchangeName],
    taker_fees: Mapping[ExchangeName, float] = DEFAULT_TAKER_FEES,
    target_notional: float | None = None,
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

                fl = long_payload.get("funding")
                fs = short_payload.get("funding")

                entry_long_price = long_t.ask
                entry_short_price = short_t.bid
                exit_long_price = long_t.bid
                exit_short_price = short_t.ask
                entry_long_avg = None
                entry_short_avg = None
                exit_long_avg = None
                exit_short_avg = None
                liquidity_warning = False

                if target_notional and target_notional > 0:
                    entry_long_avg = _avg_price_for_notional(long_ob, target_notional, side="buy")
                    entry_short_avg = _avg_price_for_notional(short_ob, target_notional, side="sell")
                    exit_long_avg = _avg_price_for_notional(long_ob, target_notional, side="sell")
                    exit_short_avg = _avg_price_for_notional(short_ob, target_notional, side="buy")

                    if entry_long_avg is not None and entry_short_avg is not None:
                        entry_long_price = entry_long_avg
                        entry_short_price = entry_short_avg
                    else:
                        liquidity_warning = True

                    if exit_long_avg is not None and exit_short_avg is not None:
                        exit_long_price = exit_long_avg
                        exit_short_price = exit_short_avg
                    else:
                        liquidity_warning = True

                entry = _entry(entry_short_price, entry_long_price)
                exitv = _exit(exit_long_price, exit_short_price)

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
                        price_long_ask=entry_long_price,
                        price_short_bid=entry_short_price,
                        price_long_bid=exit_long_price,
                        price_short_ask=exit_short_price,
                        orderbook_long=long_ob,
                        orderbook_short=short_ob,
                        notional=target_notional,
                        liquidity_warning=liquidity_warning,
                        entry_price_long_avg=entry_long_avg,
                        entry_price_short_avg=entry_short_avg,
                        exit_price_long_avg=exit_long_avg,
                        exit_price_short_avg=exit_short_avg,
                    )
                )

    rows.sort(key=lambda r: r.entry_pct, reverse=True)
    return rows
