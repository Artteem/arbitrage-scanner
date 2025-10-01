from __future__ import annotations
from dataclasses import dataclass
from typing import List, Iterable, Mapping
from ..domain import ExchangeName, Symbol
from ..store import TickerStore

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
    exs = list(exchanges)

    for sym in symbols:
        # какие биржи реально присутствуют в сторе по этой паре
        present = [ex for ex in exs if store.get_ticker(ex, sym)]
        for long_ex in present:
            for short_ex in present:
                if long_ex == short_ex:
                    continue

                long_t = store.get_ticker(long_ex, sym)
                short_t = store.get_ticker(short_ex, sym)
                if not long_t or not short_t:
                    continue

                fl = store.get_funding(long_ex, sym)
                fs = store.get_funding(short_ex, sym)

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
                    )
                )

    rows.sort(key=lambda r: r.entry_pct, reverse=True)
    return rows
