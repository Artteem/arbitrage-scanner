export interface ApiStats {
  symbols_subscribed: string[];
  tickers_in_store: number;
  exchanges: string[];
}

export interface SpreadRow {
  symbol: string;
  long_exchange: string;
  short_exchange: string;
  entry_pct: number;
  exit_pct: number;
  funding_long: number;
  funding_short: number;
  funding_interval_long: string;
  funding_interval_short: string;
  funding_spread: number;
  commission_total_pct: number;
  commission: number;
  price_long_ask: number;
  price_short_bid: number;
  price_long_bid: number;
  price_short_ask: number;
  orderbook_long?: unknown;
  orderbook_short?: unknown;
  notional?: number | null;
  liquidity_warning?: boolean;
  entry_price_long_avg?: number;
  entry_price_short_avg?: number;
  exit_price_long_avg?: number;
  exit_price_short_avg?: number;
  _ts?: number;
}

export interface PairOverviewResponse {
  symbol: string;
  rows: SpreadRow[];
}

export interface SpreadCandle {
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface PairSpreadsResponse {
  symbol: string;
  long: string;
  short: string;
  metric: 'entry' | 'exit';
  timeframe: string;
  timeframe_seconds: number;
  candles: SpreadCandle[];
}

export interface PairLimitsResponse {
  symbol: string;
  long_exchange: string;
  short_exchange: string;
  long: Record<string, unknown> | null;
  short: Record<string, unknown> | null;
}

export interface PairRealtimeResponse {
  symbol: string;
  long_exchange: string;
  short_exchange: string;
  ts: number;
  row: SpreadRow | null;
}

export interface PairSelection {
  symbol: string;
  long_exchange: string;
  short_exchange: string;
}
