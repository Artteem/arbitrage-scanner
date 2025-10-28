'use client';

import { useEffect, useMemo, useRef, useState, type MutableRefObject } from 'react';
import { useRouter } from 'next/navigation';
import {
  usePairLimits,
  usePairOverview,
  usePairRealtime,
  usePairSpreads,
  useStats,
} from '../../lib/api';
import { formatPairLabel } from '../../lib/format';
import type { PairSelection, SpreadCandle, SpreadRow } from '../../lib/types';
import {
  LineStyle,
  createChart,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type Time,
  type UTCTimestamp,
  type BusinessDay
} from 'lightweight-charts';
import { useTheme } from './useTheme';

function formatTimeWithTimezone(
  time: unknown,
  timeframe: '1m' | '5m' | '1h',
  tz: string
): string {
  const date = toDateFromAnyTime(time);
  if (!date) return '';

  const opts: Intl.DateTimeFormatOptions =
    timeframe === '1m'
      ? { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: tz }
      : timeframe === '5m'
      ? { hour: '2-digit', minute: '2-digit', timeZone: tz }
      : { year: '2-digit', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', timeZone: tz };

  return new Intl.DateTimeFormat('ru-RU', opts).format(date);
}

// --- Time helpers ------------------------------------------------------------

function isUtcTimestamp(t: unknown): t is number {
  return typeof t === 'number' && Number.isFinite(t);
}

function isDateString(t: unknown): t is string {
  return typeof t === 'string';
}

function isBusinessDay(t: unknown): t is BusinessDay {
  return typeof t === 'object' && t !== null
    && 'year' in (t as any)
    && 'month' in (t as any)
    && 'day' in (t as any);
}

// Иногда из formatters прилетает объект-обёртка { timestamp } или { businessDay }
function toDateFromAnyTime(t: unknown): Date | null {
  if (isUtcTimestamp(t)) return new Date(t * 1000);
  if (isDateString(t)) {
    const d = new Date(t);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  if (isBusinessDay(t)) {
    const { year, month, day } = t;
    return new Date(year, month - 1, day);
  }
  if (typeof t === 'object' && t !== null) {
    const anyT = t as any;
    if (typeof anyT.timestamp === 'number') {
      return new Date(anyT.timestamp * 1000);
    }
    if (anyT.businessDay && isBusinessDay(anyT.businessDay)) {
      const { year, month, day } = anyT.businessDay as BusinessDay;
      return new Date(year, month - 1, day);
    }
  }
  return null;
}

const LOOKBACK_DAYS = 10;
const TIMEFRAMES = ['1m', '5m', '1h'] as const;
const TIMEFRAME_SECONDS_MAP: Record<(typeof TIMEFRAMES)[number], number> = {
  '1m': 60,
  '5m': 300,
  '1h': 3600,
};
const LOOKBACK_SECONDS = LOOKBACK_DAYS * 24 * 60 * 60;
const DEFAULT_VISIBLE_PERIODS = 60;

const toPositiveSeconds = (value: unknown): number | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const numeric = typeof value === 'string' ? Number(value) : value;
  if (typeof numeric !== 'number') {
    return null;
  }
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }
  return numeric;
};

const extractOrderBookTimestamp = (value: unknown): number | null => {
  if (typeof value !== 'object' || value === null) {
    return null;
  }
  const record = value as Record<string, unknown>;
  const lastPriceTs = toPositiveSeconds(record['last_price_ts']);
  const snapshotTs = toPositiveSeconds(record['ts']);
  if (lastPriceTs !== null && snapshotTs !== null) {
    return Math.max(lastPriceTs, snapshotTs);
  }
  return lastPriceTs ?? snapshotTs ?? null;
};

type LatencyTimestampSummary = {
  newest: number | null;
  oldest: number | null;
};

const resolveLatencyTimestampSummary = (
  row: SpreadRow | null | undefined,
  fallback: number | null | undefined,
): LatencyTimestampSummary => {
  const candidates: number[] = [];
  if (row) {
    const rowTs = toPositiveSeconds(row._ts);
    if (rowTs !== null) {
      candidates.push(rowTs);
    }
    const longTs = extractOrderBookTimestamp(row.orderbook_long);
    if (longTs !== null) {
      candidates.push(longTs);
    }
    const shortTs = extractOrderBookTimestamp(row.orderbook_short);
    if (shortTs !== null) {
      candidates.push(shortTs);
    }
  }
  const fallbackTs = toPositiveSeconds(fallback ?? null);
  if (fallbackTs !== null) {
    candidates.push(fallbackTs);
  }
  if (!candidates.length) {
    return { newest: null, oldest: null };
  }
  return {
    newest: Math.max(...candidates),
    oldest: Math.min(...candidates),
  };
};

const getTimeframeSeconds = (
  explicit: number | null | undefined,
  fallbackKey: (typeof TIMEFRAMES)[number],
) => {
  if (explicit && explicit > 0) {
    return explicit;
  }
  return TIMEFRAME_SECONDS_MAP[fallbackKey] ?? 300;
};

type CandleDataPoint = {
  time: UTCTimestamp;
  open: number;
  high: number;
  low: number;
  close: number;
};

const roundCandleValue = (value: number) => Number(value.toFixed(6));

const toCandleDataPoint = (candle: SpreadCandle): CandleDataPoint => ({
  time: Math.round(candle.ts) as UTCTimestamp,
  open: roundCandleValue(candle.open),
  high: roundCandleValue(candle.high),
  low: roundCandleValue(candle.low),
  close: roundCandleValue(candle.close),
});

const pruneBuffer = (buffer: CandleDataPoint[]) => {
  if (!buffer.length) {
    return 0;
  }
  const cutoff = Math.floor(Date.now() / 1000) - LOOKBACK_SECONDS;
  let removed = 0;
  while (buffer.length && buffer[0].time < cutoff) {
    buffer.shift();
    removed += 1;
  }
  return removed;
};

const applyDefaultVisibleRange = (
  chart: IChartApi | null,
  buffer: CandleDataPoint[],
  timeframeSeconds: number,
) => {
  if (!chart || !buffer.length) {
    return;
  }
  if (!Number.isFinite(timeframeSeconds) || timeframeSeconds <= 0) {
    const scale = chart.timeScale();
    if (typeof scale.fitContent === 'function') {
      scale.fitContent();
    }
    return;
  }

  const scale = chart.timeScale();
  const lastIndex = buffer.length - 1;
  const lastPoint = buffer[lastIndex];
  if (!lastPoint) {
    return;
  }
  const lastTime = lastPoint.time;

  const fromIndex = Math.max(0, lastIndex - (DEFAULT_VISIBLE_PERIODS - 1));
  const rangeFromExisting = buffer[fromIndex]?.time ?? lastTime;
  const paddedFrom = (lastTime - timeframeSeconds * (DEFAULT_VISIBLE_PERIODS - 1)) as UTCTimestamp;
  const fromTime = (buffer.length >= DEFAULT_VISIBLE_PERIODS
    ? rangeFromExisting
    : (Math.max(0, paddedFrom) as UTCTimestamp));

  if (typeof scale.setVisibleRange === 'function') {
    scale.setVisibleRange({ from: fromTime, to: lastTime });
  } else if (typeof scale.fitContent === 'function') {
    scale.fitContent();
  }
};

const appendLiveCandle = (
  bufferRef: MutableRefObject<CandleDataPoint[]>,
  rawValue: number,
  timestamp: number,
  timeframeSeconds: number,
  series: ISeriesApi<'Candlestick'> | null,
  chart: IChartApi | null,
) => {
  if (!Number.isFinite(rawValue) || !Number.isFinite(timestamp)) {
    return;
  }
  if (timeframeSeconds <= 0) {
    return;
  }
  const value = roundCandleValue(rawValue);
  const bucket = (Math.floor(timestamp / timeframeSeconds) * timeframeSeconds) as UTCTimestamp;
  const buffer = bufferRef.current;
  const lastIndex = buffer.length - 1;
  const lastExisting = lastIndex >= 0 ? buffer[lastIndex] : null;
  let updated: CandleDataPoint;
  if (!lastExisting || lastExisting.time !== bucket) {
    updated = {
      time: bucket,
      open: value,
      high: value,
      low: value,
      close: value,
    };
    buffer.push(updated);
  } else {
    updated = {
      time: bucket,
      open: lastExisting.open,
      high: Math.max(lastExisting.high, value),
      low: Math.min(lastExisting.low, value),
      close: value,
    };
    buffer[lastIndex] = updated;
  }
  const dropped = pruneBuffer(buffer);
  if (series) {
    if (!buffer.length) {
      series.setData([]);
    } else if (dropped > 0) {
      series.setData(buffer);
    } else {
      series.update(updated);
    }
  }
  if (chart) {
    const scale = chart.timeScale();
    applyDefaultVisibleRange(chart, buffer, timeframeSeconds);
    if (typeof scale.scrollToRealTime === 'function') {
      scale.scrollToRealTime();
    }
  }
};

const formatPercent = (value: number | null | undefined, digits = 2) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return `${value.toFixed(digits)}%`;
};

const formatFundingPercent = (value: number | null | undefined, digits = 2) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return `${(value * 100).toFixed(digits)}%`;
};

const formatUsd = (value: number | null | undefined, digits = 2) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return value.toLocaleString('ru-RU', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
};

const normalizeExchange = (value: string | null | undefined) =>
  String(value ?? '').trim().toLowerCase();

const rowMatchesSelection = (row: SpreadRow | null | undefined, selection: PairSelection | null) => {
  if (!row || !selection) {
    return false;
  }
  return (
    normalizeExchange(row.long_exchange) === normalizeExchange(selection.long_exchange) &&
    normalizeExchange(row.short_exchange) === normalizeExchange(selection.short_exchange)
  );
};

const exchangeUrl = (exchange: string, symbol: string) => {
  switch (exchange) {
    case 'binance':
      return `https://www.binance.com/en/futures/${symbol}`;
    case 'bybit':
      return `https://www.bybit.com/trade/usdt/${symbol}`;
    case 'mexc':
      return `https://futures.mexc.com/exchange/${toMexcSymbol(symbol)}`;
    case 'bingx':
      return `https://bingx.com/en-us/futures/${toBingxSymbol(symbol)}`;
    case 'gate':
      return `https://www.gate.io/futures_trade/${toGateSymbol(symbol)}`;
    default:
      return '#';
  }
};

const toMexcSymbol = (symbol: string) => {
  if (!symbol) return symbol;
  if (symbol.includes('_')) return symbol;
  return symbol.endsWith('USDT') ? `${symbol.slice(0, -4)}_USDT` : symbol;
};

const toBingxSymbol = (symbol: string) => {
  if (!symbol) return symbol;
  const upper = symbol.toUpperCase();
  const quotes = ['USDT', 'USDC', 'USD', 'BUSD', 'FDUSD'];
  const quote = quotes.find((q) => upper.endsWith(q));
  if (!quote) return upper;
  const base = upper.slice(0, -quote.length);
  return `${base}-${quote}`;
};

const toGateSymbol = (symbol: string) => {
  if (!symbol) return symbol;
  const upper = symbol.toUpperCase();
  if (upper.includes('_')) return upper;
  return upper.endsWith('USDT') ? `${upper.slice(0, -4)}_USDT` : upper;
};

const getChartColors = (theme: 'dark' | 'light') => {
  if (theme === 'light') {
    return {
      text: '#0f172a',
      grid: 'rgba(15, 23, 42, 0.12)',
      candleUp: '#16a34a',
      candleUpBorder: '#15803d',
      candleDown: '#dc2626',
      candleDownBorder: '#b91c1c',
      zeroLine: '#0f172a',
    } as const;
  }
  return {
    text: '#e6e6e6',
    grid: 'rgba(148, 163, 184, 0.18)',
    candleUp: '#22d3ee',
    candleUpBorder: '#0891b2',
    candleDown: '#fb7185',
    candleDownBorder: '#f43f5e',
    zeroLine: '#f8fafc',
  } as const;
};

const resolveDefaultTimezone = () => {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
  } catch (error) {
    console.warn('Unable to resolve timezone, falling back to UTC', error);
    return 'UTC';
  }
};

const getTimezoneOptions = () => {
  const fallback = ['UTC', 'Europe/Moscow', 'America/New_York', 'Asia/Singapore'];
  const intlWithSupport = Intl as typeof Intl & {
    supportedValuesOf?: (input: string) => string[];
  };
  if (typeof intlWithSupport.supportedValuesOf === 'function') {
    try {
      return intlWithSupport.supportedValuesOf('timeZone');
    } catch (error) {
      console.warn('Failed to load timezone list, using fallback', error);
      return fallback;
    }
  }
  return fallback;
};

const formatTimezoneOffsetLabel = (timezone: string) => {
  try {
    const formatter = new Intl.DateTimeFormat('en-US', {
      timeZone: timezone,
      hour: '2-digit',
      minute: '2-digit',
      timeZoneName: 'shortOffset',
    });
    const parts = formatter.formatToParts(new Date());
    const tzPart = parts.find((part) => part.type === 'timeZoneName');
    if (tzPart) {
      const match = tzPart.value.match(/GMT([+-]\d{1,2})(?::(\d{2}))?/);
      if (match) {
        const sign = match[1].startsWith('-') ? '-' : '+';
        const hours = Math.abs(Number(match[1])).toString();
        const minutePart = match[2] && match[2] !== '00' ? `:${match[2]}` : '';
        return `${sign}${hours}${minutePart} UTC`;
      }
      if (tzPart.value === 'GMT') {
        return '+0 UTC';
      }
    }
  } catch (error) {
    console.warn('Unable to format timezone offset', error);
  }
  return timezone;
};

const PAIR_SETTINGS_KEY_PREFIX = 'pair-view-settings:';

type TimezoneOption = {
  value: string;
  label: string;
};

interface PairViewProps {
  symbol: string;
  initialLong?: string;
  initialShort?: string;
}

interface StoredPairSettings {
  timeframe?: (typeof TIMEFRAMES)[number];
  showExitChart?: boolean;
  volume?: string;
  timezone?: string;
  long_exchange?: string;
  short_exchange?: string;
}

export default function PairView({ symbol, initialLong, initialShort }: PairViewProps) {
  const symbolUpper = symbol.toUpperCase();
  const router = useRouter();
  const { theme, toggleTheme } = useTheme();
  const { data: statsData } = useStats();
  const settingsKey = useMemo(
    () => `${PAIR_SETTINGS_KEY_PREFIX}${symbolUpper}`,
    [symbolUpper],
  );
  const [settingsLoaded, setSettingsLoaded] = useState(false);
  const [pendingSelection, setPendingSelection] = useState<
    { long: string; short: string } | null
  >(null);
  const [timeframe, setTimeframe] = useState<(typeof TIMEFRAMES)[number]>('5m');
  const [showExitChart, setShowExitChart] = useState(false);
  const [volume, setVolume] = useState('100');
  const [timezone, setTimezone] = useState<string>(resolveDefaultTimezone);
  const [symbolDropdownOpen, setSymbolDropdownOpen] = useState(false);
  const [symbolQuery, setSymbolQuery] = useState('');

  const timezoneOptions = useMemo<TimezoneOption[]>(() => {
    return getTimezoneOptions().map((zone) => ({
      value: zone,
      label: formatTimezoneOffsetLabel(zone),
    }));
  }, []);

  const themeRef = useRef(theme);
  const timeframeRef = useRef(timeframe);
  const timezoneRef = useRef(timezone);
  const symbolDropdownRef = useRef<HTMLDivElement | null>(null);
  const symbolSearchInputRef = useRef<HTMLInputElement | null>(null);

  themeRef.current = theme;
  timeframeRef.current = timeframe;
  timezoneRef.current = timezone;

  useEffect(() => {
    if (settingsLoaded) return;
    if (typeof window === 'undefined') return;
    try {
      const raw = window.localStorage.getItem(settingsKey);
      if (!raw) {
        setSettingsLoaded(true);
        return;
      }
      const stored = JSON.parse(raw) as StoredPairSettings;
      if (stored.timeframe && TIMEFRAMES.includes(stored.timeframe)) {
        setTimeframe(stored.timeframe);
      }
      if (typeof stored.showExitChart === 'boolean') {
        setShowExitChart(stored.showExitChart);
      }
      if (typeof stored.volume === 'string') {
        setVolume(stored.volume);
      }
      if (stored.timezone) {
        setTimezone(stored.timezone);
      }
      if (stored.long_exchange && stored.short_exchange) {
        setPendingSelection({
          long: stored.long_exchange,
          short: stored.short_exchange,
        });
      }
    } catch (error) {
      console.warn('Failed to load pair settings', error);
    } finally {
      setSettingsLoaded(true);
    }
  }, [settingsKey, settingsLoaded]);

  const initialSelection = useMemo<PairSelection | null>(() => {
    if (!initialLong || !initialShort) {
      return null;
    }
    return {
      symbol: symbolUpper,
      long_exchange: initialLong.toLowerCase(),
      short_exchange: initialShort.toLowerCase(),
    };
  }, [initialLong, initialShort, symbolUpper]);

  const [selection, setSelection] = useState<PairSelection | null>(initialSelection);

  useEffect(() => {
    setSelection(initialSelection);
    setPendingSelection(null);
    setSettingsLoaded(false);
  }, [initialSelection, settingsKey]);

  const { data: overviewData } = usePairOverview(symbolUpper);
  const overviewRows = useMemo(
    () => overviewData?.rows ?? [],
    [overviewData?.rows],
  );

  const availableSymbols = useMemo(() => {
    const collected = new Set<string>();
    statsData?.symbols_subscribed?.forEach((item) => {
      if (item) {
        collected.add(String(item).toUpperCase());
      }
    });
    if (overviewData?.symbol) {
      collected.add(overviewData.symbol.toUpperCase());
    }
    collected.add(symbolUpper);
    return Array.from(collected).sort((a, b) => a.localeCompare(b));
  }, [overviewData?.symbol, statsData?.symbols_subscribed, symbolUpper]);

  const filteredSymbols = useMemo(() => {
    const normalizedQuery = symbolQuery.trim().toLowerCase();
    if (!normalizedQuery) {
      return availableSymbols;
    }
    return availableSymbols.filter((item) =>
      item.toLowerCase().startsWith(normalizedQuery)
    );
  }, [availableSymbols, symbolQuery]);

  const longOptions = useMemo(() => {
    const set = new Set<string>();
    overviewRows.forEach((row) => {
      set.add(row.long_exchange);
    });
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [overviewRows]);

  const shortOptions = useMemo(() => {
    const set = new Set<string>();
    overviewRows.forEach((row) => {
      set.add(row.short_exchange);
    });
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [overviewRows]);

  const selectedLong = selection?.long_exchange ?? longOptions[0] ?? '';
  const selectedShort = selection?.short_exchange ?? shortOptions[0] ?? '';

  const applySelection = (nextLong: string, nextShort: string) => {
    if (!overviewRows.length) return;
    const normalizedLong = nextLong.toLowerCase();
    const normalizedShort = nextShort.toLowerCase();

    const applyRow = (row: SpreadRow) => {
      if (
        selection &&
        selection.long_exchange === row.long_exchange &&
        selection.short_exchange === row.short_exchange
      ) {
        return;
      }
      setSelection({
        symbol: symbolUpper,
        long_exchange: row.long_exchange,
        short_exchange: row.short_exchange,
      });
    };

    const exact = overviewRows.find(
      (row) =>
        row.long_exchange === normalizedLong && row.short_exchange === normalizedShort,
    );
    if (exact) {
      applyRow(exact);
      return;
    }
    const fallbackLong = overviewRows.find((row) => row.long_exchange === normalizedLong);
    if (fallbackLong) {
      applyRow(fallbackLong);
      return;
    }
    const fallbackShort = overviewRows.find((row) => row.short_exchange === normalizedShort);
    if (fallbackShort) {
      applyRow(fallbackShort);
      return;
    }
    const first = overviewRows[0];
    if (first) {
      applyRow(first);
    }
  };

  const handleSymbolChange = (nextSymbol: string) => {
    const normalized = nextSymbol.trim().toUpperCase();
    if (!normalized || normalized === symbolUpper) {
      return;
    }
    router.push(`/pair/${encodeURIComponent(normalized)}`);
  };

  const handleSymbolSelect = (nextSymbol: string) => {
    setSymbolDropdownOpen(false);
    setSymbolQuery('');
    handleSymbolChange(nextSymbol);
  };

  useEffect(() => {
    if (!overviewRows.length) return;

    if (pendingSelection) {
      const match = overviewRows.find(
        (row) =>
          row.long_exchange === pendingSelection.long &&
          row.short_exchange === pendingSelection.short,
      );
      if (match) {
        setSelection({
          symbol: symbolUpper,
          long_exchange: match.long_exchange,
          short_exchange: match.short_exchange,
        });
        setPendingSelection(null);
        return;
      }
      setPendingSelection(null);
    }

    if (!selection) {
      if (initialSelection) {
        const match = overviewRows.find((row) => rowMatchesSelection(row, initialSelection));
        if (match) {
          setSelection({
            symbol: symbolUpper,
            long_exchange: match.long_exchange,
            short_exchange: match.short_exchange,
          });
          return;
        }
      }
      const first = overviewRows[0];
      if (first) {
        setSelection({
          symbol: symbolUpper,
          long_exchange: first.long_exchange,
          short_exchange: first.short_exchange,
        });
      }
    }
  }, [initialSelection, overviewRows, pendingSelection, selection, symbolUpper]);

  useEffect(() => {
    if (!selection || !overviewRows.length) return;
    const exists = overviewRows.some((row) => rowMatchesSelection(row, selection));
    if (!exists) {
      const fallback = overviewRows[0];
      if (fallback) {
        setSelection({
          symbol: symbolUpper,
          long_exchange: fallback.long_exchange,
          short_exchange: fallback.short_exchange,
        });
      }
    }
  }, [overviewRows, selection, symbolUpper]);

  useEffect(() => {
    if (!settingsLoaded) return;
    if (typeof window === 'undefined') return;
    const payload: StoredPairSettings = {
      timeframe,
      showExitChart,
      volume,
      timezone,
      long_exchange: selection?.long_exchange,
      short_exchange: selection?.short_exchange,
    };
    try {
      window.localStorage.setItem(settingsKey, JSON.stringify(payload));
    } catch (error) {
      console.warn('Failed to persist pair settings', error);
    }
  }, [selection, settingsKey, settingsLoaded, showExitChart, timeframe, timezone, volume]);

  const { data: entrySpreadsData } = usePairSpreads(selection, {
    timeframe,
    metric: 'entry',
    days: LOOKBACK_DAYS,
  });
  const reverseSelection = useMemo(() => {
    if (!selection) return null;
    return {
      symbol: selection.symbol,
      long_exchange: selection.short_exchange,
      short_exchange: selection.long_exchange,
    } satisfies PairSelection;
  }, [selection]);
  const { data: exitSpreadsData } = usePairSpreads(showExitChart ? reverseSelection : null, {
    timeframe,
    metric: 'entry',
    days: LOOKBACK_DAYS,
  });
  const { data: realtimeData } = usePairRealtime(selection);
  const { data: limitsData } = usePairLimits(selection);

  const overviewRow = useMemo(() => {
    if (!selection) return null;
    return overviewRows.find((row) => rowMatchesSelection(row, selection)) ?? null;
  }, [overviewRows, selection]);

  const realtimeRow = useMemo(() => {
    if (!selection || !realtimeData?.row) return null;
    if (rowMatchesSelection(realtimeData.row, selection)) return realtimeData.row;
    return null;
  }, [realtimeData, selection]);

  const activeRow = useMemo(() => {
    if (overviewRow && realtimeRow) return { ...overviewRow, ...realtimeRow };
    return realtimeRow ?? overviewRow ?? null;
  }, [overviewRow, realtimeRow]);

  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const zeroLineRef = useRef<IPriceLine | null>(null);
  const entrySeriesDataRef = useRef<CandleDataPoint[]>([]);
  const entryTimeframeSecondsRef = useRef<number>(getTimeframeSeconds(null, timeframe));

  const exitChartContainerRef = useRef<HTMLDivElement | null>(null);
  const exitChartRef = useRef<IChartApi | null>(null);
  const exitSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const exitZeroLineRef = useRef<IPriceLine | null>(null);
  const exitSeriesDataRef = useRef<CandleDataPoint[]>([]);
  const exitTimeframeSecondsRef = useRef<number>(getTimeframeSeconds(null, timeframe));

  const [latencyMs, setLatencyMs] = useState<number | null>(null);
  const latencySourceSecondsRef = useRef<number | null>(null);
  const clockOffsetMsRef = useRef<number | null>(null);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return undefined;
    }
    const interval = window.setInterval(() => {
      const sourceSeconds = latencySourceSecondsRef.current;
      if (!sourceSeconds) {
        return;
      }
      const now = Date.now();
      const sourceMs = sourceSeconds * 1000;
      const diff = now - sourceMs;
      const offset = clockOffsetMsRef.current ?? 0;
      const safeOffset = offset < 0 ? offset : 0;
      const normalized = diff < 0 ? diff - safeOffset : diff;
      const next = normalized > 0 ? Math.round(normalized) : 0;
      setLatencyMs((prev) => (prev === next ? prev : next));
    }, 250);
    return () => {
      window.clearInterval(interval);
    };
  }, []);

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container || chartRef.current) return;

    const colors = getChartColors(themeRef.current);

    const chart = createChart(container, {
      width: container.clientWidth,
      height: 340,
      layout: {
        background: { color: 'transparent' },
        textColor: colors.text,
      },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid },
      },
      timeScale: {
        borderColor: 'transparent',
        timeVisible: true,
        secondsVisible: timeframeRef.current === '1m',
        tickMarkFormatter: (time: Time) =>
          formatTimeWithTimezone(time, timeframeRef.current, timezoneRef.current),
      },
      rightPriceScale: {
        borderColor: 'transparent',
      },
      localization: {
        timeFormatter: (time: Time) =>
          formatTimeWithTimezone(time, timeframeRef.current, timezoneRef.current),
      },
    });

    const series = chart.addCandlestickSeries({
      upColor: colors.candleUp,
      downColor: colors.candleDown,
      borderUpColor: colors.candleUpBorder,
      borderDownColor: colors.candleDownBorder,
      wickUpColor: colors.candleUp,
      wickDownColor: colors.candleDown,
    });

    chartRef.current = chart;
    seriesRef.current = series;
    zeroLineRef.current = series.createPriceLine({
      price: 0,
      color: colors.zeroLine,
      lineWidth: 2,
      lineStyle: LineStyle.Solid,
      axisLabelVisible: true,
    });

    if (entrySeriesDataRef.current.length) {
      series.setData(entrySeriesDataRef.current);
      let timeframeSeconds = entryTimeframeSecondsRef.current;
      if (!Number.isFinite(timeframeSeconds) || timeframeSeconds <= 0) {
        timeframeSeconds = getTimeframeSeconds(null, timeframeRef.current);
      }
      applyDefaultVisibleRange(chart, entrySeriesDataRef.current, timeframeSeconds);
    }

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        chart.applyOptions({ width: entry.contentRect.width });
      }
    });
    observer.observe(container);
    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      zeroLineRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!showExitChart) {
      if (exitChartRef.current) exitChartRef.current.remove();
      exitChartRef.current = null;
      exitSeriesRef.current = null;
      exitZeroLineRef.current = null;
    }
  }, [showExitChart]);

  useEffect(() => {
    if (!showExitChart) return;
    const container = exitChartContainerRef.current;
    if (!container || exitChartRef.current) return;

    const colors = getChartColors(themeRef.current);

    const chart = createChart(container, {
      width: container.clientWidth,
      height: 340,
      layout: {
        background: { color: 'transparent' },
        textColor: colors.text,
      },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid },
      },
      timeScale: {
        borderColor: 'transparent',
        timeVisible: true,
        secondsVisible: timeframeRef.current === '1m',
        tickMarkFormatter: (time: Time) =>
          formatTimeWithTimezone(time, timeframeRef.current, timezoneRef.current),
      },
      rightPriceScale: {
        borderColor: 'transparent',
      },
      localization: {
        timeFormatter: (time: Time) =>
          formatTimeWithTimezone(time, timeframeRef.current, timezoneRef.current),
      },
    });

    const series = chart.addCandlestickSeries({
      upColor: colors.candleUp,
      downColor: colors.candleDown,
      borderUpColor: colors.candleUpBorder,
      borderDownColor: colors.candleDownBorder,
      wickUpColor: colors.candleUp,
      wickDownColor: colors.candleDown,
    });

    exitChartRef.current = chart;
    exitSeriesRef.current = series;
    exitZeroLineRef.current = series.createPriceLine({
      price: 0,
      color: colors.zeroLine,
      lineWidth: 2,
      lineStyle: LineStyle.Solid,
      axisLabelVisible: true,
    });

    if (exitSeriesDataRef.current.length) {
      series.setData(exitSeriesDataRef.current);
      let timeframeSeconds = exitTimeframeSecondsRef.current;
      if (!Number.isFinite(timeframeSeconds) || timeframeSeconds <= 0) {
        timeframeSeconds = getTimeframeSeconds(null, timeframeRef.current);
      }
      applyDefaultVisibleRange(chart, exitSeriesDataRef.current, timeframeSeconds);
    }

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        chart.applyOptions({ width: entry.contentRect.width });
      }
    });
    observer.observe(container);
    return () => {
      observer.disconnect();
      chart.remove();
      exitChartRef.current = null;
      exitSeriesRef.current = null;
      exitZeroLineRef.current = null;
    };
  }, [showExitChart]);

  useEffect(() => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) return;

    const colors = getChartColors(theme);
    chart.applyOptions({
      layout: {
        background: { color: 'transparent' },
        textColor: colors.text,
      },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: timeframe === '1m',
        tickMarkFormatter: (time: Time) => formatTimeWithTimezone(time, timeframe, timezone),
        borderColor: 'transparent',
      },
      localization: {
        timeFormatter: (time: Time) => formatTimeWithTimezone(time, timeframe, timezone),
      },
    });
    series.applyOptions({
      upColor: colors.candleUp,
      downColor: colors.candleDown,
      borderUpColor: colors.candleUpBorder,
      borderDownColor: colors.candleDownBorder,
      wickUpColor: colors.candleUp,
      wickDownColor: colors.candleDown,
    });
    if (zeroLineRef.current) {
      series.removePriceLine(zeroLineRef.current);
    }
    zeroLineRef.current = series.createPriceLine({
      price: 0,
      color: colors.zeroLine,
      lineWidth: 2,
      lineStyle: LineStyle.Solid,
      axisLabelVisible: true,
    });
    const timeframeSeconds = Number.isFinite(entryTimeframeSecondsRef.current)
      ? entryTimeframeSecondsRef.current
      : getTimeframeSeconds(null, timeframeRef.current);
    applyDefaultVisibleRange(chart, entrySeriesDataRef.current, timeframeSeconds);
  }, [theme, timeframe, timezone]);

  useEffect(() => {
    const chart = exitChartRef.current;
    const series = exitSeriesRef.current;
    if (!chart || !series) return;

    const colors = getChartColors(theme);
    chart.applyOptions({
      layout: {
        background: { color: 'transparent' },
        textColor: colors.text,
      },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: timeframe === '1m',
        tickMarkFormatter: (time: Time) => formatTimeWithTimezone(time, timeframe, timezone),
        borderColor: 'transparent',
      },
      localization: {
        timeFormatter: (time: Time) => formatTimeWithTimezone(time, timeframe, timezone),
      },
    });
    series.applyOptions({
      upColor: colors.candleUp,
      downColor: colors.candleDown,
      borderUpColor: colors.candleUpBorder,
      borderDownColor: colors.candleDownBorder,
      wickUpColor: colors.candleUp,
      wickDownColor: colors.candleDown,
    });
    if (exitZeroLineRef.current) {
      series.removePriceLine(exitZeroLineRef.current);
    }
    exitZeroLineRef.current = series.createPriceLine({
      price: 0,
      color: colors.zeroLine,
      lineWidth: 2,
      lineStyle: LineStyle.Solid,
      axisLabelVisible: true,
    });
    const timeframeSeconds = Number.isFinite(exitTimeframeSecondsRef.current)
      ? exitTimeframeSecondsRef.current
      : getTimeframeSeconds(null, timeframeRef.current);
    applyDefaultVisibleRange(chart, exitSeriesDataRef.current, timeframeSeconds);
  }, [theme, timeframe, timezone]);

  useEffect(() => {
    const candles = entrySpreadsData?.candles ?? [];
    const series = seriesRef.current;
    const chart = chartRef.current;

    if (!candles.length) {
      entrySeriesDataRef.current = [];
      series?.setData([]);
      return;
    }

    const data = candles.map(toCandleDataPoint).sort((a, b) => a.time - b.time);
    const timeframeSeconds = getTimeframeSeconds(
      entrySpreadsData?.timeframe_seconds,
      timeframeRef.current,
    );
    entryTimeframeSecondsRef.current = timeframeSeconds;
    entrySeriesDataRef.current = data;
    series?.setData(data);
    if (chart) {
      applyDefaultVisibleRange(chart, data, timeframeSeconds);
    }
  }, [entrySpreadsData?.candles, entrySpreadsData?.timeframe_seconds]);

  useEffect(() => {
    const candles = exitSpreadsData?.candles ?? [];
    const series = exitSeriesRef.current;
    const chart = exitChartRef.current;

    if (!candles.length) {
      exitSeriesDataRef.current = [];
      series?.setData([]);
      return;
    }

    const data = candles.map(toCandleDataPoint).sort((a, b) => a.time - b.time);
    const timeframeSeconds = getTimeframeSeconds(
      exitSpreadsData?.timeframe_seconds,
      timeframeRef.current,
    );
    exitTimeframeSecondsRef.current = timeframeSeconds;
    exitSeriesDataRef.current = data;
    series?.setData(data);
    if (chart) {
      applyDefaultVisibleRange(chart, data, timeframeSeconds);
    }
  }, [exitSpreadsData?.candles, exitSpreadsData?.timeframe_seconds]);

  useEffect(() => {
    entrySeriesDataRef.current = [];
    entryTimeframeSecondsRef.current = getTimeframeSeconds(null, timeframe);
    if (seriesRef.current) {
      seriesRef.current.setData([]);
    }
    if (chartRef.current) {
      applyDefaultVisibleRange(
        chartRef.current,
        entrySeriesDataRef.current,
        entryTimeframeSecondsRef.current,
      );
    }
    exitSeriesDataRef.current = [];
    exitTimeframeSecondsRef.current = getTimeframeSeconds(null, timeframe);
    if (exitSeriesRef.current) {
      exitSeriesRef.current.setData([]);
    }
    if (exitChartRef.current) {
      applyDefaultVisibleRange(
        exitChartRef.current,
        exitSeriesDataRef.current,
        exitTimeframeSecondsRef.current,
      );
    }
  }, [selection, timeframe]);

  useEffect(() => {
    const tsCandidate = Number(realtimeData?.ts);
    const fallbackTs = Number(realtimeRow?._ts);
    const timestamp: number | null = Number.isFinite(tsCandidate)
      ? tsCandidate
      : Number.isFinite(fallbackTs)
      ? fallbackTs
      : null;
    if (timestamp === null || !Number.isFinite(timestamp)) {
      return;
    }
    const valueCandidate = Number(
      Number.isFinite(realtimeRow?.entry_pct)
        ? realtimeRow?.entry_pct
        : activeRow?.entry_pct,
    );
    if (!Number.isFinite(valueCandidate)) {
      return;
    }
    const timeframeSeconds = getTimeframeSeconds(
      entrySpreadsData?.timeframe_seconds,
      timeframeRef.current,
    );
    appendLiveCandle(
      entrySeriesDataRef,
      valueCandidate,
      timestamp,
      timeframeSeconds,
      seriesRef.current,
      chartRef.current,
    );
  }, [
    activeRow?.entry_pct,
    entrySpreadsData?.timeframe_seconds,
    realtimeData?.ts,
    realtimeRow?._ts,
    realtimeRow?.entry_pct,
  ]);

  useEffect(() => {
    const tsCandidate = Number(realtimeData?.ts);
    const fallbackTs = Number(realtimeRow?._ts);
    const timestamp: number | null = Number.isFinite(tsCandidate)
      ? tsCandidate
      : Number.isFinite(fallbackTs)
      ? fallbackTs
      : null;
    if (timestamp === null || !Number.isFinite(timestamp)) {
      return;
    }
    const valueCandidate = Number(
      Number.isFinite(realtimeRow?.exit_pct)
        ? realtimeRow?.exit_pct
        : activeRow?.exit_pct,
    );
    if (!Number.isFinite(valueCandidate)) {
      return;
    }
    const timeframeSeconds = getTimeframeSeconds(
      exitSpreadsData?.timeframe_seconds,
      timeframeRef.current,
    );
    appendLiveCandle(
      exitSeriesDataRef,
      valueCandidate,
      timestamp,
      timeframeSeconds,
      exitSeriesRef.current,
      exitChartRef.current,
    );
  }, [
    activeRow?.exit_pct,
    exitSpreadsData?.timeframe_seconds,
    realtimeData?.ts,
    realtimeRow?._ts,
    realtimeRow?.exit_pct,
  ]);

  const entryPct = activeRow?.entry_pct ?? null;
  const exitPct = activeRow?.exit_pct ?? null;
  const fundingSpread = activeRow?.funding_spread ?? null;
  const feesPct = activeRow?.commission_total_pct ?? null;

  const {
    newest: latestDataTimestampSeconds,
    oldest: worstLatencyTimestampSeconds,
  } = useMemo(() => {
    const merged = activeRow ?? realtimeData?.row ?? null;
    const fallback = realtimeData?.ts ?? null;
    return resolveLatencyTimestampSummary(merged, fallback);
  }, [activeRow, realtimeData?.row, realtimeData?.ts]);

  useEffect(() => {
    if (!worstLatencyTimestampSeconds) {
      latencySourceSecondsRef.current = null;
      clockOffsetMsRef.current = null;
      setLatencyMs(null);
      return;
    }
    latencySourceSecondsRef.current = worstLatencyTimestampSeconds;
    const arrivalMs = Date.now();
    const sourceMs = worstLatencyTimestampSeconds * 1000;
    const diff = arrivalMs - sourceMs;
    if (!Number.isFinite(diff)) {
      return;
    }
    if (diff < 0 && (clockOffsetMsRef.current === null || diff < clockOffsetMsRef.current)) {
      clockOffsetMsRef.current = diff;
    }
    const offset = clockOffsetMsRef.current ?? 0;
    const safeOffset = offset < 0 ? offset : 0;
    const normalized = diff < 0 ? diff - safeOffset : diff;
    const next = normalized > 0 ? Math.round(normalized) : 0;
    setLatencyMs((prev) => (prev === next ? prev : next));
  }, [worstLatencyTimestampSeconds]);

  const lastUpdatedTs = latestDataTimestampSeconds;
  const lastUpdatedLabel = lastUpdatedTs
    ? new Intl.DateTimeFormat('ru-RU', {
        timeZone: timezone,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      }).format(new Date(lastUpdatedTs * 1000))
    : null;

  const handleReverse = () => {
    if (!selection) return;
    applySelection(selection.short_exchange, selection.long_exchange);
  };

  const longFundingInterval =
    activeRow?.funding_interval_long ?? overviewRow?.funding_interval_long ?? '—';
  const shortFundingInterval =
    activeRow?.funding_interval_short ?? overviewRow?.funding_interval_short ?? '—';

  const longFundingValue = activeRow?.funding_long ?? overviewRow?.funding_long ?? null;
  const shortFundingValue = activeRow?.funding_short ?? overviewRow?.funding_short ?? null;

  const longExchange = activeRow?.long_exchange ?? overviewRow?.long_exchange ?? null;
  const shortExchange = activeRow?.short_exchange ?? overviewRow?.short_exchange ?? null;

  const longLimit = limitsData?.long as Record<string, unknown> | null | undefined;
  const shortLimit = limitsData?.short as Record<string, unknown> | null | undefined;

  const longPrice =
    activeRow?.price_long_ask ?? overviewRow?.price_long_ask ?? overviewRow?.price_long_bid ?? null;
  const shortPrice =
    activeRow?.price_short_bid ?? overviewRow?.price_short_bid ?? overviewRow?.price_short_ask ?? null;

  const computeLimitUsd = (
    limit: Record<string, unknown> | null | undefined,
    price: number | null,
  ) => {
    if (!limit) return null;
    const notionalRaw = limit['max_notional'];
    const notional = Number(notionalRaw);
    if (Number.isFinite(notional) && notional > 0) {
      return notional;
    }
    const qtyRaw = limit['max_qty'];
    const qty = Number(qtyRaw);
    if (Number.isFinite(qty) && qty > 0 && Number.isFinite(price ?? NaN) && (price ?? 0) > 0) {
      return qty * (price as number);
    }
    return null;
  };

  const longLimitUsd = computeLimitUsd(longLimit, longPrice);
  const shortLimitUsd = computeLimitUsd(shortLimit, shortPrice);

  const documentTitleEntry = entryPct !== null ? entryPct.toFixed(2) : '—';
  const documentTitleExit = exitPct !== null ? exitPct.toFixed(2) : '—';

  const displaySymbol = useMemo(() => formatPairLabel(symbolUpper), [symbolUpper]);

  useEffect(() => {
    if (typeof document === 'undefined') return;
    document.title = `${documentTitleEntry} | ${documentTitleExit} | ${displaySymbol}`;
  }, [displaySymbol, documentTitleEntry, documentTitleExit]);

  useEffect(() => {
    if (!symbolDropdownOpen) return;
    const handleOutsideClick = (event: MouseEvent | TouchEvent) => {
      const target = event.target as Node;
      if (symbolDropdownRef.current && !symbolDropdownRef.current.contains(target)) {
        setSymbolDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleOutsideClick);
    document.addEventListener('touchstart', handleOutsideClick);
    return () => {
      document.removeEventListener('mousedown', handleOutsideClick);
      document.removeEventListener('touchstart', handleOutsideClick);
    };
  }, [symbolDropdownOpen]);

  useEffect(() => {
    if (symbolDropdownOpen) {
      symbolSearchInputRef.current?.focus();
    } else {
      setSymbolQuery('');
    }
  }, [symbolDropdownOpen]);

  useEffect(() => {
    setSymbolDropdownOpen(false);
    setSymbolQuery('');
  }, [symbolUpper]);

  return (
    <div className="page-container pair-container">
      <header className="pair-header">
        <h1 className="pair-title">
          <div
            className={`symbol-select ${symbolDropdownOpen ? 'open' : ''}`}
            ref={symbolDropdownRef}
          >
            <button
              type="button"
              className="symbol-select-trigger"
              onClick={() => setSymbolDropdownOpen((prev) => !prev)}
              aria-haspopup="listbox"
              aria-expanded={symbolDropdownOpen}
            >
              <span className="symbol-select-label">{displaySymbol}</span>
              <span className="symbol-select-chevron" aria-hidden="true" />
            </button>
            {symbolDropdownOpen ? (
              <div className="symbol-select-menu">
                <input
                  ref={symbolSearchInputRef}
                  type="text"
                  value={symbolQuery}
                  onChange={(event) => setSymbolQuery(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter') {
                      event.preventDefault();
                      const first = filteredSymbols[0];
                      if (first) {
                        handleSymbolSelect(first);
                      }
                    } else if (event.key === 'Escape') {
                      setSymbolDropdownOpen(false);
                    }
                  }}
                  placeholder="Поиск монеты"
                  className="symbol-select-search"
                  spellCheck={false}
                />
                <ul role="listbox" className="symbol-select-options">
                  {filteredSymbols.length ? (
                    filteredSymbols.map((item) => (
                      <li key={item}>
                        <button
                          type="button"
                          className={`symbol-select-option ${
                            item === symbolUpper ? 'active' : ''
                          }`}
                          onClick={() => handleSymbolSelect(item)}
                        >
                          {formatPairLabel(item)}
                        </button>
                      </li>
                    ))
                  ) : (
                    <li className="symbol-select-empty">Нет совпадений</li>
                  )}
                </ul>
              </div>
            ) : null}
          </div>
        </h1>
        <div className="header-actions">
          <label className="theme-toggle">
            <input
              type="checkbox"
              checked={theme === 'light'}
              onChange={toggleTheme}
              aria-label="Переключить тему"
            />
            <span className="theme-toggle-track">
              <span className="theme-toggle-thumb" />
            </span>
          </label>
        </div>
      </header>

      <section className="pair-layout">
        <div className="pair-controls">
          <label className="control-group">
            Таймфрейм
            <select
              value={timeframe}
              onChange={(event) => setTimeframe(event.target.value as (typeof TIMEFRAMES)[number])}
            >
              {TIMEFRAMES.map((tf) => (
                <option key={tf} value={tf}>
                  {tf === '1m' ? '1 мин' : tf === '5m' ? '5 мин' : '1 час'}
                </option>
              ))}
            </select>
          </label>
          <label className="control-group toggle-inline">
            <span>Отобразить график выхода</span>
            <span className="switch">
              <input
                type="checkbox"
                checked={showExitChart}
                onChange={(event) => setShowExitChart(event.target.checked)}
              />
              <span className="switch-track">
                <span className="switch-thumb" />
              </span>
            </span>
          </label>
          <button type="button" className="btn" onClick={handleReverse} disabled={!selection}>
            Перевернуть связку
          </button>
        </div>

        <div className="pair-main">
          <div className="chart-column">
            <div className="chart-card">
              <div className="chart-card-header">
                <h2>Вход %</h2>
                {lastUpdatedLabel ? (
                  <span className="muted small">Обновлено: {lastUpdatedLabel}</span>
                ) : null}
              </div>
              <div className="chart-container">
                <div ref={chartContainerRef} className="chart-surface"></div>
                <div className="chart-timezone-select">
                  <select
                    aria-label="Выбрать часовой пояс"
                    value={timezone}
                    onChange={(event) => setTimezone(event.target.value)}
                  >
                    {timezoneOptions.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
              {entrySeriesDataRef.current.length === 0 ? (
                <div className="chart-empty">Нет данных для выбранной комбинации</div>
              ) : null}
            </div>

            {showExitChart ? (
              <div className="chart-card">
                <div className="chart-card-header">
                  <h2>Выход %</h2>
                  {reverseSelection ? (
                    <span className="muted small">
                      {reverseSelection.long_exchange.toUpperCase()} →{' '}
                      {reverseSelection.short_exchange.toUpperCase()}
                    </span>
                  ) : null}
                </div>
                <div className="chart-container">
                  <div ref={exitChartContainerRef} className="chart-surface"></div>
                </div>
                {exitSeriesDataRef.current.length === 0 ? (
                  <div className="chart-empty">Нет данных для выбранной комбинации</div>
                ) : null}
              </div>
            ) : null}
          </div>

          <div className="summary-card">
            <div className="summary-top">
              <label className="volume-input">
                Объём сделки, USDT
                <input
                  type="number"
                  min="0"
                  step="10"
                  value={volume}
                  onChange={(event) => setVolume(event.target.value)}
                />
              </label>
              <div className="leg-selects">
                <label>
                  Long
                  <select
                    value={selectedLong}
                    onChange={(event) => {
                      applySelection(event.target.value, selectedShort);
                    }}
                    disabled={!longOptions.length}
                  >
                    {longOptions.map((option) => (
                      <option key={option} value={option}>
                        {option.toUpperCase()}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Short
                  <select
                    value={selectedShort}
                    onChange={(event) => {
                      applySelection(selectedLong, event.target.value);
                    }}
                    disabled={!shortOptions.length}
                  >
                    {shortOptions.map((option) => (
                      <option key={option} value={option}>
                        {option.toUpperCase()}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
            </div>

            {latencyMs !== null ? (
              <div className="latency-indicator">
                <span>Задержка обновления</span>
                <span
                  className={`latency-value ${latencyMs <= 500 ? 'latency-good' : 'latency-bad'}`}
                >
                  {latencyMs.toLocaleString('ru-RU')} мс
                </span>
              </div>
            ) : null}

            <div className="metrics-grid">
              <div className="metric-card">
                <div className="metric-label">Вход</div>
                <div className={`metric-value ${entryPct !== null ? (entryPct >= 0 ? 'metric-pos' : 'metric-neg') : ''}`}>
                  {formatPercent(entryPct)}
                </div>
              </div>
              <div className="metric-card">
                <div className="metric-label">Выход</div>
                <div className={`metric-value ${exitPct !== null ? (exitPct >= 0 ? 'metric-pos' : 'metric-neg') : ''}`}>
                  {formatPercent(exitPct)}
                </div>
              </div>
              <div className="metric-card">
                <div className="metric-label">Спред фандинга</div>
                <div className={`metric-value ${fundingSpread !== null ? (fundingSpread >= 0 ? 'metric-pos' : 'metric-neg') : ''}`}>
                  {formatFundingPercent(fundingSpread)}
                </div>
              </div>
              <div className="metric-card">
                <div className="metric-label">Комиссии</div>
                <div className="metric-value">{formatPercent(feesPct)}</div>
              </div>
            </div>

            <div className="limits-grid">
              <div className="limit-card">
                <div className="limit-title">
                  {longExchange ? (
                    <a
                      href={exchangeUrl(longExchange, symbolUpper)}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {longExchange.toUpperCase()}
                    </a>
                  ) : (
                    'LONG'
                  )}
                </div>
                <dl className="limit-details">
                  <div className="limit-row">
                    <dt>Лимит</dt>
                    <dd>{longLimitUsd !== null ? `${formatUsd(longLimitUsd)} USDT` : '—'}</dd>
                  </div>
                  <div className="limit-row">
                    <dt>Фандинг</dt>
                    <dd>
                      {formatFundingPercent(longFundingValue)} / {longFundingInterval}
                    </dd>
                  </div>
                </dl>
              </div>
              <div className="limit-card">
                <div className="limit-title">
                  {shortExchange ? (
                    <a
                      href={exchangeUrl(shortExchange, symbolUpper)}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {shortExchange.toUpperCase()}
                    </a>
                  ) : (
                    'SHORT'
                  )}
                </div>
                <dl className="limit-details">
                  <div className="limit-row">
                    <dt>Лимит</dt>
                    <dd>{shortLimitUsd !== null ? `${formatUsd(shortLimitUsd)} USDT` : '—'}</dd>
                  </div>
                  <div className="limit-row">
                    <dt>Фандинг</dt>
                    <dd>
                      {formatFundingPercent(shortFundingValue)} / {shortFundingInterval}
                    </dd>
                  </div>
                </dl>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
