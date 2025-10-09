'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import {
  usePairLimits,
  usePairOverview,
  usePairRealtime,
  usePairSpreads,
} from '../../lib/api';
import type { PairSelection, SpreadRow } from '../../lib/types';
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

type TimezoneOption = {
  value: string;
  label: string;
};

interface PairViewProps {
  symbol: string;
  initialLong?: string;
  initialShort?: string;
}

export default function PairView({ symbol, initialLong, initialShort }: PairViewProps) {
  const symbolUpper = symbol.toUpperCase();
  const { theme, toggleTheme } = useTheme();
  const [timeframe, setTimeframe] = useState<(typeof TIMEFRAMES)[number]>('5m');
  const [showExitChart, setShowExitChart] = useState(false);
  const [volume, setVolume] = useState('100');
  const [timezone, setTimezone] = useState<string>(resolveDefaultTimezone);

  const timezoneOptions = useMemo<TimezoneOption[]>(() => {
    return getTimezoneOptions().map((zone) => ({
      value: zone,
      label: formatTimezoneOffsetLabel(zone),
    }));
  }, []);

  const themeRef = useRef(theme);
  const timeframeRef = useRef(timeframe);
  const timezoneRef = useRef(timezone);

  themeRef.current = theme;
  timeframeRef.current = timeframe;
  timezoneRef.current = timezone;

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

  const { data: overviewData } = usePairOverview(symbolUpper);
  const overviewRows = useMemo(
    () => overviewData?.rows ?? [],
    [overviewData?.rows],
  );

  useEffect(() => {
    if (!selection && overviewRows.length) {
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
      setSelection({
        symbol: symbolUpper,
        long_exchange: first.long_exchange,
        short_exchange: first.short_exchange,
      });
    }
  }, [initialSelection, overviewRows, selection, symbolUpper]);

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

  const exitChartContainerRef = useRef<HTMLDivElement | null>(null);
  const exitChartRef = useRef<IChartApi | null>(null);
  const exitSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const exitZeroLineRef = useRef<IPriceLine | null>(null);

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
    chart.timeScale().fitContent();
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
    chart.timeScale().fitContent();
  }, [theme, timeframe, timezone]);

  const entryCandles = useMemo(
    () => entrySpreadsData?.candles ?? [],
    [entrySpreadsData?.candles],
  );

  const exitCandles = useMemo(
    () => exitSpreadsData?.candles ?? [],
    [exitSpreadsData?.candles],
  );

  useEffect(() => {
    const series = seriesRef.current;
    const chart = chartRef.current;
    if (!series || !chart) return;

    if (!entryCandles.length) {
      series.setData([]);
      return;
    }
    const data = entryCandles.map((candle) => ({
      time: Math.round(candle.ts) as UTCTimestamp,
      open: Number(candle.open.toFixed(6)),
      high: Number(candle.high.toFixed(6)),
      low: Number(candle.low.toFixed(6)),
      close: Number(candle.close.toFixed(6)),
    }));
    series.setData(data);
    chart.timeScale().fitContent();
  }, [entryCandles]);

  useEffect(() => {
    const series = exitSeriesRef.current;
    const chart = exitChartRef.current;
    if (!series || !chart) return;

    if (!exitCandles.length) {
      series.setData([]);
      return;
    }
    const data = exitCandles.map((candle) => ({
      time: Math.round(candle.ts) as UTCTimestamp,
      open: Number(candle.open.toFixed(6)),
      high: Number(candle.high.toFixed(6)),
      low: Number(candle.low.toFixed(6)),
      close: Number(candle.close.toFixed(6)),
    }));
    series.setData(data);
    chart.timeScale().fitContent();
  }, [exitCandles]);

  const volumeValue = useMemo(() => {
    const parsed = Number.parseFloat(volume.replace(',', '.'));
    if (!Number.isFinite(parsed) || parsed <= 0) return 0;
    return parsed;
  }, [volume]);

  const entryPct = activeRow?.entry_pct ?? null;
  const exitPct = activeRow?.exit_pct ?? null;
  const fundingSpread = activeRow?.funding_spread ?? null;
  const feesPct = activeRow?.commission_total_pct ?? null;

  const entryUsd = entryPct !== null ? (entryPct / 100) * volumeValue : null;
  const exitUsd = exitPct !== null ? (exitPct / 100) * volumeValue : null;
  const fundingUsd = fundingSpread !== null ? fundingSpread * volumeValue : null;
  const feesUsd = feesPct !== null ? (feesPct / 100) * volumeValue : null;

  const lastUpdatedTs = realtimeData?.ts ?? realtimeRow?._ts ?? null;
  const lastUpdatedLabel = lastUpdatedTs
    ? new Intl.DateTimeFormat('ru-RU', {
        timeZone: timezone,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      }).format(new Date(lastUpdatedTs * 1000))
    : null;

  const selectionKey = selection
    ? `${selection.long_exchange}|${selection.short_exchange}`
    : '';

  const selectionOptions = useMemo(() => {
    return overviewRows
      .map((row) => ({
        key: `${row.long_exchange}|${row.short_exchange}`,
        label: `${row.long_exchange.toUpperCase()} → ${row.short_exchange.toUpperCase()}`,
      }))
      .sort((a, b) => a.label.localeCompare(b.label));
  }, [overviewRows]);

  const handleReverse = () => {
    if (!selection) return;
    setSelection({
      symbol: selection.symbol,
      long_exchange: selection.short_exchange,
      short_exchange: selection.long_exchange,
    });
  };

  const longFundingInterval =
    activeRow?.funding_interval_long ?? overviewRow?.funding_interval_long ?? '—';
  const shortFundingInterval =
    activeRow?.funding_interval_short ?? overviewRow?.funding_interval_short ?? '—';

  const longLimit = limitsData?.long as Record<string, unknown> | null | undefined;
  const shortLimit = limitsData?.short as Record<string, unknown> | null | undefined;

  const renderLimit = (limit: Record<string, unknown> | null | undefined) => {
    if (!limit) return 'Нет данных';
    const entries: string[] = [];
    const qty = limit['max_qty'];
    if (qty !== undefined) {
      const value = Number(qty);
      if (Number.isFinite(value) && value > 0) {
        entries.push(`Макс. объём: ${value.toLocaleString('ru-RU')}`);
      }
    }
    const notional = limit['max_notional'];
    if (notional !== undefined) {
      const value = Number(notional);
      if (Number.isFinite(value) && value > 0) {
        entries.push(`Макс. нотионал: ${value.toLocaleString('ru-RU')}`);
      }
    }
    const desc = limit['limit_desc'];
    if (desc) entries.push(String(desc));
    if (!entries.length) entries.push('Нет данных');
    return entries.join(' • ');
  };

  const documentTitleEntry = entryPct !== null ? entryPct.toFixed(2) : '—';
  const documentTitleExit = exitPct !== null ? exitPct.toFixed(2) : '—';

  useEffect(() => {
    if (typeof document === 'undefined') return;
    document.title = `${documentTitleEntry} | ${documentTitleExit} | ${symbolUpper}`;
  }, [documentTitleEntry, documentTitleExit, symbolUpper]);

  return (
    <div className="page-container pair-container">
      <header className="pair-header">
        <h1>{symbolUpper}</h1>
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
              {!entryCandles.length ? (
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
                {!exitCandles.length ? (
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
              <label className="combo-select">
                Комбинация
                <select
                  value={selectionKey}
                  onChange={(event) => {
                    const [longExchange, shortExchange] = event.target.value.split('|');
                    setSelection({
                      symbol: symbolUpper,
                      long_exchange: longExchange,
                      short_exchange: shortExchange,
                    });
                  }}
                  disabled={!selectionOptions.length}
                >
                  {selectionOptions.map((option) => (
                    <option key={option.key} value={option.key}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            <div className="metrics-grid">
              <div className="metric-card">
                <div className="metric-label">Вход</div>
                <div className={`metric-value ${entryPct !== null ? (entryPct >= 0 ? 'metric-pos' : 'metric-neg') : ''}`}>
                  {formatPercent(entryPct)}
                </div>
                <div className="metric-sub">{formatUsd(entryUsd)}</div>
              </div>
              <div className="metric-card">
                <div className="metric-label">Выход</div>
                <div className={`metric-value ${exitPct !== null ? (exitPct >= 0 ? 'metric-pos' : 'metric-neg') : ''}`}>
                  {formatPercent(exitPct)}
                </div>
                <div className="metric-sub">{formatUsd(exitUsd)}</div>
              </div>
              <div className="metric-card">
                <div className="metric-label">Спред фандинга</div>
                <div className={`metric-value ${fundingSpread !== null ? (fundingSpread >= 0 ? 'metric-pos' : 'metric-neg') : ''}`}>
                  {formatFundingPercent(fundingSpread)}
                </div>
                <div className="metric-sub">{formatUsd(fundingUsd)}</div>
              </div>
              <div className="metric-card">
                <div className="metric-label">Комиссии</div>
                <div className="metric-value">{formatPercent(feesPct)}</div>
                <div className="metric-sub">{formatUsd(feesUsd)}</div>
              </div>
            </div>

            {activeRow ? (
              <div className="exchange-line">
                <span>
                  LONG{' '}
                  <a
                    href={exchangeUrl(activeRow.long_exchange, symbolUpper)}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {activeRow.long_exchange.toUpperCase()}
                  </a>
                  : фандинг {formatFundingPercent(activeRow.funding_long)} / {longFundingInterval}
                </span>
                <span>
                  SHORT{' '}
                  <a
                    href={exchangeUrl(activeRow.short_exchange, symbolUpper)}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {activeRow.short_exchange.toUpperCase()}
                  </a>
                  : фандинг {formatFundingPercent(activeRow.funding_short)} / {shortFundingInterval}
                </span>
              </div>
            ) : (
              <p className="muted">Нет данных по выбранной комбинации.</p>
            )}

            <div className="limits-grid">
              <div className="limit-card">
                <div className="limit-title">{activeRow?.long_exchange.toUpperCase() ?? 'LONG'}</div>
                <div className="limit-body">{renderLimit(longLimit)}</div>
              </div>
              <div className="limit-card">
                <div className="limit-title">{activeRow?.short_exchange.toUpperCase() ?? 'SHORT'}</div>
                <div className="limit-body">{renderLimit(shortLimit)}</div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
