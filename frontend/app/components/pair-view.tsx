'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import {
  usePairLimits,
  usePairOverview,
  usePairRealtime,
  usePairSpreads,
} from '../../lib/api';
import type { PairSelection, SpreadRow } from '../../lib/types';
import { createChart, type IChartApi, type ISeriesApi, type UTCTimestamp } from 'lightweight-charts';
import { useTheme } from './useTheme';

const LOOKBACK_DAYS = 10;
const TIMEFRAMES = ['1m', '5m', '1h'] as const;

const formatPercent = (value: number | null | undefined, digits = 4) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return `${value.toFixed(digits)}%`;
};

const formatFundingPercent = (value: number | null | undefined, digits = 4) => {
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
  if (!symbol) {
    return symbol;
  }
  if (symbol.includes('_')) {
    return symbol;
  }
  return symbol.endsWith('USDT') ? `${symbol.slice(0, -4)}_USDT` : symbol;
};

const toBingxSymbol = (symbol: string) => {
  if (!symbol) {
    return symbol;
  }
  const upper = symbol.toUpperCase();
  const quotes = ['USDT', 'USDC', 'USD', 'BUSD', 'FDUSD'];
  const quote = quotes.find((q) => upper.endsWith(q));
  if (!quote) {
    return upper;
  }
  const base = upper.slice(0, -quote.length);
  return `${base}-${quote}`;
};

const getChartColors = (theme: 'dark' | 'light') => {
  if (theme === 'light') {
    return {
      text: '#0f172a',
      grid: 'rgba(15, 23, 42, 0.12)',
      line: '#2563eb',
      areaTop: 'rgba(37, 99, 235, 0.35)',
      areaBottom: 'rgba(37, 99, 235, 0.05)',
    } as const;
  }
  return {
    text: '#e6e6e6',
    grid: 'rgba(148, 163, 184, 0.18)',
    line: '#4aa3ff',
    areaTop: 'rgba(74, 163, 255, 0.35)',
    areaBottom: 'rgba(74, 163, 255, 0.05)',
  } as const;
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
  const [metric, setMetric] = useState<'entry' | 'exit'>('entry');
  const [volume, setVolume] = useState('100');

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
    if (!selection || !overviewRows.length) {
      return;
    }
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

  const { data: spreadsData } = usePairSpreads(selection, {
    timeframe,
    metric,
    days: LOOKBACK_DAYS,
  });
  const { data: realtimeData } = usePairRealtime(selection);
  const { data: limitsData } = usePairLimits(selection);

  const overviewRow = useMemo(() => {
    if (!selection) {
      return null;
    }
    return overviewRows.find((row) => rowMatchesSelection(row, selection)) ?? null;
  }, [overviewRows, selection]);

  const realtimeRow = useMemo(() => {
    if (!selection || !realtimeData?.row) {
      return null;
    }
    if (rowMatchesSelection(realtimeData.row, selection)) {
      return realtimeData.row;
    }
    return null;
  }, [realtimeData, selection]);

  const activeRow = useMemo(() => {
    if (overviewRow && realtimeRow) {
      return { ...overviewRow, ...realtimeRow };
    }
    return realtimeRow ?? overviewRow ?? null;
  }, [overviewRow, realtimeRow]);

  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Area'> | null>(null);

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container || chartRef.current) {
      return;
    }

    const chart = createChart(container, {
      width: container.clientWidth,
      height: 340,
      layout: {
        background: { color: 'transparent' },
        textColor: getChartColors('dark').text,
      },
      grid: {
        vertLines: { color: getChartColors('dark').grid },
        horzLines: { color: getChartColors('dark').grid },
      },
      timeScale: {
        borderColor: 'transparent',
      },
      rightPriceScale: {
        borderColor: 'transparent',
      },
    });

    const series = chart.addAreaSeries({
      lineColor: getChartColors('dark').line,
      topColor: getChartColors('dark').areaTop,
      bottomColor: getChartColors('dark').areaBottom,
    });

    chartRef.current = chart;
    seriesRef.current = series;

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
    };
  }, []);

  useEffect(() => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) {
      return;
    }
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
    });
    series.applyOptions({
      lineColor: colors.line,
      topColor: colors.areaTop,
      bottomColor: colors.areaBottom,
    });
  }, [theme]);

  const candles = useMemo(
    () => spreadsData?.candles ?? [],
    [spreadsData?.candles],
  );

  useEffect(() => {
    const series = seriesRef.current;
    const chart = chartRef.current;
    if (!series || !chart) {
      return;
    }
    if (!candles.length) {
      series.setData([]);
      return;
    }
    const data = candles.map((candle) => ({
      time: Math.round(candle.ts) as UTCTimestamp,
      value: Number(candle.close.toFixed(6)),
    }));
    series.setData(data);
    chart.timeScale().fitContent();
  }, [candles]);

  const volumeValue = useMemo(() => {
    const parsed = Number.parseFloat(volume.replace(',', '.'));
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return 0;
    }
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
    ? new Date(lastUpdatedTs * 1000).toLocaleTimeString('ru-RU')
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
    if (!selection) {
      return;
    }
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
    if (!limit) {
      return 'Нет данных';
    }
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
    if (desc) {
      entries.push(String(desc));
    }
    if (!entries.length) {
      entries.push('Нет данных');
    }
    return entries.join(' • ');
  };

  return (
    <div className="page-container pair-container">
      <header className="pair-header">
        <div>
          <div className="breadcrumb">
            <a href="/">← Назад к таблице</a>
          </div>
          <h1>{symbolUpper}</h1>
        </div>
        <div className="header-actions">
          <button type="button" className="btn" onClick={toggleTheme}>
            Светлая/Тёмная
          </button>
        </div>
      </header>

      <section className="pair-layout">
        <div className="pair-left">
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
            <div className="control-group metric-toggle">
              <button
                type="button"
                className={metric === 'entry' ? 'btn active' : 'btn'}
                onClick={() => setMetric('entry')}
              >
                Вход
              </button>
              <button
                type="button"
                className={metric === 'exit' ? 'btn active' : 'btn'}
                onClick={() => setMetric('exit')}
              >
                Выход
              </button>
            </div>
            <button
              type="button"
              className="btn"
              onClick={handleReverse}
              disabled={!selection}
            >
              Перевернуть связку
            </button>
          </div>

          <div className="chart-card">
            <div className="chart-card-header">
              <h2>{metric === 'entry' ? 'Вход %' : 'Выход %'}</h2>
              {lastUpdatedLabel ? (
                <span className="muted small">Обновлено: {lastUpdatedLabel}</span>
              ) : null}
            </div>
            <div ref={chartContainerRef} className="chart-container"></div>
            {!candles.length ? (
              <div className="chart-empty">Нет данных для выбранной комбинации</div>
            ) : null}
          </div>
        </div>

        <div className="pair-right">
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
                  LONG:{' '}
                  <a
                    href={exchangeUrl(activeRow.long_exchange, symbolUpper)}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {activeRow.long_exchange.toUpperCase()}
                  </a>
                  , фандинг {formatFundingPercent(activeRow.funding_long)} / {longFundingInterval}
                </span>
                <span>
                  SHORT:{' '}
                  <a
                    href={exchangeUrl(activeRow.short_exchange, symbolUpper)}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {activeRow.short_exchange.toUpperCase()}
                  </a>
                  , фандинг {formatFundingPercent(activeRow.funding_short)} / {shortFundingInterval}
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
