'use client';

import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import {
  usePairLimits,
  usePairOverview,
  usePairRealtime,
  usePairSpreads,
  useStats,
} from '../../lib/api';
import { getWsBaseUrl } from '../../lib/config';
import type {
  ApiStats,
  PairSelection,
  SpreadCandle,
  SpreadRow,
} from '../../lib/types';

interface DashboardProps {
  initialStats: ApiStats | null;
}

const formatPercent = (value: number | null | undefined, digits = 2) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return `${value.toFixed(digits)}%`;
};

const formatNumber = (value: number | null | undefined, digits = 2) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return value.toLocaleString('en-US', {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
};

const formatTimestamp = (ts?: number | null) => {
  if (!ts) {
    return '—';
  }
  const date = new Date(ts * 1000);
  return date.toLocaleTimeString('ru-RU', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
};

const TIMEFRAMES = ['1m', '5m', '1h'] as const;
const LOOKBACK_OPTIONS: Array<{ label: string; value: number }> = [
  { label: '24 часа', value: 1 },
  { label: '7 дней', value: 7 },
  { label: '30 дней', value: 30 },
];

type Metric = 'entry' | 'exit';

function HistoryStats({ candles }: { candles: SpreadCandle[] }) {
  const values = useMemo(() => candles.map((c) => c.close), [candles]);
  const latest = values.at(-1);
  const max = values.length ? Math.max(...values) : null;
  const min = values.length ? Math.min(...values) : null;
  const avg =
    values.length && values.every((v) => Number.isFinite(v))
      ? values.reduce((acc, v) => acc + v, 0) / values.length
      : null;
  return (
    <div className="history-grid">
      <div>
        <div className="label">Последнее</div>
        <div className="value accent">{formatPercent(latest)}</div>
      </div>
      <div>
        <div className="label">Максимум</div>
        <div className="value">{formatPercent(max)}</div>
      </div>
      <div>
        <div className="label">Минимум</div>
        <div className="value">{formatPercent(min)}</div>
      </div>
      <div>
        <div className="label">Среднее</div>
        <div className="value">{formatPercent(avg)}</div>
      </div>
    </div>
  );
}

function HistoryPreview({ candles }: { candles: SpreadCandle[] }) {
  const points = useMemo(() => {
    if (!candles.length) {
      return '';
    }
    const values = candles.map((c) => c.close);
    const max = Math.max(...values);
    const min = Math.min(...values);
    const range = max - min || 1;
    return values
      .map((value, index) => {
        const x = (index / Math.max(values.length - 1, 1)) * 100;
        const y = 100 - ((value - min) / range) * 100;
        return `${x},${y}`;
      })
      .join(' ');
  }, [candles]);

  if (!points) {
    return <div className="empty-history">Нет данных</div>;
  }

  return (
    <svg className="history-chart" viewBox="0 0 100 100" preserveAspectRatio="none">
      <defs>
        <linearGradient id="historyGradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="rgba(56, 189, 248, 1)" />
          <stop offset="100%" stopColor="rgba(168, 85, 247, 1)" />
        </linearGradient>
      </defs>
      <polyline
        fill="none"
        stroke="url(#historyGradient)"
        strokeWidth="3"
        points={points}
      />
    </svg>
  );
}

function SpreadTable({
  rows,
  onSelect,
  selection,
}: {
  rows: SpreadRow[];
  onSelect: (row: SpreadRow) => void;
  selection: PairSelection | null;
}) {
  if (!rows.length) {
    return (
      <div className="card">
        <div className="card-title">Потоки котировок</div>
        <p className="muted">Ожидаем данные с сервера...</p>
      </div>
    );
  }

  return (
    <div className="card table-card">
      <div className="card-title">Активные спреды</div>
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Пара</th>
              <th>Лонг</th>
              <th>Шорт</th>
              <th>Вход</th>
              <th>Выход</th>
              <th>Funding</th>
              <th>Комиссия</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const isSelected =
                selection?.symbol === row.symbol &&
                selection?.long_exchange === row.long_exchange &&
                selection?.short_exchange === row.short_exchange;
              return (
                <tr
                  key={`${row.symbol}-${row.long_exchange}-${row.short_exchange}`}
                  className={isSelected ? 'selected' : ''}
                  onClick={() => onSelect(row)}
                >
                  <td>
                    <div className="symbol">{row.symbol}</div>
                    <div className="muted small">
                      ask {formatNumber(row.price_long_ask, 4)} / bid {formatNumber(row.price_short_bid, 4)}
                    </div>
                  </td>
                  <td>{row.long_exchange.toUpperCase()}</td>
                  <td>{row.short_exchange.toUpperCase()}</td>
                  <td className="accent">{formatPercent(row.entry_pct)}</td>
                  <td>{formatPercent(row.exit_pct)}</td>
                  <td>{formatPercent(row.funding_spread, 4)}</td>
                  <td>{formatPercent(row.commission_total_pct)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PairDetails({
  selection,
  overview,
  realtimeRow,
}: {
  selection: PairSelection | null;
  overview: SpreadRow | null;
  realtimeRow: SpreadRow | null;
}) {
  if (!selection) {
    return (
      <div className="card">
        <div className="card-title">Детали связки</div>
        <p className="muted">Выберите связку из таблицы слева.</p>
      </div>
    );
  }

  const baseRow = realtimeRow ?? overview;

  if (!baseRow) {
    return (
      <div className="card">
        <div className="card-title">Детали связки</div>
        <p className="muted">Нет данных по выбранной связке.</p>
      </div>
    );
  }

  return (
    <div className="card">
      <div className="card-title">{selection.symbol}</div>
      <div className="pair-grid">
        <div>
          <div className="label">Лонг</div>
          <div className="value">{selection.long_exchange.toUpperCase()}</div>
          <div className="muted small">
            ask {formatNumber(baseRow.price_long_ask, 4)} / bid {formatNumber(baseRow.price_long_bid, 4)}
          </div>
        </div>
        <div>
          <div className="label">Шорт</div>
          <div className="value">{selection.short_exchange.toUpperCase()}</div>
          <div className="muted small">
            ask {formatNumber(baseRow.price_short_ask, 4)} / bid {formatNumber(baseRow.price_short_bid, 4)}
          </div>
        </div>
        <div>
          <div className="label">Входной спред</div>
          <div className="value accent">{formatPercent(baseRow.entry_pct)}</div>
        </div>
        <div>
          <div className="label">Выходной спред</div>
          <div className="value">{formatPercent(baseRow.exit_pct)}</div>
        </div>
        <div>
          <div className="label">Funding лонга</div>
          <div className="value">{formatPercent(baseRow.funding_long, 4)}</div>
          <div className="muted small">интервал {baseRow.funding_interval_long || '—'}</div>
        </div>
        <div>
          <div className="label">Funding шорта</div>
          <div className="value">{formatPercent(baseRow.funding_short, 4)}</div>
          <div className="muted small">интервал {baseRow.funding_interval_short || '—'}</div>
        </div>
        <div>
          <div className="label">Спред Funding</div>
          <div className="value">{formatPercent(baseRow.funding_spread, 4)}</div>
        </div>
        <div>
          <div className="label">Полный цикл</div>
          <div className="value">{formatPercent(baseRow.commission_total_pct)}</div>
        </div>
      </div>
    </div>
  );
}

function LimitsCard({
  selection,
  limits,
}: {
  selection: PairSelection | null;
  limits: ReturnType<typeof usePairLimits>['data'];
}) {
  if (!selection) {
    return null;
  }

  const longLimit = limits?.long as Record<string, unknown> | null | undefined;
  const shortLimit = limits?.short as Record<string, unknown> | null | undefined;

  const renderLimit = (limit: Record<string, unknown> | null | undefined) => {
    if (!limit) {
      return <p className="muted small">Нет данных</p>;
    }

    const entries: ReactNode[] = [];
    const maxQtyRaw = limit['max_qty'];
    if (maxQtyRaw !== undefined) {
      const maxQty = Number(maxQtyRaw);
      if (!Number.isNaN(maxQty) && maxQty > 0) {
        entries.push(<li key="qty">Макс. объём: {formatNumber(maxQty, 2)}</li>);
      }
    }

    const maxNotionalRaw = limit['max_notional'];
    if (maxNotionalRaw !== undefined) {
      const maxNotional = Number(maxNotionalRaw);
      if (!Number.isNaN(maxNotional) && maxNotional > 0) {
        entries.push(
          <li key="notional">Макс. нотионал: {formatNumber(maxNotional, 2)}</li>,
        );
      }
    }

    const limitDescRaw = limit['limit_desc'];
    if (limitDescRaw) {
      entries.push(<li key="desc">{String(limitDescRaw)}</li>);
    }

    if (!entries.length) {
      entries.push(<li key="empty">Нет данных</li>);
    }

    return <ul className="limits-list">{entries}</ul>;
  };

  return (
    <div className="card">
      <div className="card-title">Лимиты бирж</div>
      <div className="limits-grid">
        <div>
          <div className="label">{selection.long_exchange.toUpperCase()}</div>
          {renderLimit(longLimit)}
        </div>
        <div>
          <div className="label">{selection.short_exchange.toUpperCase()}</div>
          {renderLimit(shortLimit)}
        </div>
      </div>
    </div>
  );
}

function OverviewCard({ overview }: { overview: SpreadRow[] | undefined }) {
  if (!overview?.length) {
    return null;
  }
  return (
    <div className="card">
      <div className="card-title">Все комбинации</div>
      <div className="chips">
        {overview.map((row) => (
          <span key={`${row.long_exchange}-${row.short_exchange}`} className="chip">
            {row.long_exchange.toUpperCase()} → {row.short_exchange.toUpperCase()} ({formatPercent(row.entry_pct)})
          </span>
        ))}
      </div>
    </div>
  );
}

function Header({
  stats,
  wsStatus,
  lastUpdated,
}: {
  stats: ApiStats | undefined;
  wsStatus: string;
  lastUpdated: number | null;
}) {
  return (
    <header className="top-bar">
      <div>
        <h1>Arbitrage Scanner</h1>
        <p className="muted">
          {stats
            ? `Биржи: ${stats.exchanges.join(', ')} • Подписок: ${stats.symbols_subscribed.length}`
            : 'Не удалось получить статистику API'}
        </p>
      </div>
      <div className="status">
        <span className={`status-dot ${wsStatus}`}></span>
        <span>{wsStatus === 'open' ? 'Данные обновляются' : wsStatus === 'connecting' ? 'Подключаемся…' : 'Нет соединения'}</span>
        <span className="muted small">
          {lastUpdated ? `Обновлено: ${new Date(lastUpdated).toLocaleTimeString('ru-RU')}` : ''}
        </span>
      </div>
    </header>
  );
}

export default function Dashboard({ initialStats }: DashboardProps) {
  const { data: stats } = useStats(initialStats ?? undefined);
  const [rows, setRows] = useState<SpreadRow[]>([]);
  const [wsStatus, setWsStatus] = useState<'connecting' | 'open' | 'closed'>('connecting');
  const [search, setSearch] = useState('');
  const [selected, setSelected] = useState<PairSelection | null>(null);
  const [timeframe, setTimeframe] = useState<(typeof TIMEFRAMES)[number]>('1m');
  const [metric, setMetric] = useState<Metric>('entry');
  const [lookbackDays, setLookbackDays] = useState<number>(7);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const hasInitialSelection = useRef(false);

  useEffect(() => {
    const wsUrl = `${getWsBaseUrl()}/ws/spreads`;
    let cancelled = false;
    const ws = new WebSocket(wsUrl);
    setWsStatus('connecting');

    ws.onopen = () => {
      if (!cancelled) {
        setWsStatus('open');
      }
    };

    ws.onclose = () => {
      if (!cancelled) {
        setWsStatus('closed');
      }
    };

    ws.onerror = () => {
      if (!cancelled) {
        setWsStatus('closed');
      }
    };

    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data) as SpreadRow[];
        if (!Array.isArray(payload)) {
          return;
        }
        payload.sort((a, b) => b.entry_pct - a.entry_pct);
        setRows(payload);
        setLastUpdated(Date.now());
        if (!hasInitialSelection.current && payload.length) {
          const first = payload[0];
          setSelected({
            symbol: first.symbol,
            long_exchange: first.long_exchange,
            short_exchange: first.short_exchange,
          });
          hasInitialSelection.current = true;
        }
      } catch (error) {
        console.warn('Failed to parse websocket payload', error);
      }
    };

    return () => {
      cancelled = true;
      try {
        ws.close();
      } catch (error) {
        // ignore
      }
    };
  }, []);

  const filteredRows = useMemo(() => {
    const query = search.trim().toUpperCase();
    if (!query) {
      return rows.slice(0, 100);
    }
    return rows.filter((row) => row.symbol.toUpperCase().includes(query)).slice(0, 100);
  }, [rows, search]);

  const selection = selected;
  const { data: overviewData } = usePairOverview(selection);
  const { data: spreadsData } = usePairSpreads(selection, {
    timeframe,
    metric,
    days: lookbackDays,
  });
  const { data: limitsData } = usePairLimits(selection);
  const { data: realtimeData } = usePairRealtime(selection);

  const realtimeRow = useMemo(() => {
    if (realtimeData?.row) {
      return realtimeData.row;
    }
    if (!selection) {
      return null;
    }
    const fromStream = rows.find(
      (row) =>
        row.symbol === selection.symbol &&
        row.long_exchange === selection.long_exchange &&
        row.short_exchange === selection.short_exchange,
    );
    return fromStream ?? null;
  }, [realtimeData, rows, selection]);

  const overviewRow = useMemo(() => {
    if (!selection) {
      return null;
    }
    const rowsList = overviewData?.rows || [];
    return (
      rowsList.find(
        (row) =>
          row.long_exchange === selection.long_exchange &&
          row.short_exchange === selection.short_exchange,
      ) ?? null
    );
  }, [overviewData, selection]);

  const historyCandles = spreadsData?.candles ?? [];

  return (
    <div className="container">
      <Header stats={stats} wsStatus={wsStatus} lastUpdated={lastUpdated} />
      <section className="content">
        <div className="left-column">
          <div className="card">
            <div className="card-title">Фильтр</div>
            <input
              className="search-input"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Например, BTCUSDT"
            />
          </div>
          <SpreadTable
            rows={filteredRows}
            selection={selection}
            onSelect={(row) =>
              setSelected({
                symbol: row.symbol,
                long_exchange: row.long_exchange,
                short_exchange: row.short_exchange,
              })
            }
          />
        </div>
        <div className="right-column">
          <PairDetails selection={selection} overview={overviewRow} realtimeRow={realtimeRow} />

          <div className="card">
            <div className="card-title">История спреда</div>
            <div className="controls">
              <div className="control-group">
                {TIMEFRAMES.map((tf) => (
                  <button
                    key={tf}
                    className={tf === timeframe ? 'control active' : 'control'}
                    onClick={() => setTimeframe(tf)}
                  >
                    {tf}
                  </button>
                ))}
              </div>
              <div className="control-group">
                <button
                  className={metric === 'entry' ? 'control active' : 'control'}
                  onClick={() => setMetric('entry')}
                >
                  Вход
                </button>
                <button
                  className={metric === 'exit' ? 'control active' : 'control'}
                  onClick={() => setMetric('exit')}
                >
                  Выход
                </button>
              </div>
              <div className="control-group">
                {LOOKBACK_OPTIONS.map((option) => (
                  <button
                    key={option.value}
                    className={lookbackDays === option.value ? 'control active' : 'control'}
                    onClick={() => setLookbackDays(option.value)}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
            </div>
            {selection ? (
              historyCandles.length ? (
                <>
                  <HistoryPreview candles={historyCandles} />
                  <HistoryStats candles={historyCandles} />
                  <div className="muted small">
                    Последнее обновление: {formatTimestamp(realtimeRow?._ts ?? realtimeData?.ts ?? null)}
                  </div>
                </>
              ) : (
                <p className="muted">Нет данных для выбранного набора параметров.</p>
              )
            ) : (
              <p className="muted">Выберите связку для просмотра истории.</p>
            )}
          </div>

          <LimitsCard selection={selection} limits={limitsData} />
          <OverviewCard overview={overviewData?.rows} />
        </div>
      </section>
    </div>
  );
}
