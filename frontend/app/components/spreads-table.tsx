'use client';

import { useEffect, useMemo, useState } from 'react';
import { useStats } from '../../lib/api';
import { getWsBaseUrl } from '../../lib/config';
import type { ApiStats, SpreadRow } from '../../lib/types';
import { useTheme } from './useTheme';

const PAGE_SIZE = 50;

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

const formatDateTime = (ts: number | null) => {
  if (!ts) {
    return '';
  }
  return new Date(ts).toLocaleTimeString('ru-RU');
};

type SortKey = 'entry' | 'funding';

type WsStatus = 'connecting' | 'open' | 'closed';

interface SpreadsTableProps {
  initialStats: ApiStats | null;
}

export default function SpreadsTable({ initialStats }: SpreadsTableProps) {
  const { theme, toggleTheme } = useTheme();
  const { data: stats } = useStats(initialStats ?? undefined);
  const [rows, setRows] = useState<SpreadRow[]>([]);
  const [wsStatus, setWsStatus] = useState<WsStatus>('connecting');
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>('entry');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [page, setPage] = useState(1);
  const [minEntry, setMinEntry] = useState('0');
  const [minFunding, setMinFunding] = useState('0');
  const [manualExchangeSelection, setManualExchangeSelection] = useState(false);
  const [exchangeFilters, setExchangeFilters] = useState<Record<string, boolean>>({});

  useEffect(() => {
    const wsUrl = `${getWsBaseUrl()}/ws/spreads`;
    const ws = new WebSocket(wsUrl);
    setWsStatus('connecting');

    ws.onopen = () => {
      setWsStatus('open');
    };
    ws.onclose = () => {
      setWsStatus('closed');
    };
    ws.onerror = () => {
      setWsStatus('closed');
    };
    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data) as SpreadRow[];
        if (!Array.isArray(payload)) {
          return;
        }
        setRows(payload);
        setLastUpdated(Date.now());
      } catch (error) {
        console.warn('Failed to parse websocket payload', error);
      }
    };

    return () => {
      try {
        ws.close();
      } catch (error) {
        // ignore
      }
    };
  }, []);

  const discoveredExchanges = useMemo(() => {
    const collected = new Set<string>();
    stats?.exchanges?.forEach((ex) => {
      collected.add(String(ex).trim().toLowerCase());
    });
    rows.forEach((row) => {
      if (row.long_exchange) {
        collected.add(String(row.long_exchange).trim().toLowerCase());
      }
      if (row.short_exchange) {
        collected.add(String(row.short_exchange).trim().toLowerCase());
      }
    });
    return Array.from(collected).sort((a, b) => a.localeCompare(b));
  }, [rows, stats]);

  useEffect(() => {
    if (!discoveredExchanges.length) {
      return;
    }
    setExchangeFilters((prev) => {
      if (!manualExchangeSelection) {
        const same =
          discoveredExchanges.length === Object.keys(prev).length &&
          discoveredExchanges.every((ex) => prev[ex] === true);
        if (same) {
          return prev;
        }
        const next: Record<string, boolean> = {};
        discoveredExchanges.forEach((ex) => {
          next[ex] = true;
        });
        return next;
      }
      const next: Record<string, boolean> = {};
      discoveredExchanges.forEach((ex) => {
        const key = ex;
        if (Object.prototype.hasOwnProperty.call(prev, key)) {
          next[key] = prev[key];
        } else {
          next[key] = false;
        }
      });
      const same =
        discoveredExchanges.length === Object.keys(prev).length &&
        discoveredExchanges.every((ex) => prev[ex] === next[ex]);
      return same ? prev : next;
    });
  }, [discoveredExchanges, manualExchangeSelection]);

  const processedRows = useMemo(() => {
    const minEntryValue = Number.parseFloat(minEntry.replace(',', '.'));
    const minFundingValue = Number.parseFloat(minFunding.replace(',', '.'));
    const minEntryThreshold = Number.isFinite(minEntryValue) ? minEntryValue : 0;
    const minFundingThreshold = Number.isFinite(minFundingValue)
      ? minFundingValue / 100
      : 0;
    const knownExchanges = new Set(Object.keys(exchangeFilters));
    const enabledExchanges = new Set(
      Object.entries(exchangeFilters)
        .filter(([, enabled]) => enabled)
        .map(([exchange]) => exchange),
    );

    const filtered = rows.filter((row) => {
      if (knownExchanges.size > 0) {
        const longKnown = knownExchanges.has(row.long_exchange);
        const shortKnown = knownExchanges.has(row.short_exchange);
        const longOk = !longKnown || enabledExchanges.has(row.long_exchange);
        const shortOk = !shortKnown || enabledExchanges.has(row.short_exchange);
        if (!longOk || !shortOk) {
          return false;
        }
      }
      if (row.entry_pct < minEntryThreshold) {
        return false;
      }
      if (Math.abs(row.funding_spread) < minFundingThreshold) {
        return false;
      }
      return true;
    });

    const key = sortKey === 'entry' ? 'entry_pct' : 'funding_spread';

    filtered.sort((a, b) => {
      const va = key === 'entry_pct' ? a.entry_pct : a.funding_spread;
      const vb = key === 'entry_pct' ? b.entry_pct : b.funding_spread;
      if (sortDir === 'desc') {
        return vb - va;
      }
      return va - vb;
    });

    return filtered;
  }, [rows, exchangeFilters, minEntry, minFunding, sortDir, sortKey]);

  useEffect(() => {
    const totalPages = Math.max(1, Math.ceil(processedRows.length / PAGE_SIZE));
    setPage((prev) => {
      const next = Math.min(prev, totalPages);
      return next === prev ? prev : next;
    });
  }, [processedRows.length]);

  const totalRows = processedRows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  const currentPage = Math.min(page, totalPages);
  const start = (currentPage - 1) * PAGE_SIZE;
  const currentRows = processedRows.slice(start, start + PAGE_SIZE);

  const handleSort = (key: SortKey) => {
    setSortKey((prevKey) => {
      if (prevKey === key) {
        setSortDir((prevDir) => (prevDir === 'desc' ? 'asc' : 'desc'));
        return prevKey;
      }
      setSortDir('desc');
      return key;
    });
  };

  const handleExchangeToggle = (exchange: string) => {
    setManualExchangeSelection(true);
    setExchangeFilters((prev) => {
      const next = { ...prev };
      next[exchange] = !next[exchange];
      return next;
    });
    setPage(1);
  };

  const wsStatusLabel =
    wsStatus === 'open'
      ? 'Данные обновляются'
      : wsStatus === 'connecting'
        ? 'Подключаемся…'
        : 'Нет соединения';

  return (
    <div className="page-container">
      <header className="table-header">
        <div className="status-indicator">
          <span className={`status-dot ${wsStatus}`}></span>
          <span>{wsStatusLabel}</span>
          {lastUpdated ? (
            <span className="muted small">Обновлено: {formatDateTime(lastUpdated)}</span>
          ) : null}
        </div>
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

      <section className="filters-row">
        <div className="exchange-filters">
          {discoveredExchanges.map((exchange) => {
            const value = exchangeFilters[exchange];
            const checked = manualExchangeSelection ? Boolean(value) : true;
            return (
              <label key={exchange} className="checkbox">
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => handleExchangeToggle(exchange)}
                />
                <span>{exchange.toUpperCase()}</span>
              </label>
            );
          })}
        </div>
        <div className="number-filter">
          <label>
            Мин. курсовой спред (%)
            <input
              type="number"
              step="0.01"
              value={minEntry}
              onChange={(event) => {
                setMinEntry(event.target.value);
                setPage(1);
              }}
            />
          </label>
        </div>
        <div className="number-filter">
          <label>
            Мин. фандинговый спред (%)
            <input
              type="number"
              step="0.01"
              value={minFunding}
              onChange={(event) => {
                setMinFunding(event.target.value);
                setPage(1);
              }}
            />
          </label>
        </div>
      </section>

      <div className="card table-card">
        <table className="spreads-table">
          <thead>
            <tr>
              <th>Пара</th>
              <th>LONG</th>
              <th>SHORT</th>
              <th
                className="sort"
                data-active={sortKey === 'entry'}
                data-direction={sortDir}
                onClick={() => handleSort('entry')}
              >
                Вход
              </th>
              <th>Выход</th>
              <th
                className="sort"
                data-active={sortKey === 'funding'}
                data-direction={sortDir}
                onClick={() => handleSort('funding')}
              >
                Спред фандинга
              </th>
              <th>Комиссия</th>
            </tr>
          </thead>
          <tbody>
            {currentRows.map((row) => {
              const pairUrl = `/pair/${encodeURIComponent(row.symbol)}?long=${encodeURIComponent(row.long_exchange)}&short=${encodeURIComponent(row.short_exchange)}`;
              return (
                <tr key={`${row.symbol}-${row.long_exchange}-${row.short_exchange}`}>
                  <td>
                    <a
                      href={pairUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="pair-link"
                    >
                      {row.symbol}
                    </a>
                  </td>
                  <td>
                    <div className="exchange-cell">
                      <a
                        href={exchangeUrl(row.long_exchange, row.symbol)}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        {row.long_exchange.toUpperCase()}
                      </a>
                      <div className="exchange-badge">
                        фандинг: {formatFundingPercent(row.funding_long, 4)} /{' '}
                        {row.funding_interval_long || '—'}
                      </div>
                    </div>
                  </td>
                  <td>
                    <div className="exchange-cell">
                      <a
                        href={exchangeUrl(row.short_exchange, row.symbol)}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        {row.short_exchange.toUpperCase()}
                      </a>
                      <div className="exchange-badge">
                        фандинг: {formatFundingPercent(row.funding_short, 4)} /{' '}
                        {row.funding_interval_short || '—'}
                      </div>
                    </div>
                  </td>
                  <td className={row.entry_pct >= 0 ? 'value-pos' : 'value-neg'}>
                    {formatPercent(row.entry_pct)}
                  </td>
                  <td className={row.exit_pct >= 0 ? 'value-pos' : 'value-neg'}>
                    {formatPercent(row.exit_pct)}
                  </td>
                  <td className={row.funding_spread >= 0 ? 'value-pos' : 'value-neg'}>
                    {formatFundingPercent(row.funding_spread)}
                  </td>
                  <td>{formatPercent(row.commission_total_pct)}</td>
                </tr>
              );
            })}
            {!currentRows.length ? (
              <tr>
                <td colSpan={7} className="empty-state">
                  Нет данных, попробуйте изменить фильтры.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
        <div className="pager">
          <button
            type="button"
            className="btn"
            onClick={() => setPage((prev) => Math.max(1, prev - 1))}
            disabled={currentPage <= 1}
          >
            Назад
          </button>
          <span>
            Страница {currentPage} из {totalPages} (всего {totalRows})
          </span>
          <button
            type="button"
            className="btn"
            onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
            disabled={currentPage >= totalPages}
          >
            Вперёд
          </button>
        </div>
      </div>
    </div>
  );
}
