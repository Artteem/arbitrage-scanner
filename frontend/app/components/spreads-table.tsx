'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { useStats } from '../../lib/api';
import { getWsBaseUrl } from '../../lib/config';
import type { ApiStats, SpreadRow } from '../../lib/types';
import { useTheme } from './useTheme';

const PAGE_SIZE = 10;
const STORAGE_KEY = 'spreads-table-settings';

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

interface StoredTableSettings {
  page?: number;
  sortKey?: SortKey;
  sortDir?: 'asc' | 'desc';
  minEntry?: string;
  minFunding?: string;
  exchangeFilters?: Record<string, boolean>;
  manualExchangeSelection?: boolean;
  blacklist?: string[];
}

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
  const [blacklist, setBlacklist] = useState<string[]>([]);
  const [blacklistQuery, setBlacklistQuery] = useState('');
  const [blacklistOpen, setBlacklistOpen] = useState(false);
  const [settingsLoaded, setSettingsLoaded] = useState(false);
  const blacklistRef = useRef<HTMLDivElement | null>(null);
  const blacklistInputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (settingsLoaded) {
      return;
    }
    if (typeof window === 'undefined') {
      return;
    }
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      if (!raw) {
        setSettingsLoaded(true);
        return;
      }
      const stored = JSON.parse(raw) as StoredTableSettings;
      if (stored.sortKey === 'entry' || stored.sortKey === 'funding') {
        setSortKey(stored.sortKey);
      }
      if (stored.sortDir === 'asc' || stored.sortDir === 'desc') {
        setSortDir(stored.sortDir);
      }
      if (typeof stored.page === 'number' && Number.isFinite(stored.page) && stored.page >= 1) {
        setPage(Math.floor(stored.page));
      }
      if (typeof stored.minEntry === 'string') {
        setMinEntry(stored.minEntry);
      }
      if (typeof stored.minFunding === 'string') {
        setMinFunding(stored.minFunding);
      }
      if (typeof stored.manualExchangeSelection === 'boolean') {
        setManualExchangeSelection(stored.manualExchangeSelection);
      }
      if (stored.exchangeFilters) {
        setExchangeFilters(stored.exchangeFilters);
      }
      if (Array.isArray(stored.blacklist)) {
        const normalized = stored.blacklist
          .map((item) => String(item).toUpperCase())
          .filter((item) => item.length > 0);
        setBlacklist(Array.from(new Set(normalized)));
      }
    } catch (error) {
      console.warn('Failed to load table settings', error);
    } finally {
      setSettingsLoaded(true);
    }
  }, [settingsLoaded]);

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

  const availablePairs = useMemo(() => {
    const collected = new Set<string>();
    stats?.symbols_subscribed?.forEach((symbol) => {
      if (symbol) {
        collected.add(String(symbol).toUpperCase());
      }
    });
    rows.forEach((row) => {
      if (row.symbol) {
        collected.add(String(row.symbol).toUpperCase());
      }
    });
    return Array.from(collected).sort((a, b) => a.localeCompare(b));
  }, [rows, stats?.symbols_subscribed]);

  const blacklistSet = useMemo(() => {
    return new Set(blacklist.map((item) => item.toUpperCase()));
  }, [blacklist]);

  const filteredPairs = useMemo(() => {
    const query = blacklistQuery.trim().toUpperCase();
    return availablePairs
      .filter((symbol) => {
        if (blacklistSet.has(symbol)) {
          return false;
        }
        if (!query) {
          return true;
        }
        return symbol.includes(query);
      })
      .slice(0, 20);
  }, [availablePairs, blacklistQuery, blacklistSet]);

  useEffect(() => {
    if (!blacklistOpen) {
      return;
    }
    const handleClick = (event: MouseEvent) => {
      if (!blacklistRef.current) return;
      if (!blacklistRef.current.contains(event.target as Node)) {
        setBlacklistOpen(false);
        setBlacklistQuery('');
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => {
      document.removeEventListener('mousedown', handleClick);
    };
  }, [blacklistOpen]);

  useEffect(() => {
    if (!discoveredExchanges.length || !settingsLoaded) {
      return;
    }
    setExchangeFilters((prev) => {
      if (!manualExchangeSelection) {
        const allEnabled: Record<string, boolean> = {};
        discoveredExchanges.forEach((ex) => {
          allEnabled[ex] = true;
        });
        const same =
          discoveredExchanges.length === Object.keys(prev).length &&
          discoveredExchanges.every((ex) => prev[ex] === true);
        return same ? prev : allEnabled;
      }
      const next: Record<string, boolean> = {};
      let changed = false;
      discoveredExchanges.forEach((ex) => {
        if (Object.prototype.hasOwnProperty.call(prev, ex)) {
          next[ex] = prev[ex];
        } else {
          next[ex] = false;
          changed = true;
        }
      });
      Object.keys(prev).forEach((key) => {
        if (!Object.prototype.hasOwnProperty.call(next, key)) {
          changed = true;
        }
      });
      return changed ? next : prev;
    });
  }, [discoveredExchanges, manualExchangeSelection, settingsLoaded]);

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
      const symbolUpper = String(row.symbol).toUpperCase();
      if (blacklistSet.size > 0 && blacklistSet.has(symbolUpper)) {
        return false;
      }
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
  }, [rows, blacklistSet, exchangeFilters, minEntry, minFunding, sortDir, sortKey]);

  useEffect(() => {
    const totalPages = Math.max(1, Math.ceil(processedRows.length / PAGE_SIZE));
    setPage((prev) => {
      const next = Math.min(prev, totalPages);
      return next === prev ? prev : next;
    });
  }, [processedRows.length]);

  useEffect(() => {
    if (!settingsLoaded) {
      return;
    }
    if (typeof window === 'undefined') {
      return;
    }
    const payload: StoredTableSettings = {
      page,
      sortKey,
      sortDir,
      minEntry,
      minFunding,
      exchangeFilters,
      manualExchangeSelection,
      blacklist,
    };
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    } catch (error) {
      console.warn('Failed to persist table settings', error);
    }
  }, [blacklist, exchangeFilters, manualExchangeSelection, minEntry, minFunding, page, settingsLoaded, sortDir, sortKey]);

  const totalRows = processedRows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  const currentPage = Math.min(page, totalPages);
  const start = (currentPage - 1) * PAGE_SIZE;
  const currentRows = processedRows.slice(start, start + PAGE_SIZE);
  const pageNumbers = useMemo(
    () => Array.from({ length: totalPages }, (_, index) => index + 1),
    [totalPages],
  );

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

  const handleBlacklistAdd = (symbol: string) => {
    const upper = symbol.toUpperCase();
    setBlacklist((prev) => {
      if (prev.includes(upper)) {
        return prev;
      }
      return [...prev, upper];
    });
    setBlacklistQuery('');
    setBlacklistOpen(false);
    setPage(1);
  };

  const handleBlacklistRemove = (symbol: string) => {
    const upper = symbol.toUpperCase();
    setBlacklist((prev) => prev.filter((item) => item !== upper));
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
        <div className="blacklist-filter" ref={blacklistRef}>
          <label>
            Чёрный список
            <div
              className="multi-select"
              onClick={() => {
                blacklistInputRef.current?.focus();
                setBlacklistOpen(true);
              }}
            >
              {blacklist.map((symbol) => (
                <span key={symbol} className="multi-chip">
                  {symbol}
                  <button
                    type="button"
                    className="chip-remove"
                    onClick={() => handleBlacklistRemove(symbol)}
                    aria-label={`Убрать ${symbol} из чёрного списка`}
                  >
                    ×
                  </button>
                </span>
              ))}
              <input
                ref={blacklistInputRef}
                type="text"
                value={blacklistQuery}
                onChange={(event) => {
                  setBlacklistQuery(event.target.value);
                  setBlacklistOpen(true);
                }}
                onFocus={() => setBlacklistOpen(true)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter') {
                    event.preventDefault();
                    const normalized = event.currentTarget.value.trim().toUpperCase();
                    if (normalized && availablePairs.includes(normalized)) {
                      handleBlacklistAdd(normalized);
                    } else if (filteredPairs.length === 1) {
                      handleBlacklistAdd(filteredPairs[0]);
                    }
                  } else if (event.key === 'Escape') {
                    setBlacklistOpen(false);
                    setBlacklistQuery('');
                    event.currentTarget.blur();
                  } else if (event.key === 'Backspace' && !event.currentTarget.value && blacklist.length) {
                    handleBlacklistRemove(blacklist[blacklist.length - 1]);
                  }
                }}
                placeholder="Выберите пары"
                autoComplete="off"
                spellCheck={false}
              />
            </div>
          </label>
          {blacklistOpen ? (
            <ul className="multi-options">
              {filteredPairs.length ? (
                filteredPairs.map((symbol) => (
                  <li key={symbol}>
                    <button type="button" onClick={() => handleBlacklistAdd(symbol)}>
                      {symbol}
                    </button>
                  </li>
                ))
              ) : (
                <li className="multi-empty">Нет совпадений</li>
              )}
            </ul>
          ) : null}
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
        <div className="table-scroll">
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
                        фандинг: {formatFundingPercent(row.funding_long)} /{' '}
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
                        фандинг: {formatFundingPercent(row.funding_short)} /{' '}
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
        </div>
        <div className="pager">
          <div className="pager-nav">
            <button
              type="button"
              className="btn"
              onClick={() => setPage((prev) => Math.max(1, prev - 1))}
              disabled={currentPage <= 1}
            >
              Назад
            </button>
            <div className="pager-pages">
              {pageNumbers.map((pageNumber) => (
                <button
                  key={pageNumber}
                  type="button"
                  className={`pager-page${pageNumber === currentPage ? ' active' : ''}`}
                  onClick={() => setPage(pageNumber)}
                  aria-current={pageNumber === currentPage ? 'page' : undefined}
                >
                  {pageNumber}
                </button>
              ))}
            </div>
            <button
              type="button"
              className="btn"
              onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
              disabled={currentPage >= totalPages}
            >
              Вперёд
            </button>
          </div>
          <span className="pager-info">
            Страница {currentPage} из {totalPages} (всего {totalRows})
          </span>
        </div>
      </div>
    </div>
  );
}
