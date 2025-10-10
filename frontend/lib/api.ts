'use client';

import { useEffect, useRef } from 'react';
import useSWR from 'swr';
import { getApiBaseUrl, getWsBaseUrl } from './config';
import type {
  ApiStats,
  PairLimitsResponse,
  PairOverviewResponse,
  PairSelection,
  PairSpreadsResponse,
  PairRealtimeResponse,
  SpreadRow,
} from './types';

const fetcher = async <T>(url: string): Promise<T> => {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }
  return (await response.json()) as T;
};

export function usePairOverview(symbol: string | null) {
  const baseUrl = getApiBaseUrl();
  const key = symbol ? `${baseUrl}/api/pair/${symbol}/overview` : null;
  return useSWR<PairOverviewResponse>(key, fetcher, {
    refreshInterval: 15_000,
  });
}

export function usePairSpreads(
  selection: PairSelection | null,
  options: { timeframe: string; metric: 'entry' | 'exit'; days: number },
) {
  const baseUrl = getApiBaseUrl();
  const key = selection
    ? `${baseUrl}/api/pair/${selection.symbol}/spreads?long=${selection.long_exchange}&short=${selection.short_exchange}&timeframe=${options.timeframe}&metric=${options.metric}&days=${options.days}`
    : null;
  return useSWR<PairSpreadsResponse>(key, fetcher, {
    refreshInterval: 60_000,
  });
}

export function usePairLimits(selection: PairSelection | null) {
  const baseUrl = getApiBaseUrl();
  const key = selection
    ? `${baseUrl}/api/pair/${selection.symbol}/limits?long=${selection.long_exchange}&short=${selection.short_exchange}`
    : null;
  return useSWR<PairLimitsResponse>(key, fetcher, {
    refreshInterval: 300_000,
  });
}

const RECONNECT_DELAY_MS = 400;

const normalizeExchange = (value: string | null | undefined) =>
  String(value ?? '').trim().toLowerCase();

const normalizeSymbol = (value: string | null | undefined) =>
  String(value ?? '').trim().toUpperCase();

const matchesSelection = (row: SpreadRow | null | undefined, selection: PairSelection | null) => {
  if (!row || !selection) {
    return false;
  }
  return (
    normalizeSymbol(row.symbol) === normalizeSymbol(selection.symbol) &&
    normalizeExchange(row.long_exchange) === normalizeExchange(selection.long_exchange) &&
    normalizeExchange(row.short_exchange) === normalizeExchange(selection.short_exchange)
  );
};

export function usePairRealtime(selection: PairSelection | null) {
  const baseUrl = getApiBaseUrl();
  const key = selection
    ? `${baseUrl}/api/pair/${selection.symbol}/realtime?long=${selection.long_exchange}&short=${selection.short_exchange}`
    : null;
  const swr = useSWR<PairRealtimeResponse>(key, fetcher, {
    refreshInterval: 0,
    revalidateOnFocus: false,
    revalidateOnReconnect: false,
  });

  const { mutate } = swr;
  const selectionRef = useRef<PairSelection | null>(selection);

  useEffect(() => {
    selectionRef.current = selection;
  }, [selection]);

  useEffect(() => {
    if (!key) {
      return undefined;
    }
    if (typeof window === 'undefined') {
      return undefined;
    }

    let ws: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let manualClose = false;
    let active = true;

    const connect = () => {
      if (!active) {
        return;
      }
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      const currentSelection = selectionRef.current;
      if (!currentSelection) {
        return;
      }
      const params = new URLSearchParams({
        symbol: currentSelection.symbol,
        long: currentSelection.long_exchange,
        short: currentSelection.short_exchange,
      });
      const wsUrl = `${getWsBaseUrl()}/ws/spreads?${params.toString()}`;
      try {
        ws = new WebSocket(wsUrl);
      } catch (error) {
        console.warn('Failed to open realtime websocket', error);
        reconnectTimer = setTimeout(connect, RECONNECT_DELAY_MS);
        return;
      }

      ws.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data) as unknown;
          if (!Array.isArray(payload)) {
            return;
          }
          const selectionNow = selectionRef.current;
          if (!selectionNow) {
            return;
          }
          const match = (payload as SpreadRow[]).find((row) => matchesSelection(row, selectionNow)) ?? null;
          const ts = match?._ts ?? Date.now() / 1000;
          const rowData = match ? { ...match } : null;
          if (rowData && (rowData._ts === undefined || rowData._ts === null)) {
            rowData._ts = ts;
          }
          mutate(
            {
              symbol: selectionNow.symbol,
              long_exchange: selectionNow.long_exchange,
              short_exchange: selectionNow.short_exchange,
              ts,
              row: rowData,
            },
            false,
          ).catch(() => {
            /* ignore */
          });
        } catch (error) {
          console.warn('Failed to parse realtime websocket payload', error);
        }
      };

      ws.onerror = () => {
        if (ws) {
          try {
            ws.close();
          } catch (closeError) {
            console.warn('Failed to close realtime websocket after error', closeError);
          }
        }
      };

      ws.onclose = () => {
        ws = null;
        if (!manualClose && active) {
          reconnectTimer = setTimeout(connect, RECONNECT_DELAY_MS);
        }
      };
    };

    connect();

    return () => {
      active = false;
      manualClose = true;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (ws) {
        try {
          ws.close();
        } catch (error) {
          console.warn('Failed to close realtime websocket', error);
        }
        ws = null;
      }
    };
  }, [key, mutate]);

  return swr;
}

export function useStats(initial?: ApiStats | null) {
  const baseUrl = getApiBaseUrl();
  return useSWR<ApiStats>(`${baseUrl}/stats`, fetcher, {
    refreshInterval: 60_000,
    fallbackData: initial ?? undefined,
  });
}
