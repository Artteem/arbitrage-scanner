'use client';

import useSWR from 'swr';
import { getApiBaseUrl } from './config';
import type {
  ApiStats,
  PairLimitsResponse,
  PairOverviewResponse,
  PairSelection,
  PairSpreadsResponse,
  PairRealtimeResponse,
} from './types';

const fetcher = async <T>(url: string): Promise<T> => {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }
  return (await response.json()) as T;
};

export function usePairOverview(selection: PairSelection | null) {
  const baseUrl = getApiBaseUrl();
  const key = selection
    ? `${baseUrl}/api/pair/${selection.symbol}/overview`
    : null;
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

export function usePairRealtime(selection: PairSelection | null) {
  const baseUrl = getApiBaseUrl();
  const key = selection
    ? `${baseUrl}/api/pair/${selection.symbol}/realtime?long=${selection.long_exchange}&short=${selection.short_exchange}`
    : null;
  return useSWR<PairRealtimeResponse>(key, fetcher, {
    refreshInterval: 5_000,
  });
}

export function useStats(initial?: ApiStats | null) {
  const baseUrl = getApiBaseUrl();
  return useSWR<ApiStats>(`${baseUrl}/stats`, fetcher, {
    refreshInterval: 60_000,
    fallbackData: initial ?? undefined,
  });
}
