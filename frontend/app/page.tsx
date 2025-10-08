import SpreadsTable from './components/spreads-table';
import { getApiBaseUrl } from '../lib/config';
import type { ApiStats } from '../lib/types';

export const dynamic = 'force-dynamic';

async function fetchStats(): Promise<ApiStats | null> {
  const baseUrl = getApiBaseUrl();
  try {
    const response = await fetch(`${baseUrl}/stats`, {
      cache: 'no-store',
      next: { revalidate: 5 },
    });
    if (!response.ok) {
      return null;
    }
    const payload = (await response.json()) as ApiStats;
    return payload;
  } catch (error) {
    console.error('Failed to load /stats', error);
    return null;
  }
}

export default async function Page() {
  const stats = await fetchStats();

  return (
    <SpreadsTable initialStats={stats} />
  );
}
