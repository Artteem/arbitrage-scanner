import PairView from '../../components/pair-view';

interface PairPageProps {
  params: { symbol: string };
  searchParams?: { [key: string]: string | string[] | undefined };
}

export const dynamic = 'force-dynamic';

export default function PairPage({ params, searchParams }: PairPageProps) {
  const symbol = params.symbol;
  const query = searchParams ?? {};
  const longParam = Array.isArray(query.long) ? query.long[0] : query.long;
  const shortParam = Array.isArray(query.short) ? query.short[0] : query.short;

  return (
    <PairView
      symbol={symbol}
      initialLong={typeof longParam === 'string' ? longParam : undefined}
      initialShort={typeof shortParam === 'string' ? shortParam : undefined}
    />
  );
}
