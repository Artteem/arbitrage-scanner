export function formatPairLabel(symbol: string | null | undefined): string {
  if (!symbol) {
    return '';
  }
  const upper = String(symbol).toUpperCase();
  const trimmed = upper.replace(/(?:[-_\/])?USDT$/i, '');
  return trimmed.length > 0 ? trimmed : upper;
}
