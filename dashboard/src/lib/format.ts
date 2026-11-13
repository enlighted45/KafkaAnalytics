const inrCompact = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', notation: 'compact', maximumFractionDigits: 1 });
const inrFull = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 });
const countFmt = new Intl.NumberFormat('en-IN');
const compactCount = new Intl.NumberFormat('en-IN', { notation: 'compact', maximumFractionDigits: 1 });

/** ₹ amount; compact (lakh / crore) above 1 lakh. */
export function formatInr(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '–';
  return Math.abs(value) >= 100_000 ? inrCompact.format(value) : inrFull.format(value);
}

export function formatCount(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return '–';
  return Math.abs(value) >= 100_000 ? compactCount.format(value) : countFmt.format(value);
}

export function formatPercent(ratio: number | null | undefined, digits = 1): string {
  if (ratio == null || Number.isNaN(ratio)) return '–';
  return `${(ratio * 100).toFixed(digits)}%`;
}

export function formatMs(ms: number | null | undefined): string {
  if (ms == null || ms < 0) return '–';
  return ms >= 1000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms)} ms`;
}

export function formatClock(epochMs: number): string {
  return new Date(epochMs).toLocaleTimeString('en-GB', { hour12: false });
}

export function formatAmount(amount: number, currency: string): string {
  try {
    return new Intl.NumberFormat('en-IN', { style: 'currency', currency, maximumFractionDigits: 2 }).format(amount);
  } catch {
    return `${amount.toFixed(2)} ${currency}`;
  }
}
