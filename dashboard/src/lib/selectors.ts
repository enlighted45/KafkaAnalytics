import type { Bucket, WindowPoint } from '../types';

export interface BreakdownRow {
  key: string;
  count: number;
  volume: number;
}

/** Sums one breakdown map over the complete windows of the last minute. */
export function lastMinuteBreakdown(windows: WindowPoint[], field: 'byType' | 'byChannel' | 'byRegion'): BreakdownRow[] {
  const complete = windows.filter((w) => w.complete);
  if (complete.length === 0) return [];
  const latestEnd = Math.max(...complete.map((w) => w.end));
  const totals = new Map<string, Bucket>();
  for (const w of complete) {
    if (w.start < latestEnd - 60_000) continue;
    for (const [key, b] of Object.entries(w.agg[field] ?? {})) {
      const t = totals.get(key) ?? { count: 0, volume: 0 };
      t.count += b.count;
      t.volume += b.volume;
      totals.set(key, t);
    }
  }
  return [...totals.entries()]
    .map(([key, b]) => ({ key, count: b.count, volume: b.volume }))
    .sort((a, b) => b.volume - a.volume);
}

export interface ChartPoint {
  start: number;
  label: string;
  approved: number;
  declined: number;
  volume: number;
  complete: boolean;
  lateAccepted: number;
}

export function toChartPoints(windows: WindowPoint[]): ChartPoint[] {
  return [...windows]
    .sort((a, b) => a.start - b.start)
    .map((w) => ({
      start: w.start,
      label: new Date(w.start).toLocaleTimeString('en-GB', { hour12: false }),
      approved: w.agg.approved,
      declined: w.agg.declined,
      volume: Number(w.agg.volume),
      complete: w.complete,
      lateAccepted: w.agg.lateAccepted,
    }));
}

/** TPS per complete window, for the sparkline. */
export function tpsSeries(windows: WindowPoint[], windowMs: number): number[] {
  return [...windows]
    .filter((w) => w.complete)
    .sort((a, b) => a.start - b.start)
    .map((w) => w.agg.count / (windowMs / 1000));
}
