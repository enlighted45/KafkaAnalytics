import { describe, expect, it } from 'vitest';
import { win } from '../test/fixtures';
import { lastMinuteBreakdown, toChartPoints, tpsSeries } from './selectors';

describe('selectors', () => {
  it('sums breakdowns over the last minute of complete windows only', () => {
    const windows = [
      win(0, { byType: { PAYMENT: { count: 100, volume: 1000 } } }),                // older than a minute -> excluded
      ...Array.from({ length: 6 }, (_, i) => win(10_000 * (i + 1), { byType: { PAYMENT: { count: 1, volume: 10 }, DEPOSIT: { count: 2, volume: 50 } } })),
      win(70_000, { byType: { PAYMENT: { count: 99, volume: 9999 } } }, false),      // open -> excluded
    ];
    const rows = lastMinuteBreakdown(windows, 'byType');
    expect(rows).toEqual([
      { key: 'DEPOSIT', count: 12, volume: 300 },
      { key: 'PAYMENT', count: 6, volume: 60 },
    ]);
  });

  it('sorts chart points by window start', () => {
    const pts = toChartPoints([win(20_000, { approved: 2 }), win(10_000, { approved: 1 })]);
    expect(pts.map((p) => p.approved)).toEqual([1, 2]);
  });

  it('computes tps per complete window', () => {
    expect(tpsSeries([win(0, { count: 100 }), win(10_000, { count: 50 }, false)], 10_000)).toEqual([10]);
  });
});
