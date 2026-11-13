import { describe, expect, it } from 'vitest';
import { formatCount, formatInr, formatMs, formatPercent } from './format';

describe('format', () => {
  it('formats INR with Indian grouping and compact lakh/crore for large values', () => {
    expect(formatInr(12345)).toBe('₹12,345');
    expect(formatInr(2_50_000)).toMatch(/^₹2\.5\s?L$/);
    expect(formatInr(3_40_00_000)).toMatch(/^₹3\.4\s?Cr$/);
    expect(formatInr(null)).toBe('–');
  });
  it('formats counts, percents and durations', () => {
    expect(formatCount(1234)).toBe('1,234');
    expect(formatPercent(0.9654)).toBe('96.5%');
    expect(formatMs(250)).toBe('250 ms');
    expect(formatMs(1500)).toBe('1.5 s');
    expect(formatMs(-1)).toBe('–');
  });
});
