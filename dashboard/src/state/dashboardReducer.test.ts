import { describe, expect, it } from 'vitest';
import { alert, health, metrics, txn } from '../test/fixtures';
import { dashboardReducer, initialState, MAX_ALERTS, MAX_TRANSACTIONS } from './dashboardReducer';

describe('dashboardReducer', () => {
  it('applies the REST snapshot', () => {
    const s = dashboardReducer(initialState, {
      type: 'snapshot',
      snapshot: { metrics: metrics(), health: health(), alerts: [alert('a')], transactions: [txn('t')] },
    });
    expect(s.metrics?.kpis.tps).toBe(52.3);
    expect(s.health?.brokers).toHaveLength(3);
    expect(s.alerts).toHaveLength(1);
    expect(s.transactions).toHaveLength(1);
  });

  it('does not let an older snapshot overwrite newer live metrics', () => {
    let s = dashboardReducer(initialState, { type: 'metrics', metrics: metrics({ generatedAt: 5000 }) });
    s = dashboardReducer(s, {
      type: 'snapshot',
      snapshot: { metrics: metrics({ generatedAt: 1000 }), health: health(), alerts: [], transactions: [] },
    });
    expect(s.metrics?.generatedAt).toBe(5000);
  });

  it('ignores metrics pushes that arrive out of order', () => {
    let s = dashboardReducer(initialState, { type: 'metrics', metrics: metrics({ generatedAt: 2000 }) });
    s = dashboardReducer(s, { type: 'metrics', metrics: metrics({ generatedAt: 1000 }) });
    expect(s.metrics?.generatedAt).toBe(2000);
  });

  it('keeps alerts newest-first, de-duplicated and bounded', () => {
    let s = initialState;
    for (let i = 0; i < MAX_ALERTS + 10; i++) s = dashboardReducer(s, { type: 'alert', alert: alert(`a${i}`) });
    s = dashboardReducer(s, { type: 'alert', alert: alert(`a${MAX_ALERTS + 9}`) });
    expect(s.alerts).toHaveLength(MAX_ALERTS);
    expect(s.alerts[0].alertId).toBe(`a${MAX_ALERTS + 9}`);
    expect(new Set(s.alerts.map((a) => a.alertId)).size).toBe(MAX_ALERTS);
  });

  it('bounds the live transaction feed', () => {
    let s = initialState;
    for (let i = 0; i < MAX_TRANSACTIONS + 5; i++) s = dashboardReducer(s, { type: 'transaction', transaction: txn(`t${i}`) });
    expect(s.transactions).toHaveLength(MAX_TRANSACTIONS);
    expect(s.transactions[0].eventId).toBe(`t${MAX_TRANSACTIONS + 4}`);
  });

  it('tracks connection state', () => {
    const s = dashboardReducer(initialState, { type: 'connection', state: 'reconnecting' });
    expect(s.connection).toBe('reconnecting');
  });
});
