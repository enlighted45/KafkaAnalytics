import type { Alert, ClusterHealth, DashboardUpdate, TransactionEvent, WindowAgg, WindowPoint } from '../types';

export function agg(partial: Partial<WindowAgg> = {}): WindowAgg {
  return {
    count: 0, approved: 0, declined: 0, volume: 0, maxAmount: 0, lateAccepted: 0, outOfOrder: 0,
    byType: {}, byChannel: {}, byRegion: {}, byCurrency: {}, ...partial,
  };
}

export function win(start: number, partial: Partial<WindowAgg> = {}, complete = true): WindowPoint {
  return { start, end: start + 10_000, complete, agg: agg(partial) };
}

export function metrics(partial: Partial<DashboardUpdate> = {}): DashboardUpdate {
  return {
    generatedAt: 1000, streamsState: 'RUNNING', windowMs: 10_000, graceMs: 30_000,
    kpis: { tps: 52.3, volumeLastMin: 1_234_567, countLastMin: 3100, avgTicket: 4321, approvalRate: 0.965, peakTps: 60.1 },
    windows: [], summaryWindows: [],
    totals: { received: 1000, processed: 950, duplicates: 10, outOfOrder: 25, gaps: 30, lateAccepted: 12, lateDropped: 28, maxEventTime: 0 },
    latency: { p50Ms: 120, p95Ms: 480, p99Ms: 900, maxMs: 1500, samples: 4096 },
    ...partial,
  };
}

export function health(partial: Partial<ClusterHealth> = {}): ClusterHealth {
  return {
    timestamp: 1, reachable: true, activeControllerId: 2, controllerVoters: [1, 2, 3], totalPartitions: 18,
    underReplicated: 0, offlinePartitions: 0, consumerLag: 42, error: null,
    brokers: [
      { id: 1, host: 'kafka-1:9092', up: true, leaderPartitions: 6, replicaPartitions: 18 },
      { id: 2, host: 'kafka-2:9092', up: true, leaderPartitions: 6, replicaPartitions: 18 },
      { id: 3, host: 'kafka-3:9092', up: true, leaderPartitions: 6, replicaPartitions: 18 },
    ],
    ...partial,
  };
}

export function alert(id: string, type: Alert['type'] = 'HIGH_VALUE'): Alert {
  return {
    alertId: id, type, accountId: 'ACC-000001', eventId: 'e', amountInr: 250000, count: 1,
    eventTime: '2026-11-12T10:00:00Z', detectedAt: '2026-11-12T10:00:01Z', message: 'High-value TRANSFER',
  };
}

export function txn(id: string): TransactionEvent {
  return {
    eventId: id, accountId: 'ACC-000001', sequenceNo: 1, type: 'PAYMENT', channel: 'POS', status: 'APPROVED',
    amount: 100, currency: 'INR', merchantCategory: 'GROCERY', region: 'NORTH',
    eventTime: '2026-11-12T10:00:00Z', producedAt: '2026-11-12T10:00:00Z',
  };
}
