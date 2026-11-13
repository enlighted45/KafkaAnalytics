// Mirrors the analytics-service / producer-service JSON payloads.

export interface Bucket {
  count: number;
  volume: number;
}

export interface WindowAgg {
  count: number;
  approved: number;
  declined: number;
  volume: number;
  maxAmount: number;
  lateAccepted: number;
  outOfOrder: number;
  byType: Record<string, Bucket>;
  byChannel: Record<string, Bucket>;
  byRegion: Record<string, Bucket>;
  byCurrency: Record<string, number>;
}

export interface WindowPoint {
  start: number;
  end: number;
  complete: boolean;
  agg: WindowAgg;
}

export interface Kpis {
  tps: number;
  volumeLastMin: number;
  countLastMin: number;
  avgTicket: number;
  approvalRate: number;
  peakTps: number;
}

export interface PipelineTotals {
  received: number;
  processed: number;
  duplicates: number;
  outOfOrder: number;
  gaps: number;
  lateAccepted: number;
  lateDropped: number;
  maxEventTime: number;
}

export interface LatencyStats {
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  maxMs: number;
  samples: number;
}

export interface DashboardUpdate {
  generatedAt: number;
  streamsState: string;
  windowMs: number;
  graceMs: number;
  kpis: Kpis;
  windows: WindowPoint[];
  summaryWindows: WindowPoint[];
  totals: PipelineTotals;
  latency: LatencyStats;
}

export interface BrokerStatus {
  id: number;
  host: string;
  up: boolean;
  leaderPartitions: number;
  replicaPartitions: number;
}

export interface ClusterHealth {
  timestamp: number;
  reachable: boolean;
  brokers: BrokerStatus[];
  activeControllerId: number;
  controllerVoters: number[];
  totalPartitions: number;
  underReplicated: number;
  offlinePartitions: number;
  consumerLag: number;
  error: string | null;
}

export interface Alert {
  alertId: string;
  type: 'HIGH_VALUE' | 'VELOCITY';
  accountId: string;
  eventId: string | null;
  amountInr: number | null;
  count: number;
  eventTime: string;
  detectedAt: string;
  message: string;
}

export interface TransactionEvent {
  eventId: string;
  accountId: string;
  sequenceNo: number;
  type: 'DEPOSIT' | 'WITHDRAWAL' | 'TRANSFER' | 'PAYMENT';
  channel: string;
  status: 'APPROVED' | 'DECLINED';
  amount: number;
  currency: string;
  merchantCategory: string | null;
  region: string;
  eventTime: string;
  producedAt: string;
}

export interface Snapshot {
  metrics: DashboardUpdate;
  health: ClusterHealth;
  alerts: Alert[];
  transactions: TransactionEvent[];
}

export interface GeneratorSettings {
  ratePerSecond: number;
  accounts: number;
  lateRatio: number;
  maxLatenessSeconds: number;
  outOfOrderRatio: number;
  duplicateRatio: number;
  highValueRatio: number;
  burstChancePerSecond: number;
  burstSize: number;
}

export interface GeneratorStats {
  generated: number;
  sent: number;
  acked: number;
  sendErrors: number;
  pending: number;
  injectedLate: number;
  injectedOutOfOrder: number;
  injectedDuplicates: number;
  injectedHighValue: number;
  injectedBurstEvents: number;
}

export interface GeneratorStatus {
  running: boolean;
  settings: GeneratorSettings;
  stats: GeneratorStats;
}

export type ConnectionState = 'connecting' | 'connected' | 'reconnecting';
