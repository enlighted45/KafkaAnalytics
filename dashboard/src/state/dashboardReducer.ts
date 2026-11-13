import type { Alert, ClusterHealth, ConnectionState, DashboardUpdate, Snapshot, TransactionEvent } from '../types';

export const MAX_ALERTS = 50;
export const MAX_TRANSACTIONS = 30;

export interface DashboardState {
  connection: ConnectionState;
  metrics: DashboardUpdate | null;
  health: ClusterHealth | null;
  alerts: Alert[];
  transactions: TransactionEvent[];
  lastMessageAt: number | null;
}

export const initialState: DashboardState = {
  connection: 'connecting',
  metrics: null,
  health: null,
  alerts: [],
  transactions: [],
  lastMessageAt: null,
};

export type Action =
  | { type: 'snapshot'; snapshot: Snapshot }
  | { type: 'metrics'; metrics: DashboardUpdate }
  | { type: 'health'; health: ClusterHealth }
  | { type: 'alert'; alert: Alert }
  | { type: 'transaction'; transaction: TransactionEvent }
  | { type: 'connection'; state: ConnectionState };

function mergeAlerts(incoming: Alert[], existing: Alert[]): Alert[] {
  const seen = new Set<string>();
  const out: Alert[] = [];
  for (const a of [...incoming, ...existing]) {
    if (!seen.has(a.alertId)) {
      seen.add(a.alertId);
      out.push(a);
    }
  }
  return out.slice(0, MAX_ALERTS);
}

export function dashboardReducer(state: DashboardState, action: Action): DashboardState {
  switch (action.type) {
    case 'snapshot': {
      const { snapshot } = action;
      return {
        ...state,
        // a snapshot never overwrites newer live data that already arrived over the socket
        metrics: state.metrics && state.metrics.generatedAt > snapshot.metrics.generatedAt ? state.metrics : snapshot.metrics,
        health: state.health ?? snapshot.health,
        alerts: mergeAlerts(state.alerts, snapshot.alerts),
        transactions: state.transactions.length ? state.transactions : snapshot.transactions.slice(0, MAX_TRANSACTIONS),
      };
    }
    case 'metrics':
      if (state.metrics && action.metrics.generatedAt < state.metrics.generatedAt) {
        return state; // out-of-order delivery of pushes: keep the newest
      }
      return { ...state, metrics: action.metrics, lastMessageAt: Date.now() };
    case 'health':
      return { ...state, health: action.health };
    case 'alert':
      return { ...state, alerts: mergeAlerts([action.alert], state.alerts) };
    case 'transaction':
      return { ...state, transactions: [action.transaction, ...state.transactions].slice(0, MAX_TRANSACTIONS) };
    case 'connection':
      return { ...state, connection: action.state };
    default:
      return state;
  }
}
