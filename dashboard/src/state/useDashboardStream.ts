import { Client } from '@stomp/stompjs';
import { useEffect, useReducer } from 'react';
import { api, webSocketUrl } from '../lib/api';
import { dashboardReducer, initialState } from './dashboardReducer';

/**
 * Loads the REST snapshot once, then keeps the dashboard live over STOMP/WebSocket.
 * Reconnects automatically (2 s back-off) and re-fetches the snapshot after each reconnect.
 */
export function useDashboardStream() {
  const [state, dispatch] = useReducer(dashboardReducer, initialState);

  useEffect(() => {
    let cancelled = false;
    const loadSnapshot = () =>
      api
        .snapshot()
        .then((snapshot) => !cancelled && dispatch({ type: 'snapshot', snapshot }))
        .catch(() => undefined);

    const client = new Client({
      brokerURL: webSocketUrl(),
      reconnectDelay: 2000,
      heartbeatIncoming: 10000,
      heartbeatOutgoing: 10000,
      onConnect: () => {
        dispatch({ type: 'connection', state: 'connected' });
        loadSnapshot();
        client.subscribe('/topic/metrics', (m) => dispatch({ type: 'metrics', metrics: JSON.parse(m.body) }));
        client.subscribe('/topic/health', (m) => dispatch({ type: 'health', health: JSON.parse(m.body) }));
        client.subscribe('/topic/alerts', (m) => dispatch({ type: 'alert', alert: JSON.parse(m.body) }));
        client.subscribe('/topic/transactions', (m) => dispatch({ type: 'transaction', transaction: JSON.parse(m.body) }));
      },
      onWebSocketClose: () => !cancelled && dispatch({ type: 'connection', state: 'reconnecting' }),
    });
    client.activate();
    return () => {
      cancelled = true;
      void client.deactivate();
    };
  }, []);

  return state;
}
