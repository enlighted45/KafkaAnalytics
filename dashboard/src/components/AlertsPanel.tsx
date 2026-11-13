import type { Alert } from '../types';

export function AlertsPanel({ alerts }: { alerts: Alert[] }) {
  return (
    <div className="card">
      <h2>Alerts</h2>
      <p className="hint">High value ≥ ₹1,00,000 · velocity &gt; 10 transactions per account per minute</p>
      {alerts.length === 0 ? (
        <div className="empty">No alerts</div>
      ) : (
        <ul className="alerts">
          {alerts.map((a) => (
            <li key={a.alertId}>
              <span className={`badge`} style={{ borderColor: a.type === 'HIGH_VALUE' ? 'var(--critical)' : 'var(--serious)' }}>
                {a.type === 'HIGH_VALUE' ? '⚠ High value' : '⚡ Velocity'}
              </span>
              <span className="msg"><strong>{a.accountId}</strong> — {a.message}</span>
              <span className="when">{new Date(a.detectedAt).toLocaleTimeString('en-GB', { hour12: false })}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
