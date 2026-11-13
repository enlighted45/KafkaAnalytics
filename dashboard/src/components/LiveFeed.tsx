import { formatAmount } from '../lib/format';
import type { TransactionEvent } from '../types';

export function LiveFeed({ transactions }: { transactions: TransactionEvent[] }) {
  return (
    <div className="card">
      <h2>Live transactions</h2>
      <p className="hint">Sampled stream (up to 20 per second), newest first</p>
      {transactions.length === 0 ? (
        <div className="empty">Waiting for transactions…</div>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Time</th><th>Account</th><th className="num">Seq</th><th>Type</th><th>Channel</th>
                <th className="num">Amount</th><th>Status</th><th>Region</th>
              </tr>
            </thead>
            <tbody>
              {transactions.map((t, i) => (
                <tr key={t.eventId + i} className={i === 0 ? 'flash' : undefined}>
                  <td>{new Date(t.eventTime).toLocaleTimeString('en-GB', { hour12: false })}</td>
                  <td>{t.accountId}</td>
                  <td className="num">{t.sequenceNo % 1_000_000}</td>
                  <td>{t.type}</td>
                  <td>{t.channel}</td>
                  <td className="num">{formatAmount(t.amount, t.currency)}</td>
                  <td>
                    <span className={`status-text ${t.status === 'APPROVED' ? 'good' : 'critical'}`}>
                      {t.status === 'APPROVED' ? '✓ Approved' : '✕ Declined'}
                    </span>
                  </td>
                  <td>{t.region}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
