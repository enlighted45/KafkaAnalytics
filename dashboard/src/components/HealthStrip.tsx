import { formatCount, formatMs } from '../lib/format';
import type { ClusterHealth, LatencyStats } from '../types';

interface Props {
  health: ClusterHealth | null;
  latency: LatencyStats | undefined;
}

export function HealthStrip({ health, latency }: Props) {
  if (!health) {
    return <div className="card empty">Waiting for cluster health…</div>;
  }
  if (!health.reachable) {
    return (
      <div className="banner" role="alert">
        <span className="dot critical" aria-hidden /> <strong>Kafka cluster unreachable</strong>
        <span style={{ color: 'var(--text-secondary)' }}>{health.error}</span>
      </div>
    );
  }
  const up = health.brokers.filter((b) => b.up).length;
  return (
    <section className="health" aria-label="Kafka cluster health">
      {health.brokers.map((b) => {
        const share = health.totalPartitions ? b.leaderPartitions / health.totalPartitions : 0;
        return (
          <div className="card broker" key={b.id} data-testid={`broker-${b.id}`}>
            <div className="name">
              <span className={`dot ${b.up ? 'good' : 'critical'}`} aria-hidden />
              kafka-{b.id}
              <span className={`status-text ${b.up ? 'good' : 'critical'}`} style={{ fontWeight: 500, fontSize: 12 }}>
                {b.up ? '✓ Up' : '✕ Down'}
              </span>
              {health.activeControllerId === b.id && <span className="badge">Active controller</span>}
            </div>
            <div className="meta">
              Leader for {b.leaderPartitions} of {health.totalPartitions} partitions · replica of {b.replicaPartitions}
            </div>
            <div className="meter" aria-hidden>
              <span style={{ width: `${Math.round(share * 100)}%` }} />
            </div>
          </div>
        );
      })}
      <div className="card broker">
        <div className="name">
          <span className={`dot ${health.underReplicated || health.offlinePartitions ? 'warning' : 'good'}`} aria-hidden />
          Replication
        </div>
        <div className="meta">
          {up} of {health.brokers.length} brokers up · RF 3 (1 leader + 2 followers)
        </div>
        <div className="meta">
          Under-replicated: <strong>{health.underReplicated}</strong> · Offline: <strong>{health.offlinePartitions}</strong>
        </div>
      </div>
      <div className="card broker">
        <div className="name">Pipeline</div>
        <div className="meta">
          Consumer lag: <strong>{health.consumerLag < 0 ? '–' : formatCount(health.consumerLag)}</strong>
        </div>
        <div className="meta">
          Latency p50 <strong>{formatMs(latency?.p50Ms)}</strong> · p95 <strong>{formatMs(latency?.p95Ms)}</strong>
        </div>
      </div>
    </section>
  );
}
