import type { ConnectionState } from '../types';

const CONNECTION: Record<ConnectionState, { cls: string; label: string }> = {
  connected: { cls: 'good', label: 'Live' },
  connecting: { cls: 'warning', label: 'Connecting…' },
  reconnecting: { cls: 'critical', label: 'Reconnecting…' },
};

interface Props {
  connection: ConnectionState;
  streamsState: string | undefined;
  theme: 'light' | 'dark';
  onToggleTheme: () => void;
}

export function Header({ connection, streamsState, theme, onToggleTheme }: Props) {
  const c = CONNECTION[connection];
  const streamsOk = streamsState === 'RUNNING';
  return (
    <header className="header">
      <div>
        <h1>Banking transaction analytics</h1>
        <div className="sub">Kafka · Kafka Streams · near-real-time metrics</div>
      </div>
      <div className="header-right">
        <span className="pill" data-testid="connection" title="WebSocket connection">
          <span className={`dot ${c.cls}`} aria-hidden /> {c.label}
        </span>
        <span className="pill" title="Kafka Streams state">
          <span className={`dot ${streamsOk ? 'good' : 'warning'}`} aria-hidden /> Streams: {streamsState ?? '–'}
        </span>
        <button className="btn" onClick={onToggleTheme} aria-label="Toggle colour theme">
          {theme === 'dark' ? 'Light' : 'Dark'} theme
        </button>
      </div>
    </header>
  );
}
