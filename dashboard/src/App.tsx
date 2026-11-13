import { useEffect, useMemo, useState } from 'react';
import { AlertsPanel } from './components/AlertsPanel';
import { BreakdownChart } from './components/BreakdownChart';
import { GeneratorControls } from './components/GeneratorControls';
import { Header } from './components/Header';
import { HealthStrip } from './components/HealthStrip';
import { KpiTiles } from './components/KpiTiles';
import { LiveFeed } from './components/LiveFeed';
import { PipelinePanel } from './components/PipelinePanel';
import { VolumeChart } from './components/VolumeChart';
import { WindowCountChart } from './components/WindowCountChart';
import { lastMinuteBreakdown, toChartPoints } from './lib/selectors';
import { useDashboardStream } from './state/useDashboardStream';

type Theme = 'light' | 'dark';

function initialTheme(): Theme {
  try {
    const saved = localStorage.getItem('theme');
    if (saved === 'light' || saved === 'dark') return saved;
  } catch { /* storage unavailable */ }
  return window.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

export default function App() {
  const state = useDashboardStream();
  const [theme, setTheme] = useState<Theme>(initialTheme);
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    try { localStorage.setItem('theme', theme); } catch { /* ignore */ }
  }, [theme]);

  const m = state.metrics;
  const points = useMemo(() => toChartPoints(m?.windows ?? []), [m]);
  const byType = useMemo(() => lastMinuteBreakdown(m?.windows ?? [], 'byType'), [m]);
  const byChannel = useMemo(() => lastMinuteBreakdown(m?.windows ?? [], 'byChannel'), [m]);
  const byRegion = useMemo(() => lastMinuteBreakdown(m?.windows ?? [], 'byRegion'), [m]);
  const windowS = (m?.windowMs ?? 10_000) / 1000;
  const graceS = (m?.graceMs ?? 30_000) / 1000;

  return (
    <div className="app">
      <Header connection={state.connection} streamsState={m?.streamsState} theme={theme}
        onToggleTheme={() => setTheme(theme === 'dark' ? 'light' : 'dark')} />
      <HealthStrip health={state.health} latency={m?.latency} />
      <KpiTiles metrics={m} alertCount={state.alerts.length} />
      <section className="grid charts-main">
        <WindowCountChart points={points} windowS={windowS} graceS={graceS} />
        <VolumeChart points={points} windowS={windowS} />
      </section>
      <section className="grid charts-side">
        <BreakdownChart title="By transaction type" rows={byType} />
        <BreakdownChart title="By channel" rows={byChannel} />
        <BreakdownChart title="By region" rows={byRegion} />
      </section>
      <PipelinePanel totals={m?.totals} />
      <section className="grid bottom">
        <LiveFeed transactions={state.transactions} />
        <div className="grid" style={{ alignContent: 'start' }}>
          <AlertsPanel alerts={state.alerts} />
          <GeneratorControls totals={m?.totals} />
        </div>
      </section>
    </div>
  );
}
