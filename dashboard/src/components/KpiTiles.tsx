import { formatCount, formatInr, formatPercent } from '../lib/format';
import { tpsSeries } from '../lib/selectors';
import type { DashboardUpdate } from '../types';
import { Sparkline } from './Sparkline';

interface Props {
  metrics: DashboardUpdate | null;
  alertCount: number;
}

function Tile({ label, value, foot, children }: { label: string; value: string; foot?: string; children?: React.ReactNode }) {
  return (
    <div className="card tile">
      <span className="label">{label}</span>
      <span className="value">{value}</span>
      {children}
      {foot && <span className="foot">{foot}</span>}
    </div>
  );
}

export function KpiTiles({ metrics, alertCount }: Props) {
  const k = metrics?.kpis;
  const t = metrics?.totals;
  const windowS = (metrics?.windowMs ?? 10_000) / 1000;
  return (
    <section className="grid kpis" aria-label="Key metrics">
      <Tile label="Throughput" value={k ? `${k.tps.toFixed(1)} tps` : '–'} foot={k ? `Peak ${k.peakTps.toFixed(1)} tps · last ${windowS} s window` : undefined}>
        <Sparkline values={metrics ? tpsSeries(metrics.windows, metrics.windowMs) : []} />
      </Tile>
      <Tile label="Approved volume, last minute" value={formatInr(k?.volumeLastMin)} foot={k ? `${formatCount(k.countLastMin)} transactions` : undefined} />
      <Tile label="Average ticket" value={formatInr(k?.avgTicket)} foot="Approved, last minute" />
      <Tile label="Approval rate" value={formatPercent(k?.approvalRate)} foot="Last minute" />
      <Tile label="Late events" value={t ? formatCount(t.lateAccepted + t.lateDropped) : '–'}
        foot={t ? `${formatCount(t.lateAccepted)} corrected · ${formatCount(t.lateDropped)} too late` : undefined} />
      <Tile label="Out of order" value={formatCount(t?.outOfOrder)} foot={t ? `${formatCount(t.gaps)} sequence gaps seen` : undefined} />
      <Tile label="Duplicates removed" value={formatCount(t?.duplicates)} foot={t ? `of ${formatCount(t.received)} received` : undefined} />
      <Tile label="Alerts" value={formatCount(alertCount)} foot="High value + velocity (recent)" />
    </section>
  );
}
