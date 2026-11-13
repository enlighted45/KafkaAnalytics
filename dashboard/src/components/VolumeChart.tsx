import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { formatInr } from '../lib/format';
import type { ChartPoint } from '../lib/selectors';
import { TooltipBox } from './ChartTooltip';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload }: any) {
  if (!active || !payload?.length) return null;
  const p: ChartPoint = payload[0].payload;
  return <TooltipBox title={`Window starting ${p.label}${p.complete ? '' : ' (open)'}`}
    rows={[{ name: 'Approved volume', value: formatInr(p.volume) }]} />;
}

export function VolumeChart({ points, windowS }: { points: ChartPoint[]; windowS: number }) {
  return (
    <div className="card">
      <h2>Approved volume per {windowS} s (INR)</h2>
      <p className="hint">All currencies converted to INR at reference rates.</p>
      {points.length === 0 ? (
        <div className="empty">No data yet</div>
      ) : (
        <ResponsiveContainer width="100%" height={282}>
          <AreaChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid vertical={false} stroke="var(--grid)" />
            <XAxis dataKey="label" tick={{ fill: 'var(--text-muted)', fontSize: 11 }} tickLine={false}
              axisLine={{ stroke: 'var(--axis)' }} minTickGap={40} />
            <YAxis tick={{ fill: 'var(--text-muted)', fontSize: 11 }} tickLine={false} axisLine={false} width={64}
              tickFormatter={(v: number) => formatInr(v)} />
            <Tooltip content={<Tip />} cursor={{ stroke: 'var(--axis)', strokeWidth: 1 }} isAnimationActive={false} />
            <Area type="monotone" dataKey="volume" stroke="var(--series-1)" strokeWidth={2} fill="var(--series-1-wash)"
              fillOpacity={1} isAnimationActive={false}
              activeDot={{ r: 4, fill: 'var(--series-1)', stroke: 'var(--surface-1)', strokeWidth: 2 }} dot={false} />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
