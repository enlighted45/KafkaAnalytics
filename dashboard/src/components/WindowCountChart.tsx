import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { formatCount } from '../lib/format';
import type { ChartPoint } from '../lib/selectors';
import { TooltipBox } from './ChartTooltip';

interface Props {
  points: ChartPoint[];
  windowS: number;
  graceS: number;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload }: any) {
  if (!active || !payload?.length) return null;
  const p: ChartPoint = payload[0].payload;
  const note = !p.complete
    ? 'Window still open'
    : p.lateAccepted > 0
      ? `${p.lateAccepted} late event(s) added after the window ended`
      : undefined;
  return (
    <TooltipBox
      title={`Window starting ${p.label}`}
      rows={[
        { name: 'Approved', value: formatCount(p.approved), color: 'var(--series-1)' },
        { name: 'Declined', value: formatCount(p.declined), color: 'var(--series-2)' },
        { name: 'Total', value: formatCount(p.approved + p.declined) },
      ]}
      note={note}
    />
  );
}

/** Transactions per event-time window, stacked approved / declined. The open window is drawn lighter. */
export function WindowCountChart({ points, windowS, graceS }: Props) {
  return (
    <div className="card">
      <h2>Transactions per {windowS} s window</h2>
      <p className="hint">
        Grouped by when each transaction happened (event time), last 5 minutes. A window still accepts late events for {graceS} s after it ends.
      </p>
      <div className="legend">
        <span><i className="swatch" style={{ background: 'var(--series-1)' }} /> Approved</span>
        <span><i className="swatch" style={{ background: 'var(--series-2)' }} /> Declined</span>
        <span><i className="swatch" style={{ background: 'var(--series-1)', opacity: 0.4 }} /> Open window</span>
      </div>
      {points.length === 0 ? (
        <div className="empty">No data yet</div>
      ) : (
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: 0 }} barCategoryGap={2}>
            <CartesianGrid vertical={false} stroke="var(--grid)" />
            <XAxis dataKey="label" tick={{ fill: 'var(--text-muted)', fontSize: 11 }} tickLine={false}
              axisLine={{ stroke: 'var(--axis)' }} minTickGap={40} />
            <YAxis tick={{ fill: 'var(--text-muted)', fontSize: 11 }} tickLine={false} axisLine={false} width={44}
              tickFormatter={(v: number) => formatCount(v)} allowDecimals={false} />
            <Tooltip content={<Tip />} cursor={{ fill: 'var(--series-1-wash)' }} isAnimationActive={false} />
            <Bar dataKey="approved" stackId="w" fill="var(--series-1)" maxBarSize={24} isAnimationActive={false}
              stroke="var(--surface-1)" strokeWidth={1}>
              {points.map((p) => <Cell key={p.start} fillOpacity={p.complete ? 1 : 0.4} />)}
            </Bar>
            <Bar dataKey="declined" stackId="w" fill="var(--series-2)" maxBarSize={24} radius={[4, 4, 0, 0]}
              isAnimationActive={false} stroke="var(--surface-1)" strokeWidth={1}>
              {points.map((p) => <Cell key={p.start} fillOpacity={p.complete ? 1 : 0.4} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
