import { Bar, BarChart, LabelList, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { formatCount, formatInr } from '../lib/format';
import type { BreakdownRow } from '../lib/selectors';
import { TooltipBox } from './ChartTooltip';

const pretty = (k: string) => k.charAt(0) + k.slice(1).toLowerCase();

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function Tip({ active, payload }: any) {
  if (!active || !payload?.length) return null;
  const r: BreakdownRow = payload[0].payload;
  return <TooltipBox title={pretty(r.key)} rows={[
    { name: 'Approved volume', value: formatInr(r.volume) },
    { name: 'Transactions', value: formatCount(r.count) },
  ]} />;
}

/** Horizontal bars, single series: approved volume per category over the last minute. */
export function BreakdownChart({ title, rows }: { title: string; rows: BreakdownRow[] }) {
  const data = rows.map((r) => ({ ...r, name: pretty(r.key) }));
  return (
    <div className="card">
      <h2>{title}</h2>
      <p className="hint">Approved volume, last minute</p>
      {data.length === 0 ? (
        <div className="empty">No data yet</div>
      ) : (
        <ResponsiveContainer width="100%" height={Math.max(120, data.length * 34 + 10)}>
          <BarChart data={data} layout="vertical" margin={{ top: 0, right: 64, bottom: 0, left: 0 }} barCategoryGap={6}>
            <XAxis type="number" hide />
            <YAxis type="category" dataKey="name" width={84} tick={{ fill: 'var(--text-secondary)', fontSize: 12 }}
              tickLine={false} axisLine={{ stroke: 'var(--axis)' }} />
            <Tooltip content={<Tip />} cursor={{ fill: 'var(--series-1-wash)' }} isAnimationActive={false} />
            <Bar dataKey="volume" fill="var(--series-1)" radius={[0, 4, 4, 0]} maxBarSize={18} isAnimationActive={false}>
              <LabelList dataKey="volume" position="right" formatter={(v: unknown) => formatInr(Number(v))}
                style={{ fill: 'var(--text-secondary)', fontSize: 12 }} />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
