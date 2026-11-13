interface Props {
  values: number[];
  width?: number;
  height?: number;
}

/** Tiny trend line: history in muted ink, the current point in the accent. */
export function Sparkline({ values, width = 120, height = 28 }: Props) {
  if (values.length < 2) return <svg width={width} height={height} aria-hidden />;
  const max = Math.max(...values, 1);
  const step = width / (values.length - 1);
  const pts = values.map((v, i) => [i * step, height - 3 - (v / max) * (height - 6)] as const);
  const d = pts.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
  const [lx, ly] = pts[pts.length - 1];
  return (
    <svg width={width} height={height} aria-hidden style={{ overflow: 'visible' }}>
      <path d={d} fill="none" stroke="var(--axis)" strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={lx} cy={ly} r={3} fill="var(--series-1)" stroke="var(--surface-1)" strokeWidth={2} />
    </svg>
  );
}
