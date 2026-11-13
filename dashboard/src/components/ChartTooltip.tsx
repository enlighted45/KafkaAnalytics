interface Row {
  name: string;
  value: string;
  color?: string;
}

/** Shared tooltip body: text in ink tokens, identity carried by the swatch. */
export function TooltipBox({ title, rows, note }: { title: string; rows: Row[]; note?: string }) {
  return (
    <div className="tooltip">
      <div className="t">{title}</div>
      {rows.map((r) => (
        <div className="row" key={r.name}>
          <span>
            {r.color && <span className="swatch" style={{ background: r.color }} />}
            {r.name}
          </span>
          <strong>{r.value}</strong>
        </div>
      ))}
      {note && <div className="t" style={{ marginTop: 4, marginBottom: 0 }}>{note}</div>}
    </div>
  );
}
