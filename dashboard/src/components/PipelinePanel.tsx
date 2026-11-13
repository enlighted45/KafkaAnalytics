import { formatCount } from '../lib/format';
import type { PipelineTotals } from '../types';

const ITEMS: { key: keyof PipelineTotals; label: string; help: string }[] = [
  { key: 'received', label: 'Received', help: 'All records read from transactions.raw' },
  { key: 'processed', label: 'Counted', help: 'Unique and not too late: included in the metrics' },
  { key: 'duplicates', label: 'Duplicates', help: 'Same eventId seen again; ignored' },
  { key: 'lateAccepted', label: 'Late, corrected', help: 'Arrived after its window ended but within grace; window was updated' },
  { key: 'lateDropped', label: 'Too late', help: 'Arrived after window + grace; sent to transactions.late' },
  { key: 'outOfOrder', label: 'Out of order', help: 'Arrived after a later event of the same account' },
];

export function PipelinePanel({ totals }: { totals: PipelineTotals | undefined }) {
  return (
    <div className="card">
      <h2>Stream processing since start</h2>
      <p className="hint">How each event was handled by the Kafka Streams topology</p>
      <div className="pipeline">
        {ITEMS.map((i) => (
          <div key={i.key} className="tile" title={i.help}>
            <span className="label">{i.label}</span>
            <span className="value" style={{ fontSize: 20 }}>{formatCount(totals?.[i.key] as number | undefined)}</span>
            <span className="foot">{i.help}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
