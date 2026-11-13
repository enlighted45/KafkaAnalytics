import { useCallback, useEffect, useState } from 'react';
import { api } from '../lib/api';
import { formatCount } from '../lib/format';
import type { GeneratorSettings, GeneratorStatus, PipelineTotals } from '../types';

function delta(now: number | undefined, base: number | undefined): string {
  if (now == null) return '–';
  return formatCount(now - (base ?? 0));
}

type RatioKey = 'lateRatio' | 'outOfOrderRatio' | 'duplicateRatio' | 'highValueRatio';

const SLIDERS: { key: RatioKey; label: string; max: number }[] = [
  { key: 'lateRatio', label: 'Late events', max: 0.2 },
  { key: 'outOfOrderRatio', label: 'Out-of-order pairs', max: 0.2 },
  { key: 'duplicateRatio', label: 'Duplicates', max: 0.2 },
  { key: 'highValueRatio', label: 'High-value', max: 0.05 },
];

/** Controls the producer-service generator and compares what it injected with what the stream detected. */
export function GeneratorControls({ totals }: { totals: PipelineTotals | undefined }) {
  const [status, setStatus] = useState<GeneratorStatus | null>(null);
  const [draft, setDraft] = useState<GeneratorSettings | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // Counters on both sides start at different times (producer start vs stream start), so the
  // comparison is made from a common baseline: when the page opened, or when the user resets it.
  const [baseline, setBaseline] = useState<{ stats: GeneratorStatus['stats']; totals: PipelineTotals; at: number } | null>(null);

  useEffect(() => {
    if (!baseline && status && totals) setBaseline({ stats: status.stats, totals, at: Date.now() });
  }, [baseline, status, totals]);

  const refresh = useCallback(() => {
    api.generator()
      .then((s) => {
        setStatus(s);
        setDraft((d) => d ?? s.settings);
        setError(null);
      })
      .catch((e: Error) => setError(`Generator unavailable (${e.message})`));
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 2000);
    return () => clearInterval(id);
  }, [refresh]);

  const run = async (fn: () => Promise<GeneratorStatus>) => {
    setBusy(true);
    try {
      const s = await fn();
      setStatus(s);
      setDraft(s.settings);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const dirty = draft && status && JSON.stringify(draft) !== JSON.stringify(status.settings);

  return (
    <div className="card controls">
      <div>
        <h2>Event generator</h2>
        <p className="hint" style={{ marginBottom: 0 }}>Produces transactions into Kafka, with deliberate irregularities</p>
      </div>
      {error && <div className="status-text critical">{error}</div>}
      {status && draft && (
        <>
          <div className="row-actions">
            <span className="pill">
              <span className={`dot ${status.running ? 'good' : 'muted'}`} aria-hidden /> {status.running ? 'Running' : 'Stopped'}
            </span>
            {status.running
              ? <button className="btn" disabled={busy} onClick={() => run(api.stop)}>Stop</button>
              : <button className="btn primary" disabled={busy} onClick={() => run(api.start)}>Start</button>}
          </div>
          <div className="slider">
            <label htmlFor="rate">Rate</label>
            <output htmlFor="rate">{draft.ratePerSecond} / s</output>
            <input id="rate" type="range" min={0} max={2000} step={10} value={draft.ratePerSecond}
              onChange={(e) => setDraft({ ...draft, ratePerSecond: Number(e.target.value) })} />
          </div>
          {SLIDERS.map((s) => (
            <div className="slider" key={s.key}>
              <label htmlFor={s.key}>{s.label}</label>
              <output htmlFor={s.key}>{(draft[s.key] * 100).toFixed(1)}%</output>
              <input id={s.key} type="range" min={0} max={s.max} step={0.001} value={draft[s.key]}
                onChange={(e) => setDraft({ ...draft, [s.key]: Number(e.target.value) })} />
            </div>
          ))}
          <div className="row-actions">
            <button className="btn primary" disabled={!dirty || busy} onClick={() => draft && run(() => api.updateSettings(draft))}>
              Apply
            </button>
            <button className="btn" disabled={!dirty || busy} onClick={() => setDraft(status.settings)}>Reset</button>
          </div>
          <div className="truth" aria-label="Injected versus detected">
            <span className="h">Since {baseline ? new Date(baseline.at).toLocaleTimeString('en-GB', { hour12: false }) : '–'}</span>
            <span className="h num">Injected</span><span className="h num">Detected</span>
            <span>Duplicates</span>
            <span className="num" data-testid="dup-injected">{delta(status.stats.injectedDuplicates, baseline?.stats.injectedDuplicates)}</span>
            <span className="num" data-testid="dup-detected">{delta(totals?.duplicates, baseline?.totals.duplicates)}</span>
            <span>Late (delayed 5–{draft.maxLatenessSeconds} s)</span>
            <span className="num">{delta(status.stats.injectedLate, baseline?.stats.injectedLate)}</span>
            <span className="num">{totals ? delta(totals.lateAccepted + totals.lateDropped,
              baseline ? baseline.totals.lateAccepted + baseline.totals.lateDropped : undefined) : '–'}</span>
            <span>Out-of-order pairs</span>
            <span className="num">{delta(status.stats.injectedOutOfOrder, baseline?.stats.injectedOutOfOrder)}</span>
            <span className="num">{delta(totals?.outOfOrder, baseline?.totals.outOfOrder)}</span>
            <span>Sent / acked / errors</span>
            <span className="num" style={{ gridColumn: 'span 2' }}>
              {formatCount(status.stats.sent)} / {formatCount(status.stats.acked)} / {formatCount(status.stats.sendErrors)}
            </span>
          </div>
          <div className="row-actions">
            <button className="btn" onClick={() => status && totals && setBaseline({ stats: status.stats, totals, at: Date.now() })}>
              Reset comparison
            </button>
          </div>
          <p className="hint" style={{ margin: 0 }}>
            Events still held back or in flight make the columns differ briefly. Detected out-of-order also includes late
            events (they arrive after newer events of the same account); late events delayed less than the rest of
            their window are not late at all.
          </p>
        </>
      )}
    </div>
  );
}
