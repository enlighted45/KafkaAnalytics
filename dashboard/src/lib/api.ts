import type { GeneratorSettings, GeneratorStatus, Snapshot } from '../types';

async function json<T>(res: Response): Promise<T> {
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export const api = {
  snapshot: () => fetch('/api/dashboard/snapshot').then((r) => json<Snapshot>(r)),
  generator: () => fetch('/api/generator').then((r) => json<GeneratorStatus>(r)),
  start: () => fetch('/api/generator/start', { method: 'POST' }).then((r) => json<GeneratorStatus>(r)),
  stop: () => fetch('/api/generator/stop', { method: 'POST' }).then((r) => json<GeneratorStatus>(r)),
  updateSettings: (s: GeneratorSettings) =>
    fetch('/api/generator/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(s),
    }).then((r) => json<GeneratorStatus>(r)),
};

export function webSocketUrl(): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${proto}//${window.location.host}/ws`;
}
