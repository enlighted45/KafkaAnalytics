import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { alert, health, metrics } from '../test/fixtures';
import type { GeneratorStatus } from '../types';
import { AlertsPanel } from './AlertsPanel';
import { GeneratorControls } from './GeneratorControls';
import { Header } from './Header';
import { HealthStrip } from './HealthStrip';
import { KpiTiles } from './KpiTiles';

afterEach(() => vi.restoreAllMocks());

describe('Header', () => {
  it('shows live / reconnecting WebSocket state', () => {
    const { rerender } = render(<Header connection="connected" streamsState="RUNNING" theme="light" onToggleTheme={() => {}} />);
    expect(screen.getByTestId('connection')).toHaveTextContent('Live');
    rerender(<Header connection="reconnecting" streamsState="RUNNING" theme="light" onToggleTheme={() => {}} />);
    expect(screen.getByTestId('connection')).toHaveTextContent('Reconnecting');
  });
});

describe('HealthStrip', () => {
  it('shows each broker, its partition leadership and the active controller', () => {
    render(<HealthStrip health={health()} latency={metrics().latency} />);
    expect(screen.getByTestId('broker-1')).toHaveTextContent('Leader for 6 of 18 partitions');
    expect(screen.getByTestId('broker-2')).toHaveTextContent('Active controller');
    expect(screen.getByTestId('broker-1')).not.toHaveTextContent('Active controller');
    expect(screen.getByText(/3 of 3 brokers up/)).toBeInTheDocument();
  });

  it('marks a down broker with text, not only colour', () => {
    const h = health({
      underReplicated: 12,
      brokers: [
        { id: 1, host: 'kafka-1:9092', up: true, leaderPartitions: 9, replicaPartitions: 18 },
        { id: 2, host: 'kafka-2', up: false, leaderPartitions: 0, replicaPartitions: 18 },
        { id: 3, host: 'kafka-3:9092', up: true, leaderPartitions: 9, replicaPartitions: 18 },
      ],
    });
    render(<HealthStrip health={h} latency={undefined} />);
    expect(screen.getByTestId('broker-2')).toHaveTextContent('Down');
    expect(screen.getByText(/2 of 3 brokers up/)).toBeInTheDocument();
  });

  it('shows a banner when the cluster is unreachable', () => {
    render(<HealthStrip health={health({ reachable: false, error: 'Timeout' })} latency={undefined} />);
    expect(screen.getByRole('alert')).toHaveTextContent('Kafka cluster unreachable');
  });
});

describe('KpiTiles', () => {
  it('renders headline metrics', () => {
    render(<KpiTiles metrics={metrics()} alertCount={3} />);
    expect(screen.getByText('52.3 tps')).toBeInTheDocument();
    expect(screen.getByText('96.5%')).toBeInTheDocument();
    expect(screen.getByText('40')).toBeInTheDocument();          // late = 12 + 28
    expect(screen.getByText('12 corrected · 28 too late')).toBeInTheDocument();
  });

  it('renders placeholders before data arrives', () => {
    render(<KpiTiles metrics={null} alertCount={0} />);
    expect(screen.getAllByText('–').length).toBeGreaterThan(3);
  });
});

describe('AlertsPanel', () => {
  it('labels alert types in text', () => {
    render(<AlertsPanel alerts={[alert('1', 'HIGH_VALUE'), alert('2', 'VELOCITY')]} />);
    expect(screen.getByText(/High value$/)).toBeInTheDocument();
    expect(screen.getByText(/Velocity$/)).toBeInTheDocument();
  });
});

describe('GeneratorControls', () => {
  const status: GeneratorStatus = {
    running: true,
    settings: { ratePerSecond: 50, accounts: 5000, lateRatio: 0.03, maxLatenessSeconds: 90, outOfOrderRatio: 0.02,
      duplicateRatio: 0.01, highValueRatio: 0.005, burstChancePerSecond: 0.05, burstSize: 12 },
    stats: { generated: 100, sent: 101, acked: 101, sendErrors: 0, pending: 3, injectedLate: 4, injectedOutOfOrder: 2,
      injectedDuplicates: 1, injectedHighValue: 0, injectedBurstEvents: 0 },
  };

  it('shows status and stops the generator', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async (url, init) => {
      const body = String(url).endsWith('/stop') ? { ...status, running: false } : status;
      void init;
      return new Response(JSON.stringify(body), { status: 200 });
    });
    render(<GeneratorControls totals={metrics().totals} />);
    await screen.findByText('Running');
    await userEvent.click(screen.getByRole('button', { name: 'Stop' }));
    await screen.findByText('Stopped');
    expect(fetchMock).toHaveBeenCalledWith('/api/generator/stop', { method: 'POST' });
  });

  it('enables Apply only after a change and sends the new settings', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify(status)));
    fetchMock.mockImplementation(async () => new Response(JSON.stringify(status)));
    render(<GeneratorControls totals={undefined} />);
    await screen.findByText('Running');
    const apply = screen.getByRole('button', { name: 'Apply' });
    expect(apply).toBeDisabled();
    const rate = screen.getByLabelText('Rate') as HTMLInputElement;
    // range inputs: set value via fireEvent-like change
    const { fireEvent } = await import('@testing-library/react');
    fireEvent.change(rate, { target: { value: '500' } });
    expect(apply).toBeEnabled();
    await userEvent.click(apply);
    await waitFor(() => {
      const put = fetchMock.mock.calls.find(([u]) => String(u) === '/api/generator/settings');
      expect(put).toBeDefined();
      expect(JSON.parse(String((put![1] as RequestInit).body)).ratePerSecond).toBe(500);
    });
  });

  it('compares injected and detected counts from a common baseline', async () => {
    let calls = 0;
    vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
      calls++;
      const later = { ...status, stats: { ...status.stats, injectedDuplicates: status.stats.injectedDuplicates + (calls > 1 ? 5 : 0) } };
      return new Response(JSON.stringify(later));
    });
    const totals = metrics().totals;
    const { rerender } = render(<GeneratorControls totals={totals} />);
    await screen.findByText('Running');
    // first frame shows raw counters; the baseline is applied in an effect right after
    await waitFor(() => expect(screen.getByTestId('dup-injected')).toHaveTextContent(/^0$/));
    expect(screen.getByTestId('dup-detected')).toHaveTextContent(/^0$/);
    rerender(<GeneratorControls totals={{ ...totals, duplicates: totals.duplicates + 5 }} />);
    await waitFor(() => expect(screen.getByTestId('dup-detected')).toHaveTextContent(/^5$/), { timeout: 3000 });
    await waitFor(() => expect(screen.getByTestId('dup-injected')).toHaveTextContent(/^5$/), { timeout: 3000 });
  });

  it('shows an error when the producer service is down', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('x', { status: 502, statusText: 'Bad Gateway' }));
    render(<GeneratorControls totals={undefined} />);
    expect(await screen.findByText(/Generator unavailable/)).toBeInTheDocument();
  });
});
