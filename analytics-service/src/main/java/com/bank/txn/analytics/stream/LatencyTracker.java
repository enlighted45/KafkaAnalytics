package com.bank.txn.analytics.stream;

import java.time.Clock;
import java.util.Arrays;

/** Keeps the most recent pipeline latencies (producedAt -> processed) and reports percentiles. */
public class LatencyTracker {
    public record Stats(long p50Ms, long p95Ms, long p99Ms, long maxMs, int samples) {
        public static final Stats EMPTY = new Stats(0, 0, 0, 0, 0);
    }

    private final long[] ring;
    private final Clock clock;
    private int next;
    private int size;

    public LatencyTracker(int capacity, Clock clock) {
        this.ring = new long[capacity];
        this.clock = clock;
    }

    public void recordSince(long producedAtMs) {
        record(Math.max(0, clock.millis() - producedAtMs));
    }

    public synchronized void record(long latencyMs) {
        ring[next] = latencyMs;
        next = (next + 1) % ring.length;
        size = Math.min(size + 1, ring.length);
    }

    public synchronized Stats snapshot() {
        if (size == 0) {
            return Stats.EMPTY;
        }
        long[] copy = Arrays.copyOf(ring, size);
        Arrays.sort(copy);
        return new Stats(pct(copy, 50), pct(copy, 95), pct(copy, 99), copy[size - 1], size);
    }

    private static long pct(long[] sorted, int p) {
        int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }
}
