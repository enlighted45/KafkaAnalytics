package com.bank.txn.analytics.stream;

import org.junit.jupiter.api.Test;

import java.time.Clock;

import static org.assertj.core.api.Assertions.assertThat;

class LatencyTrackerTest {
    @Test
    void reportsPercentilesOfRecentSamples() {
        LatencyTracker t = new LatencyTracker(1000, Clock.systemUTC());
        for (int i = 1; i <= 100; i++) {
            t.record(i);
        }
        LatencyTracker.Stats s = t.snapshot();
        assertThat(s.p50Ms()).isEqualTo(50);
        assertThat(s.p95Ms()).isEqualTo(95);
        assertThat(s.p99Ms()).isEqualTo(99);
        assertThat(s.maxMs()).isEqualTo(100);
        assertThat(s.samples()).isEqualTo(100);
    }

    @Test
    void ringBufferKeepsOnlyRecentSamples() {
        LatencyTracker t = new LatencyTracker(10, Clock.systemUTC());
        for (int i = 0; i < 100; i++) {
            t.record(1000);
        }
        for (int i = 0; i < 10; i++) {
            t.record(5);
        }
        assertThat(t.snapshot().maxMs()).isEqualTo(5);
    }

    @Test
    void emptyTrackerReturnsZeros() {
        assertThat(new LatencyTracker(10, Clock.systemUTC()).snapshot()).isEqualTo(LatencyTracker.Stats.EMPTY);
    }
}
