package com.bank.txn.analytics.feed;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimiterTest {
    @Test
    void allowsAtMostConfiguredRatePerSecond() {
        AtomicLong nanos = new AtomicLong();
        RateLimiter limiter = new RateLimiter(20, nanos::get);
        int allowed = 0;
        for (int second = 0; second < 5; second++) {
            for (int i = 0; i < 1000; i++) {
                nanos.addAndGet(1_000_000); // 1000 attempts spread over 1 second
                if (limiter.tryAcquire()) allowed++;
            }
        }
        assertThat(allowed).isBetween(100, 121); // 20/s * 5s (+ initial burst of 20)
    }

    @Test
    void recentBufferKeepsNewestFirstAndIsBounded() {
        RecentBuffer<Integer> b = new RecentBuffer<>(3);
        for (int i = 1; i <= 5; i++) b.add(i);
        assertThat(b.snapshot()).containsExactly(5, 4, 3);
    }
}
