package com.bank.txn.analytics.feed;

import java.util.function.LongSupplier;

/** Simple token bucket: at most {@code perSecond} acquisitions per second. */
public class RateLimiter {
    private final int perSecond;
    private final LongSupplier nanoClock;
    private double tokens;
    private long last;

    public RateLimiter(int perSecond, LongSupplier nanoClock) {
        this.perSecond = perSecond;
        this.nanoClock = nanoClock;
        this.tokens = perSecond;
        this.last = nanoClock.getAsLong();
    }

    public synchronized boolean tryAcquire() {
        long now = nanoClock.getAsLong();
        tokens = Math.min(perSecond, tokens + (now - last) / 1e9 * perSecond);
        last = now;
        if (tokens >= 1) {
            tokens -= 1;
            return true;
        }
        return false;
    }
}
