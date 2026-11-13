package com.bank.txn.producer.generator;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Generator settings. Initial values come from application.yml (prefix "generator") and can be changed
 * at runtime through the REST API.
 *
 * @param ratePerSecond        target events per second (excluding injected duplicates)
 * @param accounts             number of simulated accounts
 * @param lateRatio            share of events delivered late (held back 5s..maxLatenessSeconds)
 * @param maxLatenessSeconds   maximum delivery delay for late events
 * @param outOfOrderRatio      share of events emitted as a swapped pair (seq n+1 delivered before n)
 * @param duplicateRatio       share of events sent twice with the same eventId
 * @param highValueRatio       share of events with a very large amount (should raise alerts)
 * @param burstChancePerSecond probability per second that one account fires a burst (velocity alert)
 * @param burstSize            number of transactions in a burst
 */
@ConfigurationProperties("generator")
public record GeneratorSettings(
        @Min(0) @Max(5000) int ratePerSecond,
        @Min(1) @Max(1_000_000) int accounts,
        @DecimalMin("0") @DecimalMax("0.5") double lateRatio,
        @Min(6) @Max(600) int maxLatenessSeconds,
        @DecimalMin("0") @DecimalMax("0.5") double outOfOrderRatio,
        @DecimalMin("0") @DecimalMax("0.5") double duplicateRatio,
        @DecimalMin("0") @DecimalMax("0.5") double highValueRatio,
        @DecimalMin("0") @DecimalMax("1") double burstChancePerSecond,
        @Min(2) @Max(100) int burstSize) {

    public static final int MIN_LATENESS_SECONDS = 5;

    public static GeneratorSettings defaults() {
        return new GeneratorSettings(50, 5000, 0.03, 90, 0.02, 0.01, 0.005, 0.05, 12);
    }
}
