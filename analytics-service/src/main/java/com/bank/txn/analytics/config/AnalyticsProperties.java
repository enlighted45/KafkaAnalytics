package com.bank.txn.analytics.config;

import com.bank.txn.analytics.stream.TransactionTopology;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * @param windowSize            live window (default 10s)
 * @param summaryWindowSize     summary window (default 1m)
 * @param grace                 how long a window accepts late events after it ends
 * @param highValueThresholdInr amount (INR) that raises a HIGH_VALUE alert
 * @param velocityThreshold     more than this many transactions per account per velocityWindow raises VELOCITY
 * @param liveFeedMaxPerSecond  max sampled transactions pushed to the browser per second
 */
@ConfigurationProperties("analytics")
public record AnalyticsProperties(Duration windowSize, Duration summaryWindowSize, Duration grace,
                                  BigDecimal highValueThresholdInr, long velocityThreshold, Duration velocityWindow,
                                  Duration dedupeRetention, int liveFeedMaxPerSecond, Duration chartHistory,
                                  Duration summaryHistory) {

    public TransactionTopology.Config topologyConfig() {
        return new TransactionTopology.Config(windowSize, summaryWindowSize, grace, highValueThresholdInr,
                velocityThreshold, velocityWindow, dedupeRetention);
    }
}
