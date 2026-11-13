package com.bank.txn.analytics.model;

/**
 * One window's partial result for one input partition, as published to metrics.windowed.
 * Consumers sum the partials of all partitions for the same (size, start) to get the global window.
 */
public record WindowMetrics(String size, long start, long end, int partition, WindowAgg agg) {
}
