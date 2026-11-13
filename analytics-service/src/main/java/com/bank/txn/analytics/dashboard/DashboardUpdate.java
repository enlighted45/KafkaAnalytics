package com.bank.txn.analytics.dashboard;

import com.bank.txn.analytics.model.PipelineTotals;
import com.bank.txn.analytics.stream.LatencyTracker;

import java.util.List;

/** Everything the dashboard needs for metrics, pushed once per second on /topic/metrics. */
public record DashboardUpdate(long generatedAt, String streamsState, long windowMs, long graceMs, Kpis kpis,
                              List<WindowPoint> windows, List<WindowPoint> summaryWindows,
                              PipelineTotals totals, LatencyTracker.Stats latency) {
}
