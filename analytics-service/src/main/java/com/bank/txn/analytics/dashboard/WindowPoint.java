package com.bank.txn.analytics.dashboard;

import com.bank.txn.analytics.model.WindowAgg;

/** One window for charts. {@code complete} = window end has passed (may still be restated during grace). */
public record WindowPoint(long start, long end, boolean complete, WindowAgg agg) {
}
