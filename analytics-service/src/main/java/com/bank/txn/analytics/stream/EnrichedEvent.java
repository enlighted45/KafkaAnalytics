package com.bank.txn.analytics.stream;

import com.bank.txn.common.model.TransactionEvent;

/** A transaction plus the flags the stream processor attaches to it. */
public record EnrichedEvent(TransactionEvent event, boolean duplicate, boolean outOfOrder, boolean gap,
                            Lateness lateness) {

    public static EnrichedEvent of(TransactionEvent event) {
        return new EnrichedEvent(event, false, false, false, Lateness.UNKNOWN);
    }

    public EnrichedEvent asDuplicate() {
        return new EnrichedEvent(event, true, outOfOrder, gap, lateness);
    }

    public EnrichedEvent withOrdering(boolean outOfOrder, boolean gap) {
        return new EnrichedEvent(event, duplicate, outOfOrder, gap, lateness);
    }

    public EnrichedEvent withLateness(Lateness lateness) {
        return new EnrichedEvent(event, duplicate, outOfOrder, gap, lateness);
    }

    /** Counted in the windowed metrics? */
    public boolean accepted() {
        return !duplicate && lateness != Lateness.LATE_DROPPED;
    }
}
