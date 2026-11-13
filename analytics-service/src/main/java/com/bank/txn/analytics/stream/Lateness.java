package com.bank.txn.analytics.stream;

/** How an event relates to its event-time window when it arrives. */
public enum Lateness {
    /** not classified yet */
    UNKNOWN,
    /** its window is still open */
    ON_TIME,
    /** its window already ended but is within the grace period: the window result is corrected (restated) */
    LATE_ACCEPTED,
    /** window + grace already passed: excluded from aggregates, routed to transactions.late */
    LATE_DROPPED
}
