package com.bank.txn.common;

/** Kafka topic names shared by all services. */
public final class Topics {
    public static final String TRANSACTIONS_RAW = "transactions.raw";
    public static final String TRANSACTIONS_LATE = "transactions.late";
    public static final String TRANSACTIONS_DLQ = "transactions.dlq";
    public static final String METRICS_WINDOWED = "metrics.windowed";
    public static final String ALERTS = "alerts";

    private Topics() {
    }
}
