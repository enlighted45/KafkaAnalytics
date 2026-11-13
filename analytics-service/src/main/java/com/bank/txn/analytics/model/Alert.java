package com.bank.txn.analytics.model;

import java.math.BigDecimal;
import java.time.Instant;

public record Alert(String alertId, AlertType type, String accountId, String eventId, BigDecimal amountInr,
                    long count, Instant eventTime, Instant detectedAt, String message) {
    public enum AlertType { HIGH_VALUE, VELOCITY }
}
