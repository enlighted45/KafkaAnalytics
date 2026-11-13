package com.bank.txn.producer.generator;

import com.bank.txn.common.model.TransactionEvent;

import java.time.Instant;

/** An event plus the wall-clock time at which it should be sent (later than now for late events). */
public record PlannedEvent(TransactionEvent event, Instant sendAt, Injection injection) {
}
