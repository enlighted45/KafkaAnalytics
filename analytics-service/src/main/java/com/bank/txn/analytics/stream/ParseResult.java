package com.bank.txn.analytics.stream;

import com.bank.txn.common.model.TransactionEvent;

/** Result of parsing a raw record: either an event or the original bytes + error for the DLQ. */
public record ParseResult(TransactionEvent event, byte[] raw, String error) {
    public boolean failed() {
        return event == null;
    }
}
