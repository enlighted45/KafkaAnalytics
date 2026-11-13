package com.bank.txn.common.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A banking transaction event.
 *
 * @param eventId     globally unique id; used for de-duplication
 * @param accountId   account; also the Kafka record key, so per-account order is preserved
 * @param sequenceNo  per-account monotonically increasing number, used to detect out-of-order delivery
 * @param eventTime   when the transaction happened (event time, used for windowing)
 * @param producedAt  when the event was sent to Kafka (used for end-to-end latency)
 */
public record TransactionEvent(
        String eventId,
        String accountId,
        long sequenceNo,
        TransactionType type,
        Channel channel,
        TransactionStatus status,
        BigDecimal amount,
        Currency currency,
        String merchantCategory,
        Region region,
        Instant eventTime,
        Instant producedAt) {

    public BigDecimal amountInr() {
        return currency.toInr(amount);
    }

    public boolean approved() {
        return status == TransactionStatus.APPROVED;
    }

    public TransactionEvent withProducedAt(Instant producedAt) {
        return new TransactionEvent(eventId, accountId, sequenceNo, type, channel, status, amount, currency,
                merchantCategory, region, eventTime, producedAt);
    }
}
