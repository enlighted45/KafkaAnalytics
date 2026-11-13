package com.bank.txn.analytics.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Uses the transaction's {@code eventTime} as the record timestamp, so stream time and all windows are
 * based on when transactions happened, not when they reached Kafka. Invalid records (headed for the DLQ) take
 * the current partition time so a bad record can never move stream time and close windows early.
 */
public class EventTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof ParseResult r && !r.failed()) {
            return r.event().eventTime().toEpochMilli();
        }
        if (partitionTime >= 0) {
            return partitionTime;
        }
        return Math.max(record.timestamp(), 0);
    }
}
