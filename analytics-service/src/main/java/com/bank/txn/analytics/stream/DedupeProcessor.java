package com.bank.txn.analytics.stream;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Duration;

/**
 * Marks events whose eventId was already seen within the retention period as duplicates.
 * Duplicates always share the account key, so they are always on the same partition / task.
 */
public class DedupeProcessor implements FixedKeyProcessor<String, EnrichedEvent, EnrichedEvent> {
    public static final String STORE = "dedupe-store";
    private final long retentionMs;
    private FixedKeyProcessorContext<String, EnrichedEvent> context;
    private WindowStore<String, Long> seen;

    public DedupeProcessor(Duration retention) {
        this.retentionMs = retention.toMillis();
    }

    @Override
    public void init(FixedKeyProcessorContext<String, EnrichedEvent> context) {
        this.context = context;
        this.seen = context.getStateStore(STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, EnrichedEvent> record) {
        String eventId = record.value().event().eventId();
        long ts = record.timestamp();
        boolean duplicate;
        try (WindowStoreIterator<Long> it = seen.fetch(eventId, ts - retentionMs, ts + retentionMs)) {
            duplicate = it.hasNext();
        }
        if (duplicate) {
            context.forward(record.withValue(record.value().asDuplicate()));
        } else {
            seen.put(eventId, ts, ts);
            context.forward(record);
        }
    }
}
