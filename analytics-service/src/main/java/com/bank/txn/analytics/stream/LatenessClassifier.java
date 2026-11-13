package com.bank.txn.analytics.stream;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

/**
 * Classifies each event against the window it belongs to, using Kafka Streams' stream time
 * (the highest event time observed so far). Same rule Kafka Streams uses for windows:
 * a window [start, end) is closed once {@code end + grace <= streamTime}.
 * Runs after the repartition to a single partition, so stream time is global across all accounts.
 */
public class LatenessClassifier implements FixedKeyProcessor<String, EnrichedEvent, EnrichedEvent> {
    private final long windowMs;
    private final long graceMs;
    private final LatencyTracker latency;
    private FixedKeyProcessorContext<String, EnrichedEvent> context;

    public LatenessClassifier(long windowMs, long graceMs, LatencyTracker latency) {
        this.windowMs = windowMs;
        this.graceMs = graceMs;
        this.latency = latency;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, EnrichedEvent> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, EnrichedEvent> record) {
        long eventTime = record.timestamp();
        long streamTime = context.currentStreamTimeMs();
        long windowEnd = eventTime - Math.floorMod(eventTime, windowMs) + windowMs;
        Lateness l;
        if (windowEnd + graceMs <= streamTime) {
            l = Lateness.LATE_DROPPED;
        } else if (windowEnd <= streamTime) {
            l = Lateness.LATE_ACCEPTED;
        } else {
            l = Lateness.ON_TIME;
        }
        EnrichedEvent v = record.value();
        if (!v.duplicate() && v.event().producedAt() != null) {
            latency.recordSince(v.event().producedAt().toEpochMilli());
        }
        context.forward(record.withValue(v.withLateness(l)));
    }
}
