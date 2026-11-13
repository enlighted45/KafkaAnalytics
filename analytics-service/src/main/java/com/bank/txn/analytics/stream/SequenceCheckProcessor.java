package com.bank.txn.analytics.stream;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Tracks the highest sequence number seen per account.
 * <ul>
 *   <li>seq == last + 1: in order</li>
 *   <li>seq &gt; last + 1: gap (something is missing / still in flight)</li>
 *   <li>seq &lt;= last: out of order (arrived after a later event of the same account)</li>
 * </ul>
 */
public class SequenceCheckProcessor implements FixedKeyProcessor<String, EnrichedEvent, EnrichedEvent> {
    public static final String STORE = "sequence-store";
    private FixedKeyProcessorContext<String, EnrichedEvent> context;
    private KeyValueStore<String, Long> lastSeq;

    @Override
    public void init(FixedKeyProcessorContext<String, EnrichedEvent> context) {
        this.context = context;
        this.lastSeq = context.getStateStore(STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, EnrichedEvent> record) {
        EnrichedEvent v = record.value();
        if (v.duplicate()) {
            context.forward(record);
            return;
        }
        String account = v.event().accountId();
        long seq = v.event().sequenceNo();
        Long last = lastSeq.get(account);
        boolean outOfOrder = false, gap = false;
        if (last == null || seq > last) {
            gap = last != null && seq > last + 1;
            lastSeq.put(account, seq);
        } else {
            outOfOrder = true;
        }
        context.forward(record.withValue(v.withOrdering(outOfOrder, gap)));
    }
}
