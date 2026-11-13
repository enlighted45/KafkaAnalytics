package com.bank.txn.analytics.stream;

import com.bank.txn.analytics.model.PipelineTotals;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

/** Running totals per input partition (summed across partitions when queried). Passes records through. */
public class TotalsProcessor implements FixedKeyProcessor<String, EnrichedEvent, EnrichedEvent> {
    private FixedKeyProcessorContext<String, EnrichedEvent> context;
    private KeyValueStore<String, PipelineTotals> totals;

    @Override
    public void init(FixedKeyProcessorContext<String, EnrichedEvent> context) {
        this.context = context;
        this.totals = context.getStateStore(TransactionTopology.TOTALS_STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, EnrichedEvent> record) {
        PipelineTotals t = totals.get(TransactionTopology.ALL);
        totals.put(TransactionTopology.ALL, (t == null ? new PipelineTotals() : t).add(record.value()));
        context.forward(record);
    }
}
