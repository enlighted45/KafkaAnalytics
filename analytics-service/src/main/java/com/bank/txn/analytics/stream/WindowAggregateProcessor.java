package com.bank.txn.analytics.stream;

import com.bank.txn.analytics.model.WindowAgg;
import com.bank.txn.analytics.model.WindowMetrics;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Tumbling-window aggregation per input partition (one task per partition, so one stream time per partition).
 * Events were already classified against window + grace by {@link LatenessClassifier}; only accepted events
 * arrive here. Changed windows are published at most every {@code emitInterval} (wall clock), not per record.
 */
public class WindowAggregateProcessor implements Processor<String, EnrichedEvent, String, WindowMetrics> {
    private final String store;
    private final String label;
    private final long sizeMs;
    private final Duration emitInterval;
    private final Set<Long> dirty = new LinkedHashSet<>();
    private ProcessorContext<String, WindowMetrics> context;
    private WindowStore<String, WindowAgg> windows;
    private int partition;

    public WindowAggregateProcessor(String store, String label, Duration size, Duration emitInterval) {
        this.store = store;
        this.label = label;
        this.sizeMs = size.toMillis();
        this.emitInterval = emitInterval;
    }

    @Override
    public void init(ProcessorContext<String, WindowMetrics> context) {
        this.context = context;
        this.windows = context.getStateStore(store);
        this.partition = context.taskId().partition();
        context.schedule(emitInterval, PunctuationType.WALL_CLOCK_TIME, now -> flush());
    }

    @Override
    public void process(Record<String, EnrichedEvent> record) {
        long start = record.timestamp() - Math.floorMod(record.timestamp(), sizeMs);
        WindowAgg agg = windows.fetch(TransactionTopology.ALL, start);
        windows.put(TransactionTopology.ALL, (agg == null ? new WindowAgg() : agg).add(record.value()), start);
        dirty.add(start);
    }

    private void flush() {
        for (long start : dirty) {
            WindowAgg agg = windows.fetch(TransactionTopology.ALL, start);
            if (agg != null) {
                context.forward(new Record<>(label + "@" + start + "@p" + partition,
                        new WindowMetrics(label, start, start + sizeMs, partition, agg), start));
            }
        }
        dirty.clear();
    }

    @Override
    public void close() {
        dirty.clear();
    }
}
