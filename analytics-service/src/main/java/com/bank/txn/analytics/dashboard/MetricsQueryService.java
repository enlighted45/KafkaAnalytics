package com.bank.txn.analytics.dashboard;

import com.bank.txn.analytics.config.AnalyticsProperties;
import com.bank.txn.analytics.model.PipelineTotals;
import com.bank.txn.analytics.model.WindowAgg;
import com.bank.txn.analytics.stream.LatencyTracker;
import com.bank.txn.analytics.stream.TransactionTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Reads current aggregates straight from the Kafka Streams state stores (interactive queries). */
@Service
public class MetricsQueryService {
    private static final Logger log = LoggerFactory.getLogger(MetricsQueryService.class);

    private final StreamsBuilderFactoryBean streamsFactory;
    private final AnalyticsProperties props;
    private final LatencyTracker latency;
    private final Clock clock;

    public MetricsQueryService(StreamsBuilderFactoryBean streamsFactory, AnalyticsProperties props, LatencyTracker latency) {
        this.streamsFactory = streamsFactory;
        this.props = props;
        this.latency = latency;
        this.clock = Clock.systemUTC();
    }

    public DashboardUpdate current() {
        long now = clock.millis();
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        String state = streams == null ? "NOT_STARTED" : streams.state().name();
        List<WindowPoint> windows = List.of();
        List<WindowPoint> summary = List.of();
        PipelineTotals totals = new PipelineTotals();
        if (streams != null && streams.state() == KafkaStreams.State.RUNNING) {
            try {
                windows = windows(streams, TransactionTopology.WINDOW_10S_STORE, props.windowSize().toMillis(),
                        now - props.chartHistory().toMillis(), now);
                summary = windows(streams, TransactionTopology.WINDOW_1M_STORE, props.summaryWindowSize().toMillis(),
                        now - props.summaryHistory().toMillis(), now);
                // one totals record per partition: sum them
                ReadOnlyKeyValueStore<String, PipelineTotals> kv = streams.store(StoreQueryParameters.fromNameAndType(
                        TransactionTopology.TOTALS_STORE, QueryableStoreTypes.keyValueStore()));
                try (KeyValueIterator<String, PipelineTotals> it = kv.all()) {
                    it.forEachRemaining(e -> totals.merge(e.value));
                }
            } catch (InvalidStateStoreException e) {
                log.debug("State stores not queryable yet: {}", e.getMessage());
                state = "REBALANCING";
            }
        }
        Kpis kpis = KpiCalculator.compute(windows, props.windowSize().toMillis(), now);
        return new DashboardUpdate(now, state, props.windowSize().toMillis(), props.grace().toMillis(), kpis,
                windows, summary, totals, latency.snapshot());
    }

    /** Reads the windows of all local partitions in the time range and merges partials of the same window. */
    private static List<WindowPoint> windows(KafkaStreams streams, String store, long sizeMs, long from, long now) {
        ReadOnlyWindowStore<String, WindowAgg> ws = streams.store(
                StoreQueryParameters.fromNameAndType(store, QueryableStoreTypes.windowStore()));
        Map<Long, WindowAgg> merged = new TreeMap<>();
        try (KeyValueIterator<Windowed<String>, WindowAgg> it = ws.fetchAll(Instant.ofEpochMilli(from - sizeMs),
                Instant.ofEpochMilli(now))) {
            it.forEachRemaining(kv -> merged.merge(kv.key.window().start(), kv.value, WindowAgg::merge));
        }
        List<WindowPoint> out = new ArrayList<>(merged.size());
        merged.forEach((start, agg) -> out.add(new WindowPoint(start, start + sizeMs, start + sizeMs <= now, agg)));
        return out;
    }
}
