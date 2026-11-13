package com.bank.txn.analytics.stream;

import com.bank.txn.analytics.model.Alert;
import com.bank.txn.analytics.model.PipelineTotals;
import com.bank.txn.analytics.model.WindowAgg;
import com.bank.txn.analytics.model.WindowMetrics;
import com.bank.txn.common.Topics;
import com.bank.txn.common.model.TransactionEvent;
import com.bank.txn.common.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The analytics pipeline. Everything runs in ONE sub-topology, i.e. one task per input partition, so every
 * partition keeps its own stream time (event time). Global numbers are the sum of the per-partition partial
 * aggregates, merged when queried. This keeps late-event classification correct even when partitions are
 * consumed at different speeds (e.g. while catching up after a restart), and scales with partitions.
 * <pre>
 * transactions.raw (key=accountId, 6 partitions)
 *   -> parse + validate; record timestamp = eventTime           (invalid  -> transactions.dlq)
 *   -> dedupe by eventId                                        (flags duplicates)
 *   -> per-account sequence check                               (flags out-of-order / gaps)
 *   -> classify lateness vs window + grace (partition stream time)
 *   -> totals per partition                                     -> store "pipeline-totals"
 *   -> alerts: high value, velocity (count per account per minute)  -> alerts
 *   -> too late                                                 -> transactions.late
 *   -> 10 s and 1 min tumbling windows per partition            -> stores + metrics.windowed (partials)
 * </pre>
 */
public final class TransactionTopology {
    private static final Logger log = LoggerFactory.getLogger(TransactionTopology.class);

    public static final String ALL = "ALL";
    public static final String WINDOW_10S_STORE = "window-10s";
    public static final String WINDOW_1M_STORE = "window-1m";
    public static final String TOTALS_STORE = "pipeline-totals";
    public static final String VELOCITY_STORE = "velocity-counts";
    public static final Duration EMIT_INTERVAL = Duration.ofSeconds(1);

    /**
     * All state is small (minutes of event ids, per-account sequences, ~100 windows), so stores are in-memory,
     * backed by replicated changelog topics. Persistent RocksDB stores caused multi-second stalls on a busy
     * disk: per-commit flushes under exactly-once, and a new RocksDB segment every 60 s per task for the
     * short-retention window stores. Small segments + eager compaction keep the changelogs short, so restoring
     * state on restart takes seconds. (Window-store changelog retention: windowstore.changelog.additional.retention.ms.)
     */
    static final Map<String, String> CHANGELOG = Map.of(
            "segment.ms", "600000",
            "min.cleanable.dirty.ratio", "0.1");  // min.insync.replicas comes from topic.min.insync.replicas

    /** Tunables of the topology. */
    public record Config(Duration windowSize, Duration summaryWindowSize, Duration grace,
                         BigDecimal highValueThresholdInr, long velocityThreshold, Duration velocityWindow,
                         Duration dedupeRetention) {
        public static Config defaults() {
            return new Config(Duration.ofSeconds(10), Duration.ofMinutes(1), Duration.ofSeconds(30),
                    new BigDecimal("100000"), 10, Duration.ofMinutes(1), Duration.ofMinutes(10));
        }
    }

    private TransactionTopology() {
    }

    public static void build(StreamsBuilder builder, Config cfg, LatencyTracker latency) {
        Serde<String> str = Serdes.String();
        Serde<EnrichedEvent> enrichedSerde = new JsonSerde<>(EnrichedEvent.class);
        Serde<WindowAgg> aggSerde = new JsonSerde<>(WindowAgg.class);

        builder.addStateStore(Stores.windowStoreBuilder(
                        Stores.inMemoryWindowStore(DedupeProcessor.STORE, cfg.dedupeRetention(), cfg.dedupeRetention(), false),
                        str, Serdes.Long())
                .withLoggingEnabled(CHANGELOG));
        // cached KV stores: the changelog gets one write per key per commit instead of one per event
        builder.addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(SequenceCheckProcessor.STORE), str, Serdes.Long())
                .withCachingEnabled().withLoggingEnabled(CHANGELOG));
        builder.addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(TOTALS_STORE), str, new JsonSerde<>(PipelineTotals.class))
                .withCachingEnabled().withLoggingEnabled(CHANGELOG));
        addWindowStore(builder, WINDOW_10S_STORE, cfg.windowSize(), Duration.ofMinutes(15), aggSerde);
        addWindowStore(builder, WINDOW_1M_STORE, cfg.summaryWindowSize(), Duration.ofMinutes(60), aggSerde);

        // 1. parse + validate (event time becomes the record timestamp); bad records -> DLQ
        Map<String, KStream<String, ParseResult>> parsed = builder
                .stream(Topics.TRANSACTIONS_RAW, Consumed.with(str, ParseResultSerde.serde())
                        .withTimestampExtractor(new EventTimeExtractor()).withName("source-raw"))
                .split(Named.as("parse-"))
                .branch((k, v) -> v.failed(), Branched.as("failed"))
                .defaultBranch(Branched.as("ok"));

        parsed.get("parse-failed")
                .peek((k, v) -> log.warn("Invalid transaction (key={}) sent to DLQ: {}", k, v.error()), Named.as("dlq-log"))
                .mapValues(ParseResult::raw, Named.as("dlq-raw-bytes"))
                .to(Topics.TRANSACTIONS_DLQ, Produced.with(str, Serdes.ByteArray()).withName("sink-dlq"));

        // 2. dedupe, 3. ordering, 4. lateness, 5. totals - all per partition
        KStream<String, EnrichedEvent> classified = parsed.get("parse-ok")
                .mapValues(r -> EnrichedEvent.of(r.event()), Named.as("to-enriched"))
                .processValues(() -> new DedupeProcessor(cfg.dedupeRetention()), Named.as("dedupe"), DedupeProcessor.STORE)
                .processValues(SequenceCheckProcessor::new, Named.as("sequence-check"), SequenceCheckProcessor.STORE)
                .processValues(() -> new LatenessClassifier(cfg.windowSize().toMillis(), cfg.grace().toMillis(), latency),
                        Named.as("classify-lateness"))
                .processValues(TotalsProcessor::new, Named.as("totals"), TOTALS_STORE);

        // 6. alerts (unique events, keyed by account)
        KStream<String, EnrichedEvent> unique = classified.filter((k, v) -> !v.duplicate(), Named.as("unique"));
        unique.filter((k, v) -> v.event().amountInr().compareTo(cfg.highValueThresholdInr()) >= 0, Named.as("high-value"))
                .mapValues(v -> highValueAlert(v.event()), Named.as("high-value-alert"))
                .to(Topics.ALERTS, Produced.with(str, new JsonSerde<>(Alert.class)).withName("sink-alerts-hv"));

        unique.groupByKey(Grouped.with("by-account", str, enrichedSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(cfg.velocityWindow(), cfg.grace()))
                .count(Named.as("velocity-count"), Materialized.<String, Long>as(Stores.inMemoryWindowStore(VELOCITY_STORE,
                                cfg.velocityWindow().plus(cfg.grace()), cfg.velocityWindow(), false))
                        .withKeySerde(str).withValueSerde(Serdes.Long()).withCachingDisabled().withLoggingEnabled(CHANGELOG))
                .toStream(Named.as("velocity-stream"))
                // fire exactly once per account+window: when the count first exceeds the threshold
                .filter((wk, count) -> count != null && count == cfg.velocityThreshold() + 1, Named.as("velocity-exceeded"))
                .map((wk, count) -> KeyValue.pair(wk.key(), velocityAlert(wk.key(), count, wk.window().startTime())),
                        Named.as("velocity-alert"))
                .to(Topics.ALERTS, Produced.with(str, new JsonSerde<>(Alert.class)).withName("sink-alerts-velocity"));

        // 7. too late -> side topic (kept for audit, not lost)
        unique.filter((k, v) -> v.lateness() == Lateness.LATE_DROPPED, Named.as("late-dropped"))
                .mapValues(EnrichedEvent::event, Named.as("late-event"))
                .to(Topics.TRANSACTIONS_LATE, Produced.with(str, new JsonSerde<>(TransactionEvent.class)).withName("sink-late"));

        // 8. windowed aggregates of accepted events (per partition partials)
        KStream<String, EnrichedEvent> accepted = classified.filter((k, v) -> v.accepted(), Named.as("accepted"));
        Produced<String, WindowMetrics> metricsOut = Produced.with(str, new JsonSerde<>(WindowMetrics.class));
        accepted.process(() -> new WindowAggregateProcessor(WINDOW_10S_STORE, "10s", cfg.windowSize(), EMIT_INTERVAL),
                        Named.as("agg-10s"), WINDOW_10S_STORE)
                .to(Topics.METRICS_WINDOWED, metricsOut.withName("sink-metrics-10s"));
        accepted.process(() -> new WindowAggregateProcessor(WINDOW_1M_STORE, "1m", cfg.summaryWindowSize(), EMIT_INTERVAL),
                        Named.as("agg-1m"), WINDOW_1M_STORE)
                .to(Topics.METRICS_WINDOWED, Produced.with(str, new JsonSerde<>(WindowMetrics.class)).withName("sink-metrics-1m"));
    }

    private static void addWindowStore(StreamsBuilder builder, String name, Duration size, Duration retention,
                                       Serde<WindowAgg> serde) {
        builder.addStateStore(Stores.windowStoreBuilder(
                        Stores.inMemoryWindowStore(name, retention, size, false), Serdes.String(), serde)
                .withCachingEnabled().withLoggingEnabled(CHANGELOG));
    }

    static Alert highValueAlert(TransactionEvent e) {
        return new Alert(UUID.randomUUID().toString(), Alert.AlertType.HIGH_VALUE, e.accountId(), e.eventId(),
                e.amountInr(), 1, e.eventTime(), Instant.now(),
                "High-value %s of INR %,.2f via %s".formatted(e.type(), e.amountInr(), e.channel()));
    }

    static Alert velocityAlert(String account, long count, Instant windowStart) {
        return new Alert(UUID.randomUUID().toString(), Alert.AlertType.VELOCITY, account, null, null, count,
                windowStart, Instant.now(), "More than %d transactions within 1 minute".formatted(count - 1));
    }
}
