package com.bank.txn.analytics.stream;

import com.bank.txn.analytics.model.Alert;
import com.bank.txn.analytics.model.PipelineTotals;
import com.bank.txn.analytics.model.WindowAgg;
import com.bank.txn.analytics.model.WindowMetrics;
import com.bank.txn.common.Topics;
import com.bank.txn.common.model.*;
import com.bank.txn.common.model.Currency;
import com.bank.txn.common.serde.JsonDeserializer;
import com.bank.txn.common.serde.TransactionEventDeserializer;
import com.bank.txn.common.serde.TransactionEventSerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the full topology with Kafka's TopologyTestDriver (no broker needed).
 * Time base T0 is aligned to a 1-minute boundary so window arithmetic in the tests is easy to read.
 */
class TransactionTopologyTest {

    static final long T0 = Instant.parse("2026-11-12T10:00:00Z").toEpochMilli();

    private TopologyTestDriver driver;
    private TestInputTopic<String, TransactionEvent> raw;
    private TestInputTopic<String, byte[]> rawBytes;
    private TestOutputTopic<String, TransactionEvent> late;
    private TestOutputTopic<String, byte[]> dlq;
    private TestOutputTopic<String, Alert> alerts;
    private TestOutputTopic<String, WindowMetrics> metrics;
    private final Map<String, Long> seq = new HashMap<>();
    private java.io.File stateDir;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        TransactionTopology.build(builder, TransactionTopology.Config.defaults(), new LatencyTracker(100, Clock.systemUTC()));
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // RocksDB is flushed on every TopologyTestDriver commit (= every record); use tmpfs when available
        String base = new java.io.File("/dev/shm").canWrite() ? "/dev/shm" : System.getProperty("java.io.tmpdir");
        stateDir = new java.io.File(base, "ttd-" + UUID.randomUUID());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
        driver = new TopologyTestDriver(builder.build(), props);
        raw = driver.createInputTopic(Topics.TRANSACTIONS_RAW, new StringSerializer(), new TransactionEventSerializer());
        rawBytes = driver.createInputTopic(Topics.TRANSACTIONS_RAW, new StringSerializer(), new ByteArraySerializer());
        late = driver.createOutputTopic(Topics.TRANSACTIONS_LATE, new StringDeserializer(), new TransactionEventDeserializer());
        dlq = driver.createOutputTopic(Topics.TRANSACTIONS_DLQ, new StringDeserializer(), new ByteArrayDeserializer());
        alerts = driver.createOutputTopic(Topics.ALERTS, new StringDeserializer(), new JsonDeserializer<>(Alert.class));
        metrics = driver.createOutputTopic(Topics.METRICS_WINDOWED, new StringDeserializer(), new JsonDeserializer<>(WindowMetrics.class));
    }

    @AfterEach
    void tearDown() throws java.io.IOException {
        driver.close();
        if (stateDir.exists()) {
            try (var paths = java.nio.file.Files.walk(stateDir.toPath())) {
                paths.sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(java.io.File::delete);
            }
        }
    }

    // ---------- helpers ----------

    private TransactionEvent event(String account, long eventTimeOffsetMs, String amount, TransactionStatus status,
                                   TransactionType type) {
        long s = seq.merge(account, 1L, Long::sum);
        return new TransactionEvent(UUID.randomUUID().toString(), account, s, type, Channel.ONLINE, status,
                new BigDecimal(amount), Currency.INR, null, Region.NORTH, Instant.ofEpochMilli(T0 + eventTimeOffsetMs),
                Instant.ofEpochMilli(T0 + eventTimeOffsetMs));
    }

    private TransactionEvent event(String account, long offsetMs) {
        return event(account, offsetMs, "100.00", TransactionStatus.APPROVED, TransactionType.PAYMENT);
    }

    /** Sends an event; arrival (record) time is irrelevant - processing must use eventTime. */
    private void send(TransactionEvent e) {
        raw.pipeInput(e.accountId(), e, T0 + 999_999);
    }

    private WindowAgg window10s(long startOffsetMs) {
        WindowStore<String, WindowAgg> store = driver.getWindowStore(TransactionTopology.WINDOW_10S_STORE);
        return store.fetch(TransactionTopology.ALL, T0 + startOffsetMs);
    }

    private WindowAgg window1m(long startOffsetMs) {
        WindowStore<String, WindowAgg> store = driver.getWindowStore(TransactionTopology.WINDOW_1M_STORE);
        return store.fetch(TransactionTopology.ALL, T0 + startOffsetMs);
    }

    private PipelineTotals totals() {
        KeyValueStore<String, PipelineTotals> store = driver.getKeyValueStore(TransactionTopology.TOTALS_STORE);
        return store.get(TransactionTopology.ALL);
    }

    // ---------- event-time windowing ----------

    @Test
    void eventsAreWindowedByEventTimeNotArrivalTime() {
        send(event("A", 1_000));
        send(event("B", 9_999));
        send(event("C", 10_000));
        send(event("D", 25_000));

        assertThat(window10s(0).count).isEqualTo(2);
        assertThat(window10s(10_000).count).isEqualTo(1);
        assertThat(window10s(20_000).count).isEqualTo(1);
        assertThat(window1m(0).count).isEqualTo(4);
    }

    @Test
    void aggregatesAreExact() {
        send(event("A", 1_000, "100.00", TransactionStatus.APPROVED, TransactionType.DEPOSIT));
        send(event("B", 2_000, "250.50", TransactionStatus.APPROVED, TransactionType.PAYMENT));
        send(event("C", 3_000, "999.99", TransactionStatus.DECLINED, TransactionType.PAYMENT));
        TransactionEvent usd = new TransactionEvent("usd-1", "D", 1, TransactionType.TRANSFER, Channel.MOBILE,
                TransactionStatus.APPROVED, new BigDecimal("10.00"), Currency.USD, null, Region.SOUTH,
                Instant.ofEpochMilli(T0 + 4_000), Instant.ofEpochMilli(T0 + 4_000));
        send(usd);

        WindowAgg w = window10s(0);
        assertThat(w.count).isEqualTo(4);
        assertThat(w.approved).isEqualTo(3);
        assertThat(w.declined).isEqualTo(1);
        assertThat(w.volume).isEqualByComparingTo("1185.50");          // 100 + 250.50 + 835.00 (10 USD), declined excluded
        assertThat(w.maxAmount).isEqualByComparingTo("999.99");
        assertThat(w.byType.get("PAYMENT").count).isEqualTo(1);
        assertThat(w.byType.get("DEPOSIT").volume).isEqualByComparingTo("100.00");
        assertThat(w.byType.get("TRANSFER").volume).isEqualByComparingTo("835.00");
        assertThat(w.byRegion.get("SOUTH").count).isEqualTo(1);
        assertThat(w.byCurrency).containsEntry("USD", 1L).containsEntry("INR", 3L);
    }

    // ---------- late events ----------

    @Test
    void lateEventWithinGraceCorrectsItsWindow() {
        send(event("A", 1_000));
        send(event("B", 15_000));          // stream time -> 15s, window [0,10) has ended
        send(event("C", 2_000));           // late, but 10s + 30s grace > 15s -> accepted

        assertThat(window10s(0).count).isEqualTo(2);
        assertThat(window10s(0).lateAccepted).isEqualTo(1);
        assertThat(totals().lateAccepted).isEqualTo(1);
        assertThat(totals().lateDropped).isZero();
        assertThat(late.isEmpty()).isTrue();
    }

    @Test
    void eventLaterThanGraceIsDroppedFromWindowsAndRoutedToLateTopic() {
        send(event("A", 1_000));
        send(event("B", 50_000));          // stream time -> 50s; window [0,10) closed at 10s + 30s grace = 40s
        TransactionEvent tooLate = event("C", 5_000);
        send(tooLate);

        assertThat(window10s(0).count).isEqualTo(1);
        assertThat(window1m(0).count).isEqualTo(2);  // the 1-minute window only counts A and B
        assertThat(totals().lateDropped).isEqualTo(1);
        assertThat(totals().processed).isEqualTo(2);
        List<KeyValue<String, TransactionEvent>> out = late.readKeyValuesToList();
        assertThat(out).hasSize(1);
        assertThat(out.getFirst().key).isEqualTo("C");
        assertThat(out.getFirst().value.eventId()).isEqualTo(tooLate.eventId());
    }

    @Test
    void graceBoundaryIsExclusive() {
        send(event("A", 1_000));
        send(event("B", 39_999));
        send(event("C", 2_000));           // close time 40s > 39.999s -> still accepted
        send(event("D", 40_000));
        send(event("E", 3_000));           // close time 40s <= 40s -> dropped
        assertThat(window10s(0).count).isEqualTo(2);
        assertThat(totals().lateAccepted).isEqualTo(1);
        assertThat(totals().lateDropped).isEqualTo(1);
    }

    // ---------- duplicates and ordering ----------

    @Test
    void duplicatesAreCountedOnce() {
        TransactionEvent e = event("A", 1_000);
        send(e);
        send(e);
        send(e);
        assertThat(window10s(0).count).isEqualTo(1);
        assertThat(totals().duplicates).isEqualTo(2);
        assertThat(totals().received).isEqualTo(3);
        assertThat(totals().processed).isEqualTo(1);
    }

    @Test
    void outOfOrderAndGapsAreDetectedPerAccount() {
        TransactionEvent s1 = event("A", 1_000);
        TransactionEvent s2 = event("A", 1_100);
        TransactionEvent s3 = event("A", 1_200);
        TransactionEvent s4 = event("A", 1_300);
        send(s1);
        send(s3);   // gap (2 missing)
        send(s2);   // out of order
        send(s4);   // in order again
        send(event("B", 1_400));  // other account unaffected

        assertThat(totals().gaps).isEqualTo(1);
        assertThat(totals().outOfOrder).isEqualTo(1);
        assertThat(window10s(0).count).isEqualTo(5);   // out-of-order events are still counted
        assertThat(window10s(0).outOfOrder).isEqualTo(1);
    }

    // ---------- alerts ----------

    @Test
    void highValueTransactionRaisesAlert() {
        send(event("A", 1_000, "99999.99", TransactionStatus.APPROVED, TransactionType.TRANSFER));
        send(event("B", 2_000, "150000.00", TransactionStatus.APPROVED, TransactionType.TRANSFER));
        List<Alert> list = alerts.readValuesToList();
        assertThat(list).hasSize(1);
        assertThat(list.getFirst().type()).isEqualTo(Alert.AlertType.HIGH_VALUE);
        assertThat(list.getFirst().accountId()).isEqualTo("B");
    }

    @Test
    void velocityAlertFiresOnceWhenAccountExceedsThresholdInOneMinute() {
        for (int i = 0; i < 15; i++) {
            send(event("BURST", 1_000 + i * 1_000L));
        }
        for (int i = 0; i < 10; i++) {
            send(event("NORMAL", 1_000 + i * 1_000L));   // exactly at threshold -> no alert
        }
        List<Alert> list = alerts.readValuesToList();
        assertThat(list).hasSize(1);
        assertThat(list.getFirst().type()).isEqualTo(Alert.AlertType.VELOCITY);
        assertThat(list.getFirst().accountId()).isEqualTo("BURST");
        assertThat(list.getFirst().count()).isEqualTo(11);
    }

    @Test
    void duplicatesDoNotTriggerVelocityAlert() {
        TransactionEvent e = event("A", 1_000);
        for (int i = 0; i < 20; i++) {
            send(e);
        }
        assertThat(alerts.isEmpty()).isTrue();
    }

    // ---------- bad data ----------

    @Test
    void malformedAndInvalidRecordsGoToDlqWithoutStoppingTheStream() {
        send(event("A", 1_000));
        // bad records arriving much later (Kafka timestamp far ahead) must not advance stream time
        rawBytes.pipeInput("X", "{not json".getBytes(StandardCharsets.UTF_8), T0 + 3_600_000);
        rawBytes.pipeInput("Y", "{\"eventId\":\"1\",\"accountId\":\"Y\"}".getBytes(StandardCharsets.UTF_8), T0 + 3_600_000);
        send(event("B", 2_000));

        List<KeyValue<String, byte[]>> bad = dlq.readKeyValuesToList();
        assertThat(bad).extracting(kv -> kv.key).containsExactly("X", "Y");
        assertThat(new String(bad.getFirst().value, StandardCharsets.UTF_8)).isEqualTo("{not json");
        assertThat(window10s(0).count).isEqualTo(2);
        assertThat(totals().lateDropped).isZero();
    }

    // ---------- output topic ----------

    @Test
    void windowResultsArePublishedToMetricsTopic() {
        send(event("A", 1_000));
        send(event("B", 2_000));
        assertThat(metrics.isEmpty()).isTrue();                 // emitted on a 1 s wall-clock cadence, not per record
        driver.advanceWallClockTime(java.time.Duration.ofSeconds(1));
        List<KeyValue<String, WindowMetrics>> out = metrics.readKeyValuesToList();
        WindowMetrics last10s = out.stream().map(kv -> kv.value).filter(m -> m.size().equals("10s")).reduce((a, b) -> b).orElseThrow();
        assertThat(last10s.start()).isEqualTo(T0);
        assertThat(last10s.end()).isEqualTo(T0 + 10_000);
        assertThat(last10s.agg().count).isEqualTo(2);
        assertThat(last10s.partition()).isZero();
        assertThat(out).anySatisfy(kv -> assertThat(kv.key).isEqualTo("1m@" + T0 + "@p0"));
        driver.advanceWallClockTime(java.time.Duration.ofSeconds(1));
        assertThat(metrics.isEmpty()).as("unchanged windows are not re-emitted").isTrue();
    }
}
