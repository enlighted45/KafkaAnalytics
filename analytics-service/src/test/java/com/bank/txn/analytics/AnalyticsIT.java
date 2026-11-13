package com.bank.txn.analytics;

import com.bank.txn.common.Topics;
import com.bank.txn.common.model.*;
import com.bank.txn.common.model.Currency;
import com.bank.txn.common.serde.TransactionEventSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.messaging.converter.JacksonJsonMessageConverter;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end: real Kafka (Testcontainers) -> Kafka Streams topology -> WebSocket (STOMP) client.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.streams.replication-factor=1",
        "spring.kafka.streams.properties.topic.min.insync.replicas=1",
        "spring.kafka.streams.properties.commit.interval.ms=100",
        "spring.kafka.streams.application-id=txn-analytics-it"})
@Testcontainers
class AnalyticsIT {

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.4.0");

    @LocalServerPort
    int port;

    @BeforeAll
    static void createTopics() throws Exception {
        System.setProperty("STREAMS_STATE_DIR", System.getProperty("java.io.tmpdir") + "/it-state-" + UUID.randomUUID());
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.TRANSACTIONS_RAW, 3, (short) 1),
                    new NewTopic(Topics.TRANSACTIONS_LATE, 1, (short) 1),
                    new NewTopic(Topics.TRANSACTIONS_DLQ, 1, (short) 1),
                    new NewTopic(Topics.METRICS_WINDOWED, 1, (short) 1),
                    new NewTopic(Topics.ALERTS, 1, (short) 1))).all().get();
        }
    }

    private static TransactionEvent event(String account, long seq, String amount, Instant t) {
        return new TransactionEvent(UUID.randomUUID().toString(), account, seq, TransactionType.PAYMENT, Channel.POS,
                TransactionStatus.APPROVED, new BigDecimal(amount), Currency.INR, "GROCERY", Region.WEST, t, Instant.now());
    }

    @Test
    void transactionsFlowThroughStreamsToWebSocketClients() throws Exception {
        List<JsonNode> metrics = new CopyOnWriteArrayList<>();
        List<JsonNode> alerts = new CopyOnWriteArrayList<>();
        WebSocketStompClient stomp = new WebSocketStompClient(new StandardWebSocketClient());
        stomp.setMessageConverter(new JacksonJsonMessageConverter());
        StompSession session = stomp.connectAsync("ws://localhost:" + port + "/ws", new StompSessionHandlerAdapter() {
        }).get(10, TimeUnit.SECONDS);
        session.subscribe("/topic/metrics", collector(metrics));
        session.subscribe("/topic/alerts", collector(alerts));

        // wait until Kafka Streams is RUNNING before producing
        await().atMost(Duration.ofSeconds(60)).until(() ->
                !metrics.isEmpty() && "RUNNING".equals(metrics.getLast().get("streamsState").asString()));

        Instant now = Instant.now();
        try (var producer = new KafkaProducer<>(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()),
                new StringSerializer(), new TransactionEventSerializer());
             var bytes = new KafkaProducer<>(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()),
                     new StringSerializer(), new ByteArraySerializer())) {
            TransactionEvent dup = null;
            for (int i = 0; i < 100; i++) {
                String acc = "ACC-" + (i % 10);
                TransactionEvent e = event(acc, i / 10 + 1, "100.00", now.minusMillis(100 - i));
                producer.send(new ProducerRecord<>(Topics.TRANSACTIONS_RAW, acc, e));
                if (i == 50) dup = e;
            }
            producer.send(new ProducerRecord<>(Topics.TRANSACTIONS_RAW, dup.accountId(), dup));          // duplicate
            TransactionEvent big = event("ACC-BIG", 1, "250000.00", now);
            producer.send(new ProducerRecord<>(Topics.TRANSACTIONS_RAW, big.accountId(), big));          // high value
            bytes.send(new ProducerRecord<>(Topics.TRANSACTIONS_RAW, "BAD", "garbage".getBytes(StandardCharsets.UTF_8)));
            producer.flush();
            bytes.flush();
        }

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            JsonNode totals = metrics.getLast().get("totals");
            assertThat(totals.get("processed").asLong()).isEqualTo(101);
            assertThat(totals.get("duplicates").asLong()).isEqualTo(1);
            assertThat(totals.get("received").asLong()).isEqualTo(102);
        });
        JsonNode last = metrics.getLast();
        long windowCount = 0;
        for (JsonNode w : last.get("windows")) windowCount += w.get("agg").get("count").asLong();
        assertThat(windowCount).isEqualTo(101);
        assertThat(last.get("latency").get("samples").asInt()).isGreaterThan(0);

        await().atMost(Duration.ofSeconds(30)).until(() -> alerts.stream()
                .anyMatch(a -> "HIGH_VALUE".equals(a.get("type").asString()) && "ACC-BIG".equals(a.get("accountId").asString())));

        // REST snapshot for freshly opened dashboards
        HttpResponse<String> res = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/dashboard/snapshot")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).isEqualTo(200);
        JsonNode snap = JsonMapper.shared().readTree(res.body());
        assertThat(snap.get("metrics").get("totals").get("processed").asLong()).isEqualTo(101);
        assertThat(snap.get("alerts").size()).isGreaterThanOrEqualTo(1);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            HttpResponse<String> r = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/dashboard/snapshot")).build(),
                    HttpResponse.BodyHandlers.ofString());
            JsonNode health = JsonMapper.shared().readTree(r.body()).get("health");
            assertThat(health.get("reachable").asBoolean()).isTrue();
            assertThat(health.get("brokers").size()).isEqualTo(1);
        });
        session.disconnect();
    }

    private static StompFrameHandler collector(List<JsonNode> sink) {
        return new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return JsonNode.class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                sink.add((JsonNode) payload);
            }
        };
    }
}
