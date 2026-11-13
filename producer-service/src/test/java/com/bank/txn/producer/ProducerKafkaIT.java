package com.bank.txn.producer;

import com.bank.txn.common.Topics;
import com.bank.txn.common.model.TransactionEvent;
import com.bank.txn.common.serde.TransactionEventDeserializer;
import com.bank.txn.producer.generator.GeneratorService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.ProducerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/** Generator -> real Kafka broker (Testcontainers): keys, partitioning and producer guarantees. */
@SpringBootTest(properties = {"generator-autostart=false", "generator.rate-per-second=200",
        "generator.accounts=50", "generator.late-ratio=0", "generator.out-of-order-ratio=0",
        "generator.duplicate-ratio=0", "generator.burst-chance-per-second=0"})
@Testcontainers
class ProducerKafkaIT {

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.4.0");

    @Autowired GeneratorService generator;
    @Autowired ProducerFactory<String, TransactionEvent> producerFactory;

    @BeforeAll
    static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(Topics.TRANSACTIONS_RAW, 6, (short) 1))).all().get();
        }
    }

    @Test
    void producerIsIdempotentWithAcksAll() {
        Map<String, Object> cfg = producerFactory.getConfigurationProperties();
        assertThat(cfg.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(String.valueOf(cfg.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))).isEqualTo("true");
    }

    @Test
    void eventsAreKeyedByAccountAndOrderedPerAccount() {
        generator.start();
        List<ConsumerRecord<String, TransactionEvent>> records = new ArrayList<>();
        try (KafkaConsumer<String, TransactionEvent> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new TransactionEventDeserializer())) {
            consumer.subscribe(List.of(Topics.TRANSACTIONS_RAW));
            long deadline = System.currentTimeMillis() + 20_000;
            while (records.size() < 400 && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        } finally {
            generator.stop();
        }

        assertThat(records).hasSizeGreaterThanOrEqualTo(400);
        Map<String, Integer> partitionOfAccount = new HashMap<>();
        Map<String, Long> lastSeq = new HashMap<>();
        for (ConsumerRecord<String, TransactionEvent> r : records) {
            assertThat(r.key()).isEqualTo(r.value().accountId());
            assertThat(r.value().producedAt()).isNotNull();
            // same account -> always the same partition
            Integer p = partitionOfAccount.putIfAbsent(r.key(), r.partition());
            if (p != null) {
                assertThat(r.partition()).isEqualTo(p);
            }
            // within a partition, an account's sequence numbers only go up (no injected disorder in this test)
            Long prev = lastSeq.put(r.key(), r.value().sequenceNo());
            if (prev != null) {
                assertThat(r.value().sequenceNo()).isGreaterThan(prev);
            }
        }
        assertThat(new HashSet<>(partitionOfAccount.values())).hasSizeGreaterThan(1);
    }
}
