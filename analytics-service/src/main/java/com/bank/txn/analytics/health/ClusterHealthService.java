package com.bank.txn.analytics.health;

import com.bank.txn.common.Topics;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Polls the cluster with the Admin API: broker liveness, partition leaders, active controller, lag. */
@Service
public class ClusterHealthService {
    private static final Logger log = LoggerFactory.getLogger(ClusterHealthService.class);
    private static final List<String> TOPICS = List.of(Topics.TRANSACTIONS_RAW, Topics.TRANSACTIONS_LATE,
            Topics.TRANSACTIONS_DLQ, Topics.METRICS_WINDOWED, Topics.ALERTS);

    private final Admin admin;
    private final SimpMessagingTemplate ws;
    private final String streamsGroup;
    private volatile ClusterHealth latest = ClusterHealth.unreachable("not checked yet");

    public ClusterHealthService(KafkaAdmin kafkaAdmin, SimpMessagingTemplate ws,
                                @Value("${spring.kafka.streams.application-id}") String streamsGroup) {
        Map<String, Object> cfg = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        cfg.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 4000);
        this.admin = Admin.create(cfg);
        this.ws = ws;
        this.streamsGroup = streamsGroup;
    }

    @Scheduled(fixedRateString = "${analytics.health-interval-ms:3000}")
    public void refresh() {
        latest = check();
        ws.convertAndSend("/topic/health", latest);
    }

    public ClusterHealth latest() {
        return latest;
    }

    ClusterHealth check() {
        try {
            Collection<Node> live = admin.describeCluster().nodes().get(4, TimeUnit.SECONDS);
            Map<Integer, Node> liveById = live.stream().collect(Collectors.toMap(Node::id, n -> n));

            int controller = -1;
            List<Integer> voters = List.of();
            try {
                QuorumInfo q = admin.describeMetadataQuorum().quorumInfo().get(4, TimeUnit.SECONDS);
                controller = q.leaderId();
                voters = q.voters().stream().map(QuorumInfo.ReplicaState::replicaId).sorted().toList();
            } catch (Exception e) {
                log.debug("Quorum info unavailable: {}", e.toString());
            }

            Map<String, TopicDescription> topics = admin.describeTopics(TOPICS).allTopicNames().get(4, TimeUnit.SECONDS);
            Map<Integer, int[]> perBroker = new TreeMap<>(); // id -> [leaders, replicas]
            liveById.keySet().forEach(id -> perBroker.put(id, new int[2]));
            int total = 0, under = 0, offline = 0;
            for (TopicDescription td : topics.values()) {
                for (TopicPartitionInfo p : td.partitions()) {
                    total++;
                    if (p.leader() == null || p.leader().isEmpty()) {
                        offline++;
                    } else {
                        perBroker.computeIfAbsent(p.leader().id(), k -> new int[2])[0]++;
                    }
                    if (p.isr().size() < p.replicas().size()) {
                        under++;
                    }
                    p.replicas().forEach(r -> perBroker.computeIfAbsent(r.id(), k -> new int[2])[1]++);
                }
            }
            List<ClusterHealth.BrokerStatus> brokers = perBroker.entrySet().stream().map(e -> {
                Node n = liveById.get(e.getKey());
                return new ClusterHealth.BrokerStatus(e.getKey(), n == null ? "kafka-" + e.getKey() : n.host() + ":" + n.port(),
                        n != null, e.getValue()[0], e.getValue()[1]);
            }).toList();

            return new ClusterHealth(System.currentTimeMillis(), true, brokers, controller, voters, total, under,
                    offline, lag(topics.get(Topics.TRANSACTIONS_RAW)), null);
        } catch (Exception e) {
            log.warn("Cluster health check failed: {}", e.toString());
            return ClusterHealth.unreachable(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private long lag(TopicDescription raw) {
        if (raw == null) {
            return -1;
        }
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(streamsGroup)
                    .partitionsToOffsetAndMetadata().get(4, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> request = raw.partitions().stream()
                    .collect(Collectors.toMap(p -> new TopicPartition(raw.name(), p.partition()), p -> OffsetSpec.latest()));
            var ends = admin.listOffsets(request).all().get(4, TimeUnit.SECONDS);
            long lag = 0;
            for (var e : ends.entrySet()) {
                OffsetAndMetadata c = committed.get(e.getKey());
                lag += Math.max(0, e.getValue().offset() - (c == null ? 0 : c.offset()));
            }
            return lag;
        } catch (Exception e) {
            return -1;
        }
    }

    @PreDestroy
    void close() {
        admin.close();
    }
}
