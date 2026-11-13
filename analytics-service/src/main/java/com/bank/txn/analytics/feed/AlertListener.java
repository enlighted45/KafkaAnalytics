package com.bank.txn.analytics.feed;

import com.bank.txn.analytics.model.Alert;
import com.bank.txn.common.Topics;
import com.bank.txn.common.serde.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Forwards alerts to the dashboard. On startup re-reads the last few alerts per partition. */
@Component
public class AlertListener implements ConsumerSeekAware {
    private static final JsonDeserializer<Alert> DESERIALIZER = new JsonDeserializer<>(Alert.class);
    private final SimpMessagingTemplate ws;
    private final RecentBuffer<Alert> recent = new RecentBuffer<>(100);

    public AlertListener(SimpMessagingTemplate ws) {
        this.ws = ws;
    }

    @KafkaListener(id = "alerts-feed", topics = Topics.ALERTS, groupId = "dashboard-alerts-${HOSTNAME:local}",
            properties = {"value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer"})
    public void onAlert(ConsumerRecord<String, byte[]> record) {
        try {
            Alert alert = DESERIALIZER.deserialize(record.topic(), record.value());
            if (alert != null) {
                recent.add(alert);
                ws.convertAndSend("/topic/alerts", alert);
            }
        } catch (RuntimeException e) {
            org.slf4j.LoggerFactory.getLogger(AlertListener.class)
                    .warn("Skipping unreadable alert: {}", new String(record.value(), StandardCharsets.UTF_8));
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().forEach(tp -> callback.seekRelative(tp.topic(), tp.partition(), -40, false));
    }

    public List<Alert> recent() {
        return recent.snapshot();
    }
}
