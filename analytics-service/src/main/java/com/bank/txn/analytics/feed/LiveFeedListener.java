package com.bank.txn.analytics.feed;

import com.bank.txn.common.Topics;
import com.bank.txn.common.model.TransactionEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/** Samples the raw stream (max N/s) for the live transaction table. Own consumer group per instance. */
@Component
public class LiveFeedListener {
    private final SimpMessagingTemplate ws;
    private final RateLimiter limiter;
    private final RecentBuffer<TransactionEvent> recent = new RecentBuffer<>(50);

    public LiveFeedListener(SimpMessagingTemplate ws,
                            @org.springframework.beans.factory.annotation.Value("${analytics.live-feed-max-per-second:20}") int maxPerSecond) {
        this.ws = ws;
        this.limiter = new RateLimiter(maxPerSecond, System::nanoTime);
    }

    @KafkaListener(id = "live-feed", topics = Topics.TRANSACTIONS_RAW, groupId = "dashboard-feed-${HOSTNAME:local}",
            properties = {"auto.offset.reset=latest"})
    public void onEvent(TransactionEvent event) {
        if (event != null && limiter.tryAcquire()) {
            recent.add(event);
            ws.convertAndSend("/topic/transactions", event);
        }
    }

    public List<TransactionEvent> recent() {
        return recent.snapshot();
    }
}
