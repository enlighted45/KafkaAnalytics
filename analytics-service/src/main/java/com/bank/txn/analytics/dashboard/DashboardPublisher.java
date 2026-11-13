package com.bank.txn.analytics.dashboard;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Pushes one metrics update per second. Batching on a fixed tick (instead of pushing every state change)
 * keeps browser load constant regardless of the event rate.
 */
@Component
public class DashboardPublisher {
    private final MetricsQueryService metrics;
    private final SimpMessagingTemplate ws;

    public DashboardPublisher(MetricsQueryService metrics, SimpMessagingTemplate ws) {
        this.metrics = metrics;
        this.ws = ws;
    }

    @Scheduled(fixedRateString = "${analytics.push-interval-ms:1000}")
    public void publish() {
        ws.convertAndSend("/topic/metrics", metrics.current());
    }
}
