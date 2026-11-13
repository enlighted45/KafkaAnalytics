package com.bank.txn.analytics.api;

import com.bank.txn.analytics.dashboard.DashboardUpdate;
import com.bank.txn.analytics.dashboard.MetricsQueryService;
import com.bank.txn.analytics.feed.AlertListener;
import com.bank.txn.analytics.feed.LiveFeedListener;
import com.bank.txn.analytics.health.ClusterHealth;
import com.bank.txn.analytics.health.ClusterHealthService;
import com.bank.txn.analytics.model.Alert;
import com.bank.txn.common.model.TransactionEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Initial state for a freshly opened dashboard; WebSocket keeps it live afterwards. */
@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {

    public record Snapshot(DashboardUpdate metrics, ClusterHealth health, List<Alert> alerts,
                           List<TransactionEvent> transactions) {
    }

    private final MetricsQueryService metrics;
    private final ClusterHealthService health;
    private final AlertListener alerts;
    private final LiveFeedListener feed;

    public DashboardController(MetricsQueryService metrics, ClusterHealthService health, AlertListener alerts,
                               LiveFeedListener feed) {
        this.metrics = metrics;
        this.health = health;
        this.alerts = alerts;
        this.feed = feed;
    }

    @GetMapping("/snapshot")
    public Snapshot snapshot() {
        return new Snapshot(metrics.current(), health.latest(), alerts.recent(), feed.recent());
    }
}
