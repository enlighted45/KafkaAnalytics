# Kafka Analytics Lessons Learned

## Summary

Every lesson here is backed by something in the repository: a config comment, a pinned setting, a test or a note in `docs/RUNBOOK.md`. Where the code and the original plan differ, the difference itself is the lesson.

| # | Lesson | Where it is recorded |
| --- | --- | --- |
| 1 | Exactly-once is reliable only once the transaction settings match the cluster. Transactions protocol v2 caused retry storms, and the 10 s transaction timeout was too short. | `create-topics.sh` pins `transaction.version=1`. `application.yml` sets `producer.transaction.timeout.ms: 60000`. |
| 2 | Restore time is a design input. Small changelogs mean fast recovery. | `windowstore.changelog.additional.retention.ms: 60000` and `dedupe-retention: 2m`, both with comments. |
| 3 | Stream time is per partition. Global windows come from merging per-partition partials when they are queried. | `TransactionTopology` Javadoc, `WindowAgg.merge`, `MergeTest`. |
| 4 | Refusing a write is correct behaviour, not an outage. With 2 of 3 brokers down, `acks=all` plus min ISR 2 rejects writes rather than risk losing data. | RUNBOOK failure table, `failover-test.sh`. |
| 5 | The plan and the docs drift away from the code unless someone keeps them in step. | `docs/observations.md` lists 8 mismatches. |

## Kafka and Kafka Streams lessons

### 1. Transactions protocol v2 caused retry storms

|  |  |
| --- | --- |
| **Problem** | Kafka 4.x defaults to transactions protocol v2 (KIP-890). Under load, the Streams transactional producers hit `OUT_OF_ORDER_SEQUENCE_NUMBER` and `CONCURRENT_TRANSACTIONS` retry storms on changelog topics. Processing stalled for seconds at a time and broker CPU went up. |
| **Fix** | `kafka-init` pins the cluster feature `transaction.version=1`. The exactly-once guarantees are the same with v1. |
| **Evidence** | `infra/kafka/create-topics.sh`, and "Known limits" in `docs/RUNBOOK.md`. |
| **Takeaway** | A new default in the broker can break a client feature. Pin it, write down why, and check it with `kafka-features.sh describe`. |

### 2. The default exactly-once transaction timeout was too short

|  |  |
| --- | --- |
| **Problem** | With the 10 s default, a broker hand-off or a stall of a few seconds aborts the transaction. That fences the producer and forces a full state wipe and restore, which takes minutes. |
| **Fix** | `producer.transaction.timeout.ms: 60000`. |
| **Evidence** | The comment above that line in `analytics-service/application.yml`. |
| **Takeaway** | With exactly-once, a timeout is not only about latency. It decides whether a short hiccup becomes a long recovery. |

### 3. Changelog size drives recovery time

|  |  |
| --- | --- |
| **Problem** | The stores are in memory, so every restart rebuilds them from their changelog topics. The default extra retention for window store changelogs is 1 day, and the first dedupe design kept ids for 10 minutes. |
| **Fix** | Extra changelog retention set to 60 s. Dedupe retention cut to 2 minutes, because client retries arrive within seconds. |
| **Evidence** | Comments in `analytics-service/application.yml`. `TransactionTopology.Config.defaults()` still has the old 10 min value for tests. |
| **Takeaway** | Size each store's retention for the job it does, not for safety margin. |

### 4. Stream time is per partition

|  |  |
| --- | --- |
| **Problem** | The first design assumed a repartition to one partition so stream time would be global. That would have made one task a bottleneck. |
| **Fix** | One sub-topology with 6 tasks. Each task aggregates its own partition. `MetricsQueryService` merges the partial windows when it reads them, and `metrics.windowed` keys carry `@p<partition>`. |
| **Evidence** | `TransactionTopology` Javadoc and `MergeTest`. The old wording is still in the `LatenessClassifier` Javadoc, as `docs/observations.md` notes. |
| **Takeaway** | Lateness is judged against the stream time of that partition. A quiet partition holds its windows open longer. |

### 5. Firing an alert exactly once needs caching off

|  |  |
| --- | --- |
| **Problem** | A windowed `count()` with the record cache on merges updates, so the stream might never show the exact moment the count crosses the threshold, or might show it twice. |
| **Fix** | Caching is off for the velocity store, and the filter fires only when `count == threshold + 1` (11). |
| **Evidence** | `TransactionTopology`, the `velocity-exceeded` node, and `TransactionTopologyTest`. |
| **Takeaway** | When downstream logic depends on seeing every update, turn caching off for that store. |

### 6. Bad records should not crash the stream

|  |  |
| --- | --- |
| **Problem** | The plan used a deserialization exception handler for the DLQ. That handler only lets you skip or fail a record, not route it. |
| **Fix** | `ParseResultSerde` never throws. A `split()` sends failed parses to `transactions.dlq` with the original bytes. |
| **Evidence** | `TransactionTopology` (the `parse-` split) and the DLQ case in `TransactionTopologyTest`. |
| **Takeaway** | Model failure as data, so the topology can route it like any other record. |

### 7. Other small lessons

- **Producer restarts can fake out-of-order events.** Sequence numbers are seeded from the start time, so they keep increasing after a restart (RUNBOOK failure table).
- **Name every topology node.** `Named.as(...)` keeps the names of internal topics and stores stable. Even so, any change to stores or partitions needs `down -v` or a new application id.
- **Pin the client version to the broker version.** `kafka.version` is 4.4.0 in the root `pom.xml`, even though Spring Boot manages its own Kafka version.
- **Use `read_committed` everywhere downstream of exactly-once.** The feed and alert listeners would otherwise show records from aborted transactions.

## Design lessons

### Choices that worked

| Choice | Why it worked |
| --- | --- |
| The dashboard reads the state stores directly (interactive queries). | No extra consumer and no second copy of the data. One code path computes the metrics. |
| A fixed 1 s push instead of a push per event. | The browser load is constant whatever the event rate. The live feed has its own limit of 20 per second (`RateLimiter`). |
| Key = `accountId` on `transactions.raw`. | All events for one account stay ordered on one partition. That makes the sequence check, dedupe and velocity count local to one task. |
| `acks=all`, idempotence, RF 3, min ISR 2. | Survives 1 broker down with no data loss, and refuses writes when it cannot be safe. |
| A generator with tunable chaos (late, out of order, duplicate, burst). | Every streaming edge case can be reproduced on demand, and `accuracy-test.sh` can check duplicates detected = duplicates injected. |
| All settings in `application.yml` and `.env` with comments. | The reason for each value sits next to the value. |

### Choices with trade-offs

| Choice | Trade-off |
| --- | --- |
| Local-only interactive queries | With more than one analytics-service instance, each would only report its own partitions. Scaling out needs remote queries, or consuming `metrics.windowed`. |
| Tumbling window for velocity | A burst that crosses a window boundary can stay under the threshold in both windows and raise no alert. |
| One lateness check for both window sizes | `LatenessClassifier` uses the 10 s window plus grace, so an event too late for its 10 s window is also left out of the 1 min window. |
| High-value check before the accepted filter | Declined events and events that were too late can still raise high-value alerts. |
| DLQ not counted | Records sent to `transactions.dlq` do not appear in the dashboard totals. |
| Everything on one host | All 3 brokers share 4 cores and one disk. Measured catch-up capacity is about 550 events/s with exactly-once and RF 3. |

## Testing and process lessons

- **`TopologyTestDriver` is the most valuable test.** It runs the whole topology in memory in milliseconds and lets the test set event time exactly. Windows, grace, late drops, dedupe, ordering, alerts and the DLQ are all covered in `TransactionTopologyTest` without a broker.
- **Test at three levels, because each catches different bugs.** Unit tests check logic. Testcontainers tests (`ProducerKafkaIT`, `AnalyticsIT`) check the wiring against a real Kafka. The system scripts check behaviour that only shows up with 3 brokers: leader failover, controller failover and exactly-once across a restart.
- **Write the expected failure behaviour down first.** The RUNBOOK failure table (1 broker down, controller down, 2 brokers down, restarts) became the script `failover-test.sh`.
- **Count, don't eyeball.** `accuracy-test.sh` compares acknowledged sends with counted events, which is the only real proof of exactly-once.
- **Docs drift.** `docs/observations.md` found 8 places where the plan or comments disagree with the code. Examples: the endpoint name, Playwright tests that were never written, Spring profiles replaced by environment variables, and load-test results that were never recorded.
- **Unused code piles up quietly.** The same review found 9 unused items, for example `summaryWindows`, which is computed and pushed every second but never shown.

## What I would do differently next time

1. **Record measurements as I take them.** Put the load-test numbers (rate, p95 latency, lag) into the repo straight after each run, so the claim of about 550 events/s has a source.
2. **Update the plan when the design changes.** The repartition idea, the exception-handler DLQ and the REST path all changed without the plan following.
3. **Use a sliding or hopping window for velocity**, so bursts across a window boundary are caught.
4. **Judge lateness per window size**, so the 1 min window accepts what it still can.
5. **Count DLQ records** in `PipelineTotals` and show them on the dashboard.
6. **Add standby replicas** (`num.standby.replicas`), so a warm copy of each store survives a restart and failover skips the full changelog restore. Persistent RocksDB stores were tried and dropped because they caused multi-second stalls under exactly-once (the `CHANGELOG` comment in `TransactionTopology`).
7. **Plan for scale-out early.** Either add remote interactive queries, or have the dashboard consume `metrics.windowed`.
8. **Production hardening:** brokers on separate hosts with dedicated controllers, and TLS, SASL and ACLs instead of PLAINTEXT.
9. **Drive alert text from configuration**, instead of hard-coding "within 1 minute" and "₹1,00,000".
