# Kafka Analytics Research Documentation

## Problem and goals

The research question was whether a small Kafka cluster can turn a stream of messy bank transactions into correct, near-real-time metrics on a live dashboard. Messy means events that arrive late, twice, out of order or malformed, which is normal for real payment systems.

| Goal | How it is measured | Target |
| --- | --- | --- |
| Correct numbers despite messy input | `accuracy-test.sh` compares what the generator injected with what the stream detected | Every acknowledged event counted once; duplicates detected = duplicates injected |
| Near-real-time dashboard | `load-test.sh` measures end-to-end latency and consumer lag | p95 latency under 2 s at 500 events/s for 10 min, lag not growing |
| Survive a broker failure | `failover-test.sh` stops brokers and controllers mid-stream | No acknowledged event lost with 1 of 3 brokers down |
| Survive an analytics restart | `accuracy-test.sh` restarts analytics-service mid-run | No double counting (exactly-once) |
| Bad input never stops processing | Topology unit test with malformed records | Invalid records go to a dead-letter topic, stream keeps running |

Scope: one host running Docker Compose, synthetic data from a built-in generator, no TLS or authentication.

## Data-quality problems studied

Six kinds of irregular input were studied. The generator injects each one on purpose and counts what it injected, so the stream's detection can be checked against ground truth.

| Problem | Real-world cause | How the generator simulates it | Default rate | How the stream handles it |
| --- | --- | --- | --- | --- |
| Late event | Offline ATM or terminal syncing later | Event time is "now", delivery delayed 5–90 s | 3 % | Event-time windows with 30 s grace; later events go to `transactions.late` |
| Out-of-order event | Retries and parallel network paths | Two consecutive events of one account sent in swapped order | 2 % | Per-account sequence numbers flag out-of-order and gaps |
| Duplicate | Client retry after a timeout | Same `eventId` resent 0.1–2 s later | 1 % | State store of seen event ids, kept 2 min |
| High-value transaction | Large transfers, possible fraud | Amount between ₹2,00,000 and ₹15,00,000 | 0.5 % | Alert when the INR amount is at least ₹1,00,000 |
| Burst | Card testing or fraud | 12 transactions on one account, 250 ms apart | 5 % chance per second | Velocity alert at the 11th transaction in a 1-minute window |
| Malformed record | Bad client data, schema drift | Not injected; covered by unit tests | n/a | Never-throwing deserializer, record sent to `transactions.dlq` |

The late-event delay range was chosen so some late events land inside the 30 s grace period and some beyond it, which exercises both outcomes.

## Technology options

Kafka Streams was chosen because it gives event-time windows, state and exactly-once processing as a library inside an ordinary Spring Boot service, with no extra cluster to run on one host. The table compares it with the usual alternatives against this project's requirements.

| Option | Event time and late data | Exactly-once | Extra infrastructure | Fit here |
| --- | --- | --- | --- | --- |
| Kafka Streams (chosen) | Event-time windows, grace periods, per-partition stream time | Yes, through Kafka transactions (`exactly_once_v2`) | None: a library in the analytics service | Best: state stores can be queried directly to feed the dashboard |
| Apache Flink | Strongest: watermarks, rich window types | Yes, with checkpoints | A Flink cluster (JobManager and TaskManagers) | Capable, but heavy for a single host |
| Spark Structured Streaming | Watermarks, windowed aggregation | Yes, with checkpoints and idempotent sinks | A Spark cluster | Micro-batch latency, heavy setup |
| ksqlDB | SQL windows over Kafka Streams | Yes | A ksqlDB server | Less control over custom lateness rules and output rate |
| Plain consumer and a database | Must be built by hand | Must be built by hand | A database | Too much custom code for dedupe, windows and recovery |

### Other choices

| Decision | Chosen | Alternative considered | Reason |
| --- | --- | --- | --- |
| Cluster metadata | KRaft, 3 combined broker + controller nodes | ZooKeeper | Kafka 4.x runs without ZooKeeper; combined mode fits one machine |
| Serialization | JSON (Jackson 3) | Avro with Schema Registry | Readable in Kafka UI, no registry to run; Avro noted as a later upgrade |
| Live updates to the browser | STOMP over WebSocket, one push per second | Polling REST, server-sent events | One connection carries metrics, live feed, alerts and health |
| Dashboard | React 19, Vite, TypeScript, Recharts | Server-rendered pages | Fast-updating charts and a typed client model |

## Core concepts researched

These are the ideas the design depends on, each tied to where it appears in the project.

| Concept | What it means | Where it shows up |
| --- | --- | --- |
| Event time vs processing time | When a transaction happened vs when the system saw it | `EventTimeExtractor` makes `eventTime` the record timestamp, so windows follow event time |
| Stream time | The highest event time a task has processed for its partition; it never moves back | Decides whether a window is still open in `LatenessClassifier` |
| Tumbling window | Fixed-size, non-overlapping window aligned to the clock | 10 s windows for live charts, 1 min windows for the summary |
| Grace period | How long after a window ends late events are still accepted | 30 s; accepted late events restate the window's result |
| Partition and key | Records with the same key go to the same partition, in order | Key = `accountId`, 6 partitions on `transactions.raw` |
| Task | One unit of processing per input partition, with its own state | 6 tasks spread over 2 stream threads |
| State store and changelog | Local state backed by a compacted topic so it can be rebuilt | 6 in-memory stores, each with a changelog topic at RF 3 |
| Delivery guarantees | At-most-once, at-least-once, exactly-once | Idempotent producer plus `exactly_once_v2` in Streams |
| Kafka transactions | Output records, state changes and consumed offsets commit together | Committed every 1 s; consumers read with `read_committed` |
| Replication and ISR | Copies of each partition on several brokers; in-sync replicas are caught up | RF 3, `min.insync.replicas=2`, `acks=all` |
| KRaft quorum | Brokers that also act as metadata controllers, one active | 3 voters; a standby takes over if the active one stops |
| Interactive queries | Reading a Streams state store from inside the running app | The dashboard's numbers come straight from the stores |

## Design questions and findings

The most important finding: lateness must be judged per partition. With one shared clock, partitions replayed at different speeds after a restart made on-time events look late; the README records 1,501 events flagged late against 104 actually delayed, and 101 against 104 after the change.

| Question | Finding | Decision | Evidence |
| --- | --- | --- | --- |
| Judge lateness globally or per partition? | A global clock over-counts late events after a restart (1,501 flagged vs 104 delayed) | One sub-topology, per-partition stream time, partials merged at query time (101 vs 104) | README, `TransactionTopology` Javadoc |
| Persistent RocksDB or in-memory stores? | RocksDB caused multi-second stalls: per-commit flushes under exactly-once and a new segment every 60 s per task | In-memory stores with short, eagerly compacted changelogs | `TransactionTopology.CHANGELOG` comment |
| Kafka transactions protocol v2 or v1? | v2 (KIP-890, the 4.x default) gave `OUT_OF_ORDER_SEQUENCE_NUMBER` / `CONCURRENT_TRANSACTIONS` retry storms on changelogs under load | Pin the cluster feature `transaction.version=1`; same guarantees | `infra/kafka/create-topics.sh`, RUNBOOK |
| Default transaction timeout (10 s)? | A broker hand-off or host stall aborted the transaction, fenced the producer and forced a state wipe and restore | `producer.transaction.timeout.ms: 60000` | analytics `application.yml` |
| How long to remember event ids? | Client retries arrive within seconds; longer retention means a bigger changelog and slower restore | 2 min (was 10 min) | analytics `application.yml` |
| How long to keep window changelogs? | The default extra retention is 1 day | `windowstore.changelog.additional.retention.ms: 60000` | analytics `application.yml` |
| Push every update or batch? | Per-event pushes scale browser load with traffic | One metrics push per second; live feed capped at 20 events/s | `DashboardPublisher`, `LiveFeedListener` |
| Sequence numbers after a producer restart? | Restarting at 1 made the stream report false out-of-order events | Seed sequences from the start time | RUNBOOK, `TransactionGenerator` |
| How much can one host handle? | About 550 events/s catch-up throughput with exactly-once and RF 3 on 4 cores and one disk | Default rate 50 events/s; load test at 500 events/s | RUNBOOK |

## Open questions and future research

- **Scaling the dashboard past one instance.** Interactive queries only see local tasks, so two analytics instances would each show about half the data. Options: remote queries through `application.server`, or building the dashboard from `metrics.windowed`.
- **Faster failover.** Standby replicas (`num.standby.replicas`) would keep a warm copy of each store and avoid a full restore.
- **Velocity windows.** Tumbling 1-minute windows miss a burst that straddles a minute boundary; sliding or hopping windows would catch it at a higher state cost.
- **Lateness for the 1-minute windows.** They reuse the 10 s window's lateness decision, so some events are excluded from a 1-minute window that is still open.
- **Transactions v2.** Re-test KIP-890 on a later Kafka release before keeping the v1 pin.
- **Load-test evidence.** The RUNBOOK points to load-test results that are not in the repository; re-run `load-test.sh` and record p95 latency and lag.
- **Schema evolution.** Avro or Protobuf with a schema registry instead of JSON.
- **Production hardening.** Dedicated controller nodes, separate hosts or racks, TLS, SASL and ACLs.

## Sources

All findings come from the project repository.

- `README.md`: design summary and the measured late-event counts
- `IMPLEMENTATION_PLAN.md`: goals, requirements and test plan
- `docs/RUNBOOK.md`: failure behaviour, capacity and the transactions v1 note
- `infra/kafka/create-topics.sh`: topic settings and the `transaction.version` pin
- `analytics-service/src/main/resources/application.yml`: Streams and analytics settings with their reasons
- `analytics-service/.../stream/TransactionTopology.java`: topology and the in-memory store decision
- `producer-service/src/main/resources/application.yml` and `TransactionGenerator.java`: producer settings and injected irregularities
