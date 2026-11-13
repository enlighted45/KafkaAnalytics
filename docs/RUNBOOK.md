# Runbook

All commands run from the project root.

## Start / stop

```bash
docker compose up -d --build     # build + start everything
docker compose ps                # status (all should be "healthy"; kafka-init "Exited (0)")
docker compose stop              # stop, keep data
docker compose start             # start again
docker compose down              # remove containers, keep volumes (data)
docker compose down -v           # remove containers AND all Kafka / Streams data (full reset)
```

Startup order is enforced by health checks:
kafka-1/2/3 → kafka-init (creates topics) → producer-service, analytics-service, kafka-ui → dashboard.

On a fresh cluster, Kafka Streams needs about 30–40 s to create its internal topics before it reports
`RUNNING`. The dashboard header shows `Streams: REBALANCING` until then.

## Topics

| Topic | Partitions | RF | min ISR | Retention |
|---|---|---|---|---|
| transactions.raw | 6 | 3 | 2 | 1 day |
| transactions.late | 3 | 3 | 2 | 7 days |
| transactions.dlq | 3 | 3 | 2 | 7 days |
| metrics.windowed | 3 | 3 | 2 | 1 hour |
| alerts | 3 | 3 | 2 | 1 day |

Kafka Streams also creates internal changelog topics prefixed `txn-analytics-`, all with RF 3.
`metrics.windowed` carries **per-partition partial** window results. The key is `<size>@<windowStart>@p<partition>`.
Consumers sum the partials with the same size and start to get the global window.

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions.raw
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
```

## Failover demo (watch the dashboard while doing this)

```bash
# which broker leads which partition
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions.raw
docker stop kafka-2     # dashboard: kafka-2 Down, its partitions move to kafka-1/3, metrics keep updating
docker start kafka-2    # replicas catch up, under-replicated returns to 0
```

| Failure | Behaviour |
|---|---|
| 1 broker down | Followers take over leadership. Writes continue (2 in-sync replicas ≥ `min.insync.replicas=2`). No data loss. |
| Active controller down | One of the 2 standby controllers becomes active within seconds. |
| 2 brokers down | Writes are **refused** (`NOT_ENOUGH_REPLICAS`) rather than accepted unsafely. The producer retries for up to 120 s (`delivery.timeout.ms`), then counts errors. Reads of existing data continue. |
| analytics-service restart | State is restored from changelog topics. Processing resumes from committed offsets, with exactly-once semantics (no double counting). |
| producer-service restart | Sequence numbers are seeded from the start time, so they keep increasing and the stream sees no false out-of-order events. |

`./scripts/failover-test.sh` automates all of the above.

## Generator control

From the dashboard ("Event generator" panel), or:

```bash
curl -s localhost:8081/api/generator | python3 -m json.tool
curl -s -X POST localhost:8081/api/generator/stop
curl -s -X POST localhost:8081/api/generator/start
curl -s -X PUT localhost:8081/api/generator/settings -H 'Content-Type: application/json' -d '{
  "ratePerSecond": 200, "accounts": 5000, "lateRatio": 0.03, "maxLatenessSeconds": 90,
  "outOfOrderRatio": 0.02, "duplicateRatio": 0.01, "highValueRatio": 0.005,
  "burstChancePerSecond": 0.05, "burstSize": 12 }'
```

## Logs and troubleshooting

```bash
docker compose logs -f analytics-service
docker logs kafka-1 2>&1 | grep -E 'ERROR|WARN' | tail
```

| Symptom | Check |
|---|---|
| Dashboard "Reconnecting…" | Is `analytics-service` healthy? Check `docker compose ps` and its logs. |
| `Streams: REBALANCING` for a long time | Look for errors in the analytics logs. Check that all topics exist (`kafka-init` exit code 0). |
| Consumer lag growing | The rate is too high for this machine. Lower the generator rate, or add stream threads (`num.stream.threads`). |
| Changing the topology (stores, partitions) | Internal changelog partition counts must match. Reset with `docker compose down -v`, or use a new `spring.kafka.streams.application-id`. |

## Known limits and production notes

- All 3 Kafka nodes run on one host in combined broker+controller mode. Production would use separate
  hosts or racks and dedicated controller nodes.
- No TLS or authentication (PLAINTEXT listeners). Production needs SASL/SSL and ACLs.
- Windows are aggregated per input partition and merged when queried, so throughput scales with
  partitions and stream threads (or more analytics-service instances; interactive queries would then need
  to query the other instances too).
- **Exactly-once uses transactions protocol v1.** `kafka-init` pins the cluster feature
  `transaction.version=1`. With the 4.x default (v2, KIP-890), Kafka Streams' transactional producers hit
  `OUT_OF_ORDER_SEQUENCE_NUMBER` / `CONCURRENT_TRANSACTIONS` retry storms on changelog topics under load. That
  stalled processing for seconds at a time and drove the brokers' CPU up. The exactly-once guarantees are the
  same with v1. Check with `kafka-features.sh --bootstrap-server localhost:9092 describe`.
- Kafka client libraries are pinned to 4.4.0 (the root `pom.xml` property `kafka.version`) to match the brokers.
- **Capacity on this host:** all 3 brokers, both services and the UI share 4 cores and one disk. Measured
  catch-up throughput is about 550 events/s with exactly-once and RF 3. See the load test results in
  `IMPLEMENTATION_PLAN.md`.
