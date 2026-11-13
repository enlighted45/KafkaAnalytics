# Banking Transaction Analytics — real-time streaming with Kafka

Generates banking transactions, processes them with **Kafka Streams** (ordering, late events, duplicates,
windowed aggregation, alerts) and shows near-real-time metrics on a **React** dashboard updated over
**WebSocket (STOMP)**. Kafka runs as a **3-broker KRaft cluster**: every partition has 1 leader + 2 followers,
and the controller quorum has 1 active controller + 2 standbys.

```
producer-service ──► transactions.raw (6 partitions, RF 3, key = accountId)
                         │
                         ▼
analytics-service (Kafka Streams, exactly-once, one task per partition)
  parse/validate ──► transactions.dlq          (bad records)
  dedupe by eventId, per-account sequence check
  classify lateness (per-partition event time) ──► transactions.late   (later than window + grace)
  alerts (high value, velocity) ──► alerts
  10 s + 1 min tumbling windows, 30 s grace, per partition ──► state stores + metrics.windowed
  │  interactive queries merge the partitions, 1 push/s
  ▼
dashboard (React, nginx)  ◄── /ws (STOMP) and /api, proxied by nginx
```

| Part | Tech |
|---|---|
| `txn-common` | Event model, topic names, JSON serdes (Jackson 3) |
| `producer-service` | Spring Boot 4.1, idempotent producer (`acks=all`), REST control of the generator |
| `analytics-service` | Spring Boot 4.1, Kafka Streams 4.4 (`exactly_once_v2`), STOMP WebSocket, Admin API health |
| `dashboard` | React 19, Vite, TypeScript, Recharts, `@stomp/stompjs` |
| Kafka | `apache/kafka:4.4.0` × 3 (broker + controller), Kafka UI (kafbat) |

## Run it (deployment)

Requirements: Docker with Compose. About 4 GB of free RAM.

```bash
docker compose up -d --build
```

| URL | What |
|---|---|
| http://localhost:3000 | Dashboard |
| http://localhost:8080 | Kafka UI (topics, partitions, consumer groups) |
| http://localhost:8081/api/generator | Generator API (producer-service) |
| http://localhost:8090/api/dashboard/snapshot | Analytics snapshot (analytics-service) |
| localhost:19092, :29092, :39092 | Kafka brokers from the host |

Configuration lives in `.env` (ports, generator rate, Kafka heap). See [docs/RUNBOOK.md](docs/RUNBOOK.md)
for operations: failover demo, reset, scaling.

## Develop (IntelliJ / local)

Open the root `pom.xml` in IntelliJ as a Maven project. Start Kafka only:

```bash
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-init kafka-ui
```

Then run `ProducerApplication` and `AnalyticsApplication` from the IDE. Both default to
`localhost:19092,29092,39092`. Then start the dashboard dev server (it proxies `/api` and `/ws`):

```bash
cd dashboard && npm install && npm run dev
```

Open http://localhost:5173.

## Tests

| Level | Command | What it covers |
|---|---|---|
| Backend unit + integration | `./mvnw verify` | Serde, generator, REST, **Kafka Streams topology** (TopologyTestDriver: event-time windows, grace, late drop, dedupe, ordering, alerts, DLQ), KPIs, plus Testcontainers ITs: producer → Kafka, and Kafka → Streams → WebSocket client |
| Frontend | `cd dashboard && npm test` | Reducer, selectors, formatting, components (health, KPIs, alerts, generator controls) |
| System: smoke | `./scripts/smoke-test.sh` | Containers healthy, RF 3 / ISR 3 / min.insync 2 on every topic, leaders spread, controller quorum, data flowing, WebSocket |
| System: failover | `./scripts/failover-test.sh` | Stop the leader broker, stop the active controller, stop 2 of 3 brokers (writes refused); no send errors, no data loss |
| System: accuracy | `./scripts/accuracy-test.sh` | Restart analytics mid-stream; every acked event received **exactly once**, duplicates detected = injected |
| System: load | `./scripts/load-test.sh` (`RATE=500 DURATION=600`) | Sustained rate, p95 latency < 2 s, lag stable |

The `./mvnw verify` integration tests need Docker (Testcontainers).

## How the streaming concerns are handled

- **Ordering:** records are keyed by `accountId`, so each account lives in one partition. The idempotent
  producer prevents reordering on retry. A per-account sequence store flags *out-of-order* events and *gaps*.
- **Event time:** a `TimestampExtractor` uses the transaction's `eventTime`, so windows reflect when
  transactions happened, not when they arrived.
- **Late events:** windows accept late events for 30 s after they end, and the window result is corrected.
  Later events are excluded from the windows and written to `transactions.late`, so nothing is silently lost.
  Lateness is judged against each partition's own event-time clock. Global windows are the merge of the
  per-partition partials. Without this, partitions replayed at different speeds after a restart made on-time
  events look late (measured: 1,501 "late" against 104 actually delayed; now 101 against 104).
- **Duplicates:** a windowed state store of seen `eventId`s (10 min) removes client retries.
- **Exactly-once:** Kafka Streams `exactly_once_v2`. State stores are backed by changelog topics (RF 3), so a
  restart restores state without double counting.
- **Bad data:** the input deserializer never throws. Invalid records go to `transactions.dlq` with their
  original bytes.
- **Live push:** the backend reads the state stores once per second and pushes one message. Browser load stays
  constant regardless of event rate.
