# Real-Time Banking Transaction Analytics — Implementation Plan

**Stack:** React (Vite + TypeScript) · Java 21 · Spring Boot · Apache Kafka (KRaft, **3 brokers**) · Kafka Streams · STOMP over WebSocket · Docker Compose

**Goal:** Generate banking transaction events, process the stream (ordering, late events, duplicates, windowed aggregation), and show near-real-time metrics on a live dashboard.

---

## 1. Architecture

```
 ┌──────────────────┐   transactions.raw (6 partitions, key=accountId)
 │ producer-service │ ─────────────────────────────────────────────┐   (RF=3, acks=all)
 │ (Spring Boot)    │                                              ▼
 │ - event generator│   ┌──────────────────── Kafka cluster (KRaft, 3 nodes) ────────────────────┐
 │ - chaos knobs    │   │  kafka-1 (broker+controller)  kafka-2 (broker+controller)  kafka-3 (…)  │
 │ - REST control   │   │  every partition: 1 leader + 2 followers, leaders spread over brokers   │
 └──────────────────┘   │  controller quorum: 1 active controller (leader) + 2 standby followers  │
                        └─────────────────────────────────────────────────────────────────────────┘
          ▲                                              │
          │ REST (/api/generator/*)                      ▼
          │                         ┌──────────────────────────────────────┐
          │                         │ analytics-service (Spring Boot)      │
          │                         │ - Kafka Streams topology             │
          │                         │   dedupe → order check → windows     │
          │                         │ - late events → transactions.late    │
          │                         │ - metrics → metrics.windowed         │
          │                         │ - WebSocket (STOMP) broadcaster      │
          │                         │ - REST snapshot (interactive queries)│
          │                         └──────────────────────────────────────┘
          │                                              │ ws://.../ws  (STOMP)
          │                                              ▼
          │                         ┌──────────────────────────────────────┐
          └──────────────────────── │ dashboard (React, served by nginx)   │
                                    │ nginx proxies /api and /ws           │
                                    └──────────────────────────────────────┘
```

### Kafka cluster (3 brokers: 1 leader + 2 followers)
In Kafka, "leader" and "follower" apply at two levels, and this cluster has both:

| Level | Leader | Followers |
|---|---|---|
| **Partition replication** (data) | Each partition has 1 leader replica that serves reads/writes | 2 follower replicas copy it (RF=3). If the leader's broker dies, a follower is elected leader automatically. |
| **KRaft controller quorum** (metadata) | 1 active controller | 2 standby controllers replicate the metadata log and take over if the active one dies. |

| Node | Roles | Ports (host) |
|---|---|---|
| `kafka-1` | broker + controller | 19092 |
| `kafka-2` | broker + controller | 29092 |
| `kafka-3` | broker + controller | 39092 |

Partition leaders are **spread across all 3 brokers** (Kafka balances them), so load is shared rather than one broker doing all the work. The dashboard health strip will show, per broker, how many partitions it currently leads, and which node is the active controller.

Broker settings:
- `default.replication.factor=3`, `offsets.topic.replication.factor=3`, `transaction.state.log.replication.factor=3`
- `min.insync.replicas=2`, `transaction.state.log.min.isr=2`, producer `acks=all`
  → every acknowledged write is stored on at least 2 brokers (no data loss on a single broker failure), and the cluster keeps accepting writes with 1 broker down.
- `unclean.leader.election.enable=false` (an out-of-date replica never becomes leader).
- Nodes run in combined broker+controller mode to fit on one machine; production would use dedicated controller nodes.

### Topics
| Topic | Partitions | RF | Key | Purpose |
|---|---|---|---|---|
| `transactions.raw` | 6 | 3 | `accountId` | All generated events |
| `transactions.late` | 3 | 3 | `accountId` | Events that arrived after window + grace (kept for audit, not lost) |
| `transactions.dlq` | 3 | 3 | — | Malformed / undeserializable records |
| `metrics.windowed` | 3 | 3 | window key | Aggregated window results |
| `alerts` | 3 | 3 | `accountId` | High-value / velocity alerts |

Topics are created by a one-shot `kafka-init` container (explicit config, not auto-create).

---

## 2. Event model

```json
{
  "eventId": "uuid",
  "accountId": "ACC-000123",
  "sequenceNo": 4182,                 // per-account, monotonically increasing
  "type": "DEPOSIT | WITHDRAWAL | TRANSFER | PAYMENT",
  "channel": "ATM | POS | ONLINE | MOBILE | BRANCH",
  "status": "APPROVED | DECLINED",
  "amount": 249.90,                   // BigDecimal, serialized as string-safe number
  "currency": "INR | USD | EUR | GBP",
  "merchantCategory": "GROCERY | TRAVEL | ...",
  "region": "NORTH | SOUTH | EAST | WEST",
  "eventTime": "2026-11-12T10:15:30.123Z",   // when the transaction happened
  "producedAt": "2026-11-12T10:15:31.004Z"   // when it was sent to Kafka
}
```
JSON serialization (simple, readable in Kafka UI). Avro + Schema Registry is a possible later upgrade.

---

## 3. How each streaming concern is handled

| Concern | Approach |
|---|---|
| **Event ordering** | Key = `accountId`, so all events of one account land in one partition and stay ordered. Producer is idempotent (`enable.idempotence=true`, `acks=all`, `max.in.flight=5`) so retries don't reorder or duplicate. The stream keeps the last `sequenceNo` per account in a state store and counts **out-of-order** events and **sequence gaps**. |
| **Late events** | Windows use **event time** (custom `TimestampExtractor` reads `eventTime`), not arrival time. Tumbling windows of **10 s** (live chart) and **1 min** (summary), **grace period 30 s**. Late-but-within-grace events correct the window (dashboard shows the restated value). Events later than grace are routed to `transactions.late` and counted as "late dropped". |
| **Duplicates** | Generator can inject duplicates. Stream dedupes by `eventId` using a windowed state store (10-min retention) before aggregation. |
| **Aggregation** | Per window: count, total volume, average ticket, approval rate, breakdown by type / channel / region / currency, TPS. Plus 1-min top accounts by volume. |
| **Alerts** | High-value (amount > configurable threshold) and velocity (> N transactions per account per minute) → `alerts` topic → dashboard alert feed. |
| **Exactly-once** | Kafka Streams `processing.guarantee=exactly_once_v2`. |
| **Bad data** | Deserialization errors → `transactions.dlq` via a custom exception handler, stream keeps running. |

### Generator chaos knobs (producer-service)
Configurable at runtime from the dashboard / REST:
- `ratePerSecond` (default 50, up to ~2000)
- `lateRatio` (e.g. 5%) and `maxLatenessSeconds` (e.g. 90 → some land within grace, some beyond)
- `outOfOrderRatio` (swap sequence order for some account events)
- `duplicateRatio` (resend same `eventId`)
- `highValueRatio` (to trigger alerts)

The generator also exposes counters of what it injected, so tests can check the dashboard numbers against ground truth.

---

## 4. Live updates (WebSocket)
- Spring WebSocket + STOMP endpoint `/ws`.
- Destinations:
  - `/topic/metrics` — latest window aggregates, **batched and pushed once per second** (avoids flooding the browser at high TPS).
  - `/topic/transactions` — sampled live feed (max ~20 events/s).
  - `/topic/alerts` — every alert.
  - `/topic/health` — brokers online, consumer lag, end-to-end latency p50/p95 (`eventTime` → push time).
- REST `GET /api/metrics/snapshot` (Kafka Streams interactive query) so a freshly opened dashboard is filled immediately, then WebSocket keeps it live.
- Client auto-reconnects (`@stomp/stompjs` with heartbeat + reconnect delay).

---

## 5. Dashboard (React)
- **KPI tiles:** TPS, volume (last 1 min), avg ticket, approval rate, late (corrected / dropped), out-of-order, duplicates removed, active alerts.
- **Charts (Recharts):** volume & count per 10 s window (line, last 5 min), by transaction type (bar), by channel (bar), by region.
- **Live transaction table** (sampled stream, status colored).
- **Alerts panel.**
- **Pipeline health strip:** broker status (kafka-1 / 2 / 3), partition leaders per broker, active controller, consumer lag, e2e latency, WebSocket connection state.
- **Generator controls:** start/stop, rate slider, chaos ratios.
- Light/dark theme, responsive.

---

## 6. Project layout
```
Kafka Analytics/
├── docker-compose.yml          # kafka-1, kafka-2, kafka-3, kafka-init, kafka-ui, services, dashboard
├── .env                        # versions, ports, rates
├── infra/kafka/create-topics.sh
├── producer-service/           # Spring Boot (Maven wrapper, Java 21)
├── analytics-service/          # Spring Boot + Kafka Streams + WebSocket
├── dashboard/                  # React + Vite + TypeScript, nginx Dockerfile
├── scripts/
│   ├── smoke-test.sh           # stack up → data flows → dashboard receives metrics
│   ├── failover-test.sh        # stop the leader broker mid-stream, verify continuity, restart
│   └── load-test.sh            # sustained rate, measure latency/lag
└── docs/                       # architecture notes, runbook
```
Maven isn't installed locally, so each service ships a **Maven wrapper** and builds inside a **multi-stage Dockerfile** (no local Maven needed).

---

## 7. Test plan

### 7.1 Unit tests (JUnit 5, fast, no Docker)
| Area | What is checked |
|---|---|
| Generator | Per-account sequence numbers increase; ratios of late / out-of-order / duplicate events match config (statistically); amounts valid. |
| Serialization | Event ↔ JSON round-trip, `BigDecimal` and timestamps preserved; malformed JSON goes to DLQ handler. |
| **Topology (`TopologyTestDriver`)** — the most important tests | • Events land in the right 10 s / 1 min window by **event time**, not arrival time. <br>• Late event **within** grace updates the old window. <br>• Late event **beyond** grace is excluded and written to `transactions.late`. <br>• Duplicate `eventId` counted once. <br>• Out-of-order sequence detected and counted; gaps detected. <br>• Aggregates (sum, avg, approval rate, breakdowns) exactly correct for a known input set. <br>• High-value and velocity alerts fire at the right thresholds. |
| WebSocket batcher | Many updates within 1 s → one push with the latest values. |

### 7.2 Integration tests (Testcontainers — real Kafka in Docker)
- producer-service → real Kafka: events keyed by `accountId`, idempotent producer config applied.
- analytics-service end-to-end: produce known events → Streams processes → a test **STOMP client** subscribed to `/topic/metrics` receives the expected aggregates.
- REST snapshot endpoint returns current state.

### 7.3 Frontend tests (Vitest + React Testing Library)
- Metrics reducer / store merges window updates and restatements correctly.
- KPI tiles, charts, alert panel render from mocked messages.
- Connection-state indicator reacts to disconnect/reconnect (mocked STOMP client).

### 7.4 System tests on the full Docker Compose stack (3 brokers)
| Test | Pass criteria |
|---|---|
| **Smoke** | All containers healthy; topics exist with RF=3; dashboard shows rising TPS within 15 s. |
| **Replication check** | `kafka-topics --describe` shows every partition with 3 replicas, ISR = 3, leaders spread over brokers 1/2/3. |
| **Partition-leader failover** | Stop the broker leading the most partitions while generating → followers are elected leader, producer keeps sending (retries only), dashboard keeps updating, no acknowledged event lost. Restart it → ISR back to 3, preferred leaders rebalance. |
| **Controller failover** | Stop the active controller → one of the 2 standby controllers becomes active; topic metadata still works. |
| **Two brokers down** | Stop 2 of 3 → writes are refused (`NotEnoughReplicas`, by design with `min.insync.replicas=2`); restart one → writes resume with no data loss. |
| **Analytics restart** | Restart analytics-service → resumes from committed offsets / state store, totals continue correctly (EOS: no double counting). |
| **Late / ordering accuracy** | Dashboard "late dropped", "out-of-order", "duplicates removed" match generator's injected counters (within the expected tolerance for events still in flight). |
| **Load** | 500 events/s for 10 min: end-to-end latency p95 < 2 s, consumer lag stable (not growing), no errors. |
| **Browser E2E (Playwright)** | Open dashboard, see KPIs update, change rate from the UI, see TPS follow. |

All tests must be green before deployment.

---

## 8. Deployment
- **Target (assumed): Docker Compose on this machine** (4 CPU / 15 GB RAM is enough). Can be moved to a cloud VM with the same compose file; Kubernetes (Strimzi) would be a separate step.
- Services: `kafka-1`, `kafka-2`, `kafka-3`, `kafka-init`, `kafka-ui`, `producer-service`, `analytics-service`, `dashboard`.
- Health checks + `depends_on: condition: service_healthy`, `restart: unless-stopped`, named volumes for Kafka data and Streams state.
- Ports: dashboard `3000`, Kafka UI `8080`, Kafka external listeners `19092` / `29092` / `39092` (for local tools).
- Config via `.env`; Spring profiles `local` / `docker`.
- Runbook in `docs/`: start/stop, failover demo, reset data.

---

## 9. Build order (milestones)
1. **Kafka cluster** — compose file with 3 brokers (combined broker+controller) + topic init + Kafka UI; verify RF=3, leaders spread.
2. **producer-service** — event model, generator, chaos knobs, REST control + unit & integration tests.
3. **analytics-service** — Streams topology (dedupe, ordering, windows, late, alerts) + `TopologyTestDriver` tests; then WebSocket/REST + integration tests.
4. **dashboard** — React UI, STOMP client, charts, controls + Vitest tests.
5. **Containerize** — Dockerfiles, nginx proxy, full compose stack.
6. **System testing** — smoke, failover, restart, accuracy, load, Playwright.
7. **Deploy** — `docker compose up -d`, verify, write runbook.

## 10. Versions (pinned at build time, latest stable)
Java 21 · Spring Boot 4.1.x (Spring Kafka, Kafka Streams) · Apache Kafka 4.4.0 image (`apache/kafka`, KRaft) · React 19 · Vite · TypeScript · Recharts · `@stomp/stompjs` · Testcontainers · Vitest · Playwright.
