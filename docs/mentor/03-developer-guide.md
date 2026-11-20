# Kafka Analytics Developer Guide

## 1. Tech stack

Every version below is pinned in the repo (root `pom.xml`, `dashboard/package.json`, the Dockerfiles, `docker-compose.yml` and `.env`).

### Backend

| Technology | Version | Role in this project |
| --- | --- | --- |
| Java (Eclipse Temurin) | 21 | Language and runtime for all three Maven modules. Records are used for events, settings and DTOs. |
| Spring Boot | 4.1.2 | Parent POM and application framework for both services. |
| Spring Web MVC | from Boot 4.1.2 | REST APIs: `/api/generator` (producer) and `/api/dashboard/snapshot` (analytics). |
| Spring for Apache Kafka | from Boot 4.1.2 | Producer `KafkaTemplate`, `@KafkaListener` consumers, and the `StreamsBuilderFactoryBean` that runs Kafka Streams. |
| Spring WebSocket (STOMP) | from Boot 4.1.2 | Pushes a dashboard update to `/topic/metrics` every second over `/ws`. |
| Spring Boot Actuator | from Boot 4.1.2 | Health, liveness and readiness probes used by Docker health checks. |
| Bean Validation | from Boot 4.1.2 | Validates generator settings, for example a rate from 0 to 5,000 per second. |
| Apache Kafka clients and Kafka Streams | 4.4.0 | Pinned by the `kafka.version` property so the clients match the brokers. |
| Jackson | from Boot 4.1.2 | JSON serdes in `txn-common`. |
| Maven (wrapper) | 3.9.16 | Build tool. Use `./mvnw`, so no local Maven install is needed. |

### Messaging and infrastructure

| Technology | Version | Role in this project |
| --- | --- | --- |
| Apache Kafka (`apache/kafka` image) | 4.4.0 | 3 nodes in KRaft mode. Each node is both broker and controller. No ZooKeeper. |
| Kafka UI (`kafbat/kafka-ui`) | v1.5.0 | Web UI on port 8080 for browsing topics, partitions and consumer groups. |
| Docker and Docker Compose | Compose v2 | Runs the 3 brokers, the topic init job, both services, the UI and the dashboard. |
| nginx | 1.29-alpine | Serves the built dashboard and proxies `/api` and `/ws` to analytics-service. |

### Frontend

| Technology | Version | Role in this project |
| --- | --- | --- |
| Node.js | 22 (alpine build image) | Builds the dashboard. |
| React | 19.3 | UI. State lives in a `useReducer` reducer, `dashboardReducer.ts`. |
| TypeScript | 7.0 | Types for the update payload (`types.ts`) and all components. |
| Vite | 8.3 | Dev server on port 5173 (proxies `/api` and `/ws`) and production bundler. |
| @stomp/stompjs | 7.5 | STOMP client for the WebSocket feed, with automatic reconnect. |
| Recharts | 3.10 | Volume, window-count and breakdown charts. |
| Vitest, Testing Library, jsdom | 5.1, 16.3, 31 | Frontend unit and component tests. |
| oxlint | 1.90 | Linter (`npm run lint`). |

### Testing

| Technology | Role |
| --- | --- |
| JUnit 5 and AssertJ | Unit tests in every module. |
| `kafka-streams-test-utils` (`TopologyTestDriver`) | Runs the full topology in memory, with no broker, and controls event time. |
| Testcontainers (Kafka module) | Starts a real `apache/kafka:4.4.0` container for the `*IT` tests. |
| Maven Surefire and Failsafe | `*Test` classes run in `test`. `*IT` classes run in `verify`. |
| Bash and Node scripts in `scripts/` | System tests against the running Compose stack: smoke, failover, accuracy and load. |

## 2. Repository layout

```
Kafka Analytics/
├── pom.xml                  parent POM: Java 21, Spring Boot 4.1.2, kafka.version 4.4.0
├── mvnw, .mvn/wrapper/      Maven wrapper (3.9.16)
├── .env                     ports, Kafka image and heap, generator defaults
├── docker-compose.yml       3 brokers + kafka-init + kafka-ui + 2 services + dashboard
├── infra/kafka/
│   └── create-topics.sh     creates the 5 topics (RF 3, min ISR 2), pins transaction.version=1
├── txn-common/              shared library, no Spring
│   ├── Topics.java          topic name constants
│   ├── model/               TransactionEvent + enums (Channel, Currency, Region, Status, Type)
│   └── serde/               JSON serializer, deserializer and Serde
├── producer-service/        port 8081, synthetic transaction generator
│   ├── api/                 GeneratorController, GeneratorStatus
│   ├── generator/           TransactionGenerator, GeneratorService, settings, stats, injections
│   └── kafka/               KafkaEventSender (idempotent, acks=all)
├── analytics-service/       port 8090, Kafka Streams + dashboard backend
│   ├── stream/              TransactionTopology and its processors
│   ├── config/              StreamsTopologyConfig, WebSocketConfig, AnalyticsProperties
│   ├── dashboard/           MetricsQueryService, KpiCalculator, DashboardPublisher
│   ├── feed/                LiveFeedListener, AlertListener, RateLimiter, RecentBuffer
│   ├── health/              ClusterHealthService (AdminClient)
│   ├── model/               WindowAgg, Bucket, PipelineTotals, Alert, WindowMetrics
│   └── api/                 DashboardController (REST snapshot)
├── dashboard/               React 19 + Vite, served by nginx on port 3000
│   └── src/                 components/, state/, lib/, types.ts, test/
├── scripts/                 system tests: smoke, failover, accuracy, load, ws-check.mjs
├── docs/                    RUNBOOK.md and per-module docs
├── README.md
└── IMPLEMENTATION_PLAN.md   original design and test plan
```

The root `pom.xml` is a multi-module build: `txn-common` is built first because both services depend on it.

## 3. Setup

### Prerequisites

| Tool | Needed for | Notes |
| --- | --- | --- |
| Docker with Compose v2 | Everything | About 4 GB of free RAM. Each broker gets a 512 MB heap (`KAFKA_HEAP`). |
| JDK 21 | Running the services from an IDE | Not needed for the Docker-only path: the images build with `eclipse-temurin:21-jdk`. |
| Node.js 22 and npm | The dashboard dev server and frontend tests | Not needed for the Docker-only path. |
| Maven | Nothing | Use the wrapper, `./mvnw`. |
| python3 and curl | The system test scripts | Used to read JSON from the APIs. |

### First run

```bash
git clone <repo> && cd "Kafka Analytics"
docker compose up -d --build
docker compose ps          # all healthy, kafka-init "Exited (0)"
```

Open the dashboard at http://localhost:3000. On a fresh cluster Kafka Streams takes about 30–40 s to create its internal topics. Until then the header shows `Streams: REBALANCING`.

| URL | What |
| --- | --- |
| http://localhost:3000 | Dashboard |
| http://localhost:8080 | Kafka UI |
| http://localhost:8081/api/generator | Generator API (producer-service) |
| http://localhost:8090/api/dashboard/snapshot | Analytics snapshot (analytics-service) |
| localhost:19092, 29092, 39092 | Kafka brokers from the host |

## 4. Build and run

### Option A: everything in Docker

```bash
docker compose up -d --build     # build + start
docker compose stop              # stop, keep data
docker compose start             # start again
docker compose down              # remove containers, keep volumes
docker compose down -v           # full reset: also deletes Kafka and Streams data
```

Health checks enforce the startup order: kafka-1/2/3 → kafka-init (creates topics) → producer-service, analytics-service and kafka-ui → dashboard.

### Option B: Kafka in Docker, services in the IDE

This is the fastest loop for backend work.

```bash
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-init kafka-ui
```

Open the root `pom.xml` in IntelliJ as a Maven project. Run `ProducerApplication` and `AnalyticsApplication`. Both default to `localhost:19092,29092,39092`, so no extra configuration is needed.

### Dashboard dev server

```bash
cd dashboard
npm install
npm run dev        # http://localhost:5173, /api/generator -> :8081, other /api and /ws -> :8090
```

### Build commands

| Command | Result |
| --- | --- |
| `./mvnw -DskipTests package` | Builds the jars for all modules. |
| `./mvnw verify` | Unit tests plus Testcontainers integration tests. Needs Docker. |
| `cd dashboard && npm run build` | Type-checks (`tsc -b`) and bundles into `dist/`. |
| `cd dashboard && npm run lint` | Runs oxlint. |
| `docker compose build <service>` | Rebuilds one image. The service Dockerfiles are two-stage: JDK build, then JRE runtime. |

## 5. Configuration

### Environment variables (`.env`, read by Compose)

| Variable | Default | Effect |
| --- | --- | --- |
| `KAFKA_CLUSTER_ID` | fixed base64 UUID | The KRaft cluster id shared by all 3 nodes. |
| `KAFKA_IMAGE` | `apache/kafka:4.4.0` | The broker image. |
| `KAFKA_HEAP` | `-Xms512m -Xmx512m` | Heap for each broker. |
| `DASHBOARD_PORT` | 3000 | Host port for nginx and the dashboard. |
| `KAFKA_UI_PORT` | 8080 | Host port for Kafka UI. |
| `PRODUCER_PORT` | 8081 | Port of producer-service. |
| `ANALYTICS_PORT` | 8090 | Port of analytics-service. |
| `GENERATOR_RATE` | 50 | Events per second at start-up. |
| `GENERATOR_AUTOSTART` | true | Whether the generator starts with the service. |

Variables that each service reads with a local default: `KAFKA_BOOTSTRAP`, plus for analytics `STREAMS_REPLICATION_FACTOR` (3), `STREAMS_MIN_ISR` (2) and `STREAMS_STATE_DIR`.

### producer-service (`application.yml`)

| Setting | Value | Why |
| --- | --- | --- |
| `acks` | `all` | Wait for the leader and all in-sync followers. |
| `enable.idempotence` | true | No duplicates or reordering when the client retries. |
| `max.in.flight.requests.per.connection` | 5 | The highest value that still keeps ordering with idempotence. |
| `compression-type`, `linger.ms` | lz4, 5 | Small batches, cheap compression. |
| `delivery.timeout.ms` | 120000 | Keep retrying for 2 minutes, for example while 2 brokers are down. |
| `generator.*` | rate 50, 5,000 accounts, 3% late, 2% out of order, 1% duplicate, 0.5% high value, bursts of 12 | Controls the synthetic data. All of it can be changed at runtime with `PUT /api/generator/settings`. |

### analytics-service (`application.yml`)

| Setting | Value | Why |
| --- | --- | --- |
| `application-id` | `txn-analytics` | Consumer group id and prefix for internal topics. |
| `processing.guarantee` | `exactly_once_v2` | No double counting on failover or restart. |
| `commit.interval.ms` | 1000 | Fewer transactions. The dashboard reads the stores directly anyway. |
| `num.stream.threads` | 2 | 6 tasks, one per input partition, shared by 2 threads. |
| `producer.transaction.timeout.ms` | 60000 | The 10 s default aborted transactions during broker hand-offs. |
| `windowstore.changelog.additional.retention.ms` | 60000 | The default is 1 day. A shorter value keeps changelogs small and restores fast. |
| `analytics.window-size` / `summary-window-size` | 10s / 1m | The two tumbling windows. |
| `analytics.grace` | 30s | How long a window still accepts late events. |
| `analytics.high-value-threshold-inr` | 100000 | Threshold for a high-value alert. |
| `analytics.velocity-threshold` / `velocity-window` | 10 / 1m | Alert when an account makes more than 10 transactions in a minute. |
| `analytics.dedupe-retention` | 2m | How long transaction ids are remembered. |
| `analytics.live-feed-max-per-second` | 20 | Rate limit for the live feed. |
| `analytics.push-interval-ms` / `health-interval-ms` | 1000 / 3000 | Dashboard push rate and cluster health poll rate. |

## 6. Code tour

### txn-common

- **`TransactionEvent`**: the record carried on `transactions.raw`. It holds the transaction id, account id, amount and currency, `amountInr`, type, channel, region, status, event time and a per-account sequence number.
- **`Topics`**: constants for the 5 topic names. Always use these instead of string literals.
- **`serde/`**: Jackson-based `JsonSerializer`, `JsonDeserializer` and `JsonSerde<T>`, plus typed `TransactionEvent` serializers.

### producer-service

- **`TransactionGenerator`**: builds realistic events and injects late, out-of-order, duplicate and high-value events at the configured ratios (`Injection`, `PlannedEvent`).
- **`GeneratorService`**: a scheduler that runs the generator at the target rate and keeps `GeneratorStats`. Sequence numbers are seeded from the start time, so they keep increasing across restarts.
- **`KafkaEventSender`**: sends with key = `accountId`, so all events for one account land on the same partition in order.
- **`GeneratorController`**: `GET /api/generator`, `POST /start`, `POST /stop` and `PUT /settings`.

### analytics-service: the stream

- **`TransactionTopology`**: builds the whole topology and registers the stores. Every node is named with `Named.as(...)`, so the topology description and internal topic names stay stable.
- **`ParseResultSerde`**: parses without throwing. Bad records come out as `failed` and the `parse-` split sends them to `transactions.dlq` as raw bytes.
- **`EventTimeExtractor`**: uses the event's own timestamp, not the time it arrived.
- **`DedupeProcessor`**: marks a transaction id seen within the last 2 minutes as a duplicate (window store `dedupe-store`).
- **`SequenceCheckProcessor`**: flags out-of-order events per account using the last sequence number seen.
- **`LatenessClassifier`**: tags each event as `ON_TIME`, `LATE_ACCEPTED` or `LATE_DROPPED`, based on stream time, the 10 s window and the 30 s grace.
- **`TotalsProcessor`**: keeps pipeline counters (received, duplicates, late, out of order) in `pipeline-totals`.
- **`WindowAggregateProcessor`**: aggregates accepted events into the `window-10s` and `window-1m` stores per partition, and emits partials to `metrics.windowed`.
- **Velocity**: a DSL `groupByKey().windowedBy(...).count()` with caching off, which fires exactly once when the count reaches 11.

### analytics-service: serving

- **`StreamsTopologyConfig`**: wires the topology into Spring's `StreamsBuilderFactoryBean` and exposes the Streams state to the rest of the app.
- **`MetricsQueryService`**: interactive queries. It reads all partitions' partial windows from the local stores and merges them into one series (`WindowAgg.merge`).
- **`KpiCalculator`** and **`Kpis`**: compute throughput, average amount, failure rate and similar values from the merged windows.
- **`DashboardPublisher`**: every 1 s, builds a `DashboardUpdate` and sends it to `/topic/metrics`.
- **`LiveFeedListener`** and **`AlertListener`**: `@KafkaListener` consumers with `read_committed` isolation. They keep a `RecentBuffer`, and the feed is throttled by `RateLimiter`.
- **`ClusterHealthService`**: polls the brokers, controller quorum, under-replicated partitions and consumer lag with `AdminClient` every 3 s.

### dashboard

- **`useDashboardStream`**: the STOMP connection with reconnect. Each message becomes a reducer action.
- **`dashboardReducer`**: the single source of UI state.
- **`lib/selectors.ts`** and **`lib/format.ts`**: derive values for display and format numbers.
- **`components/`**: `Header`, `HealthStrip`, `KpiTiles`, `VolumeChart`, `WindowCountChart`, `BreakdownChart`, `PipelinePanel`, `LiveFeed`, `AlertsPanel` and `GeneratorControls`, which calls the producer API through `lib/api.ts`.

## 7. Testing

| Level | Command | What it covers |
| --- | --- | --- |
| Backend unit and integration | `./mvnw verify` | Every test class listed below. The `*IT` tests need Docker. |
| Frontend | `cd dashboard && npm test` | Reducer, selectors, formatting and components. |
| System: smoke | `./scripts/smoke-test.sh` | All containers healthy. RF 3, ISR 3 and min ISR 2 on every topic. Leaders spread across brokers. Controller quorum. Data flowing. WebSocket works. |
| System: failover | `./scripts/failover-test.sh` | Stops the leader broker, then the active controller, then 2 of 3 brokers (writes refused). Checks there are no send errors and no data loss. |
| System: accuracy | `./scripts/accuracy-test.sh` | Restarts analytics mid-stream. Every acknowledged event is counted exactly once, and detected duplicates equal injected duplicates. |
| System: load | `RATE=500 DURATION=600 ./scripts/load-test.sh` | Sustained rate, p95 latency under 2 s, stable lag. |

### Test classes

| Class | Module | Kind | What it checks |
| --- | --- | --- | --- |
| `TransactionEventSerdeTest` | txn-common | Unit | JSON round trip and bad input. |
| `TransactionGeneratorTest` | producer | Unit | Injection ratios, sequence numbers, late timestamps. |
| `GeneratorServiceTest` | producer | Unit | Start, stop, settings changes, stats. |
| `GeneratorControllerTest` | producer | Web slice | REST endpoints and validation errors. |
| `ProducerKafkaIT` | producer | Testcontainers | Events actually reach a real Kafka with the right key. |
| `TransactionTopologyTest` | analytics | `TopologyTestDriver` | Event-time windows, grace, late drop, dedupe, ordering, alerts and DLQ. |
| `MergeTest` | analytics | Unit | Merging per-partition partial windows. |
| `LatencyTrackerTest` | analytics | Unit | Latency percentiles. |
| `KpiCalculatorTest` | analytics | Unit | KPI maths. |
| `RateLimiterTest` | analytics | Unit | Live feed throttling. |
| `AnalyticsIT` | analytics | Testcontainers | Kafka → Streams → WebSocket client, end to end. |

The frontend tests are `dashboardReducer.test.ts`, `selectors.test.ts`, `format.test.ts` and `components.test.tsx`, with shared data in `src/test/fixtures.ts`.

The system scripts share helpers from `scripts/lib.sh`. `scripts/ws-check.mjs` is a small Node STOMP client that checks the WebSocket push.

## 8. Extending the system

### Add a topic

1. Add a `create <name> <partitions> --config retention.ms=...` line to `infra/kafka/create-topics.sh`. RF 3 and min ISR 2 are applied automatically.
2. Add a constant to `txn-common/.../Topics.java`.
3. Recreate the init job: `docker compose up -d kafka-init`. The script is idempotent (`--if-not-exists`).

### Add a stream processor

1. Write a class that implements `FixedKeyProcessor<String, EnrichedEvent, EnrichedEvent>` in `stream/`.
2. If it needs state, register a store in `TransactionTopology` with `builder.addStateStore(...)` and pass the store name to `processValues`.
3. Add it to the chain with `.processValues(MyProcessor::new, Named.as("my-step"), MyProcessor.STORE)`. Always name the node.
4. Add cases to `TransactionTopologyTest`. Use `TopologyTestDriver` and set the event time explicitly.

### Add a KPI

1. Compute it in `KpiCalculator` and add the field to the `Kpis` record.
2. Add the field to `dashboard/src/types.ts`.
3. Show it in `KpiTiles.tsx` and add a selector or formatter if needed.
4. Extend `KpiCalculatorTest` and the component test.

### Add a dashboard panel

1. If the data is new, add it to `DashboardUpdate` and fill it in `DashboardPublisher`.
2. Extend `types.ts` and `dashboardReducer.ts`, and add selectors in `lib/selectors.ts`.
3. Create the component in `components/` and place it in `App.tsx`.

### Topology changes need a reset

Adding, removing or renaming stores or repartitioning changes the internal changelog topics. Streams will not start against old changelogs with a different layout. Either run `docker compose down -v`, or set a new `spring.kafka.streams.application-id`.

## 9. Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| Dashboard shows "Reconnecting…" | analytics-service is down or unhealthy. Check `docker compose ps` and `docker compose logs -f analytics-service`. |
| `Streams: REBALANCING` for a long time | Normal for 30–40 s on a fresh cluster. If it lasts longer, check the analytics logs and that `kafka-init` exited with code 0. |
| Consumer lag keeps growing | The rate is too high for this machine (about 550 events/s with exactly-once and RF 3 on 4 cores). Lower the generator rate or raise `num.stream.threads`. |
| Streams fails after a topology change | The changelog layout no longer matches. Run `docker compose down -v` or change the application id. |
| Producer errors with `NOT_ENOUGH_REPLICAS` | 2 of 3 brokers are down. This is intended: writes are refused rather than accepted unsafely. Start a broker again. |
| `OUT_OF_ORDER_SEQUENCE_NUMBER` or `CONCURRENT_TRANSACTIONS` retry storms | The cluster is on transactions protocol v2. `kafka-init` pins `transaction.version=1`. Check with `kafka-features.sh --bootstrap-server localhost:9092 describe`. |
| `*IT` tests fail straight away | Docker is not running. Testcontainers needs it. |
| The dashboard dev server shows no data | analytics-service is not running on 8090. The Vite proxy has nothing to forward to. |

### Useful commands

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions.raw
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
docker logs kafka-1 2>&1 | grep -E 'ERROR|WARN' | tail
curl -s localhost:8090/api/dashboard/snapshot | python3 -m json.tool
```

For operations such as the failover demo, see `docs/RUNBOOK.md`.
