# `txn-common` — shared model, topics and JSON serdes

[← Back to docs index](README.md)

`txn-common` is a plain Java library (no Spring) that both backend services depend on. It defines:

- the **event model** that travels on `transactions.raw` (`TransactionEvent` and its enums),
- the **topic names** (`Topics`),
- the **JSON serialization** used for every Kafka value in the system (`Json`, `JsonSerializer`, `JsonDeserializer`, `JsonSerde`, plus two no-arg wrappers for Kafka config).

Because the producer, the Kafka Streams topology, the `@KafkaListener`s and the tests all use these classes, a change here changes the wire format for the whole system.

| | |
|---|---|
| Maven coordinates | `com.bank.txn:txn-common:1.0.0` (parent: root [`pom.xml`](../pom.xml)) |
| Dependencies | `org.apache.kafka:kafka-clients` (version pinned to 4.4.0 by the root pom), `tools.jackson.core:jackson-databind` (Jackson 3) |
| Test dependencies | JUnit Jupiter, AssertJ |
| Used by | [`producer-service`](producer-service.md), [`analytics-service`](analytics-service.md) |

## Folder layout

```
txn-common/
├── pom.xml
└── src/
    ├── main/java/com/bank/txn/common/
    │   ├── Topics.java
    │   ├── model/   Channel, Currency, Region, TransactionEvent, TransactionStatus, TransactionType
    │   └── serde/   Json, JsonDeserializer, JsonSerde, JsonSerializer,
    │                TransactionEventDeserializer, TransactionEventSerializer
    └── test/java/com/bank/txn/common/
        └── TransactionEventSerdeTest.java
```

---

## [`pom.xml`](../txn-common/pom.xml)

Declares the module (`artifactId` `txn-common`, description "Shared event model, topics and JSON serdes") with the four dependencies above. Versions come from the Spring Boot parent BOM, except `kafka-clients`, which follows the root pom's `kafka.version` property (4.4.0).

---

## Package `com.bank.txn.common`

### [`Topics.java`](../txn-common/src/main/java/com/bank/txn/common/Topics.java)

`public final class Topics`: string constants for every application topic. It has a private constructor, so it can't be instantiated.

| Constant | Value | Written by | Read by |
|---|---|---|---|
| `TRANSACTIONS_RAW` | `transactions.raw` | `KafkaEventSender` (producer-service) | `TransactionTopology` source, `LiveFeedListener` |
| `TRANSACTIONS_LATE` | `transactions.late` | `TransactionTopology` (`sink-late`) | nothing in the repo (audit topic) |
| `TRANSACTIONS_DLQ` | `transactions.dlq` | `TransactionTopology` (`sink-dlq`) | nothing in the repo (dead-letter queue) |
| `METRICS_WINDOWED` | `metrics.windowed` | `TransactionTopology` (`sink-metrics-10s`, `sink-metrics-1m`) | nothing in the repo (the dashboard reads state stores instead) |
| `ALERTS` | `alerts` | `TransactionTopology` (`sink-alerts-hv`, `sink-alerts-velocity`) | `AlertListener` |

`ClusterHealthService` also uses all five names to describe partitions and leaders. The topics are created by [`infra/kafka/create-topics.sh`](infrastructure.md#infrakafkacreate-topicssh), not by the applications. Broker auto-create is disabled.

---

## Package `com.bank.txn.common.model`

### [`TransactionEvent.java`](../txn-common/src/main/java/com/bank/txn/common/model/TransactionEvent.java)

`public record TransactionEvent(...)` is the one event type in the system. Each instance is one banking transaction.

| Component | Type | Meaning |
|---|---|---|
| `eventId` | `String` | Globally unique id (a random UUID from the generator). Used for **de-duplication** by `DedupeProcessor`. |
| `accountId` | `String` | Account such as `ACC-000123`. It is also the **Kafka record key**, so all of an account's events go to one partition and keep their order. |
| `sequenceNo` | `long` | Per-account number that only goes up. Used by `SequenceCheckProcessor` to detect out-of-order events and gaps. |
| `type` | `TransactionType` | `DEPOSIT`, `WITHDRAWAL`, `TRANSFER`, `PAYMENT` |
| `channel` | `Channel` | `ATM`, `POS`, `ONLINE`, `MOBILE`, `BRANCH` |
| `status` | `TransactionStatus` | `APPROVED`, `DECLINED` |
| `amount` | `BigDecimal` | Amount in the transaction's own currency, scale 2 |
| `currency` | `Currency` | `INR`, `USD`, `EUR`, `GBP` |
| `merchantCategory` | `String` (nullable) | Only set for `PAYMENT` (e.g. `GROCERY`, `TRAVEL`) |
| `region` | `Region` | `NORTH`, `SOUTH`, `EAST`, `WEST` |
| `eventTime` | `Instant` | When the transaction **happened**. This is the event time used for all windowing (see `EventTimeExtractor`). |
| `producedAt` | `Instant` (nullable) | When it was **sent** to Kafka. Set just before sending; used for end-to-end latency (`LatencyTracker`). |

Methods:

| Method | Returns | What it does |
|---|---|---|
| `amountInr()` | `BigDecimal` | `currency.toInr(amount)`: the amount converted to INR at the fixed reference rate, scale 2, `HALF_UP`. All aggregates and alert thresholds use this value. |
| `approved()` | `boolean` | `status == APPROVED` |
| `withProducedAt(Instant)` | `TransactionEvent` | A copy with `producedAt` replaced. `GeneratorService.send` calls it at send time. |

`amountInr()` and `approved()` are methods, not record components, so they are **not serialized** to JSON. `TransactionEventSerdeTest.timestampsAreWrittenAsIsoStrings` checks this.

JSON shape on the wire (from the serde test):

```json
{"eventId":"e-1","accountId":"ACC-000001","sequenceNo":42,"type":"PAYMENT","channel":"POS",
 "status":"APPROVED","amount":1234.50,"currency":"USD","merchantCategory":"GROCERY","region":"NORTH",
 "eventTime":"2026-11-12T10:15:30.123Z","producedAt":"2026-11-12T10:15:31.004Z"}
```

### [`Currency.java`](../txn-common/src/main/java/com/bank/txn/common/model/Currency.java)

`public enum Currency { INR, USD, EUR, GBP }`. Each constant has a fixed `rateToInr` (`BigDecimal`):

| Constant | Rate to INR |
|---|---|
| `INR` | 1 |
| `USD` | 83.50 |
| `EUR` | 90.20 |
| `GBP` | 105.80 |

- **Constructor** `Currency(String rateToInr)`: parses the rate.
- **`toInr(BigDecimal amount)`**: returns `amount × rateToInr`, rounded to scale 2 with `HALF_UP`. The generator also calls `toInr(BigDecimal.ONE)` to convert an INR amount *into* the transaction currency.

### [`Channel.java`](../txn-common/src/main/java/com/bank/txn/common/model/Channel.java)
`public enum Channel { ATM, POS, ONLINE, MOBILE, BRANCH }`: where the transaction was made.

### [`Region.java`](../txn-common/src/main/java/com/bank/txn/common/model/Region.java)
`public enum Region { NORTH, SOUTH, EAST, WEST }`: the account's region. The generator derives it from the account id hash, so each account always has the same region.

### [`TransactionStatus.java`](../txn-common/src/main/java/com/bank/txn/common/model/TransactionStatus.java)
`public enum TransactionStatus { APPROVED, DECLINED }`.

### [`TransactionType.java`](../txn-common/src/main/java/com/bank/txn/common/model/TransactionType.java)
`public enum TransactionType { DEPOSIT, WITHDRAWAL, TRANSFER, PAYMENT }`.

---

## Package `com.bank.txn.common.serde`

### [`Json.java`](../txn-common/src/main/java/com/bank/txn/common/serde/Json.java)

`public final class Json` holds one shared, thread-safe Jackson 3 `JsonMapper`:

- **`MAPPER`** is built with:
  - `DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS` **disabled**, so `Instant`s are written as ISO-8601 strings (`"2026-11-12T10:15:30.123Z"`), which are readable in Kafka UI.
  - `DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES` **disabled**, so readers ignore new fields (forward compatible).

Used directly by `ParseResultSerde` (analytics-service) and by the classes below.

### [`JsonSerializer.java`](../txn-common/src/main/java/com/bank/txn/common/serde/JsonSerializer.java)

`public class JsonSerializer<T> implements org.apache.kafka.common.serialization.Serializer<T>`

- **`serialize(String topic, T data)`**: returns `null` for `null` data (a Kafka tombstone). Otherwise returns `Json.MAPPER.writeValueAsBytes(data)`. A Jackson `RuntimeException` is wrapped in Kafka's `SerializationException("Cannot serialize value for topic …")`.

### [`JsonDeserializer.java`](../txn-common/src/main/java/com/bank/txn/common/serde/JsonDeserializer.java)

`public class JsonDeserializer<T> implements Deserializer<T>`

- **Constructor** `JsonDeserializer(Class<T> type)`: the target type.
- **`deserialize(String topic, byte[] data)`**: `null` → `null`. Otherwise returns `Json.MAPPER.readValue(data, type)`. Parse errors are wrapped in `SerializationException("Cannot deserialize <Type> from <topic>")`.

Because this deserializer **throws** on bad input, the Streams topology does **not** use it for the input topic. It uses the non-throwing [`ParseResultSerde`](analytics-service.md#parseresultserdejava) instead. `AlertListener` uses a `JsonDeserializer<Alert>` but catches the exception itself.

### [`JsonSerde.java`](../txn-common/src/main/java/com/bank/txn/common/serde/JsonSerde.java)

`public class JsonSerde<T> extends Serdes.WrapperSerde<T>`

- **Constructor** `JsonSerde(Class<T> type)`: pairs a `JsonSerializer<T>` with a `JsonDeserializer<T>(type)`. The topology uses it for `EnrichedEvent`, `WindowAgg`, `PipelineTotals`, `Alert`, `WindowMetrics` and `TransactionEvent` values.

### [`TransactionEventSerializer.java`](../txn-common/src/main/java/com/bank/txn/common/serde/TransactionEventSerializer.java)

`public class TransactionEventSerializer extends JsonSerializer<TransactionEvent>`. It has a **no-arg** constructor, so Kafka/Spring can create it from a class name in configuration. Used as `spring.kafka.producer.value-serializer` in producer-service and by test producers.

### [`TransactionEventDeserializer.java`](../txn-common/src/main/java/com/bank/txn/common/serde/TransactionEventDeserializer.java)

`public class TransactionEventDeserializer extends JsonDeserializer<TransactionEvent>`. Its no-arg constructor calls `super(TransactionEvent.class)`. analytics-service uses it as the delegate of Spring's `ErrorHandlingDeserializer` (`spring.deserializer.value.delegate.class`) for `LiveFeedListener`. Tests also use it.

---

## Tests

### [`TransactionEventSerdeTest.java`](../txn-common/src/test/java/com/bank/txn/common/TransactionEventSerdeTest.java)

Uses a `JsonSerde<TransactionEvent>` and a fixed `sample()` event (USD 1234.50, fixed instants).

| Test | Checks |
|---|---|
| `roundTripPreservesAllFieldsIncludingBigDecimalAndInstants` | serialize → deserialize gives an equal record |
| `timestampsAreWrittenAsIsoStrings` | `eventTime` is an ISO string; `amountInr` and `approved` are not in the JSON |
| `unknownFieldsAreIgnored` | an extra `"newField"` does not break deserialization |
| `malformedJsonRaisesSerializationException` | `"not-json"` → `SerializationException` |
| `convertsToInr` | 1234.50 USD × 83.50 = 103080.75 INR |
