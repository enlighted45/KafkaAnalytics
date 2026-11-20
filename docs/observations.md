# Observations: inconsistencies and unused code found while documenting

[← Back to docs index](README.md)

These notes came up while reading the code for this documentation. **No source code was changed.** They are listed so a maintainer can decide whether to fix or keep each one. None of them breaks the system as tested.

## Stale or contradictory comments and docs

| Where | What it says | What the code does |
|---|---|---|
| [`LatenessClassifier.java`](../analytics-service/src/main/java/com/bank/txn/analytics/stream/LatenessClassifier.java) Javadoc | "Runs after the repartition to a single partition, so stream time is global across all accounts." | There is no repartition. The topology is one sub-topology with one task per input partition, and stream time is **per partition** (as the `TransactionTopology` Javadoc and the root README describe). |
| Root [`README.md`](../README.md) | "a windowed state store of seen `eventId`s (10 min)" | `application.yml` sets `analytics.dedupe-retention: 2m`. Only `TransactionTopology.Config.defaults()` (used in tests) has 10 min. |
| [`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md) §4 | REST `GET /api/metrics/snapshot` | The endpoint is `GET /api/dashboard/snapshot`. |
| `IMPLEMENTATION_PLAN.md` §3 | "Plus 1-min top accounts by volume" | Not implemented. |
| `IMPLEMENTATION_PLAN.md` §3 | "Deserialization errors → `transactions.dlq` via a custom exception handler" | Done differently: a non-throwing `ParseResultSerde` plus a `split` branch, with no exception handler. |
| `IMPLEMENTATION_PLAN.md` §7–8 | Playwright browser E2E tests; Spring profiles `local` / `docker`; a "WebSocket batcher" unit test | There is no Playwright test, no profile-specific config (environment variables are used instead) and no batcher test. `DashboardPublisher` is a fixed-rate scheduled push. |
| [`docs/RUNBOOK.md`](RUNBOOK.md) | "See the load test results in `IMPLEMENTATION_PLAN.md`" | `IMPLEMENTATION_PLAN.md` has no load test results. |
| [`dashboard/README.md`](../dashboard/README.md) | Vite template text | Nothing project-specific. |

## Behaviour worth knowing

- **The 1-minute windows use 10-second lateness.** `LatenessClassifier` only checks against `windowSize` (10 s) + grace. An event too late for its 10 s window is left out of the 1 min window as well, even if that window is still open. The test `eventLaterThanGraceIsDroppedFromWindowsAndRoutedToLateTopic` asserts this.
- **Velocity alert text is fixed.** `TransactionTopology.velocityAlert` always says "within 1 minute", whatever `analytics.velocity-window` is set to. The dashboard's `AlertsPanel` hint also hard-codes "₹1,00,000" and "> 10 transactions per account per minute".
- **Velocity uses tumbling windows.** A burst that crosses a window boundary can stay under the threshold in both windows and raise no alert.
- **High-value alerts include declined and too-late events.** The filter runs on the `unique` stream, before the `accepted` filter.
- **DLQ records are not in the totals.** `PipelineTotals.received` only counts events that parsed. Records sent to `transactions.dlq` are logged but not counted anywhere on the dashboard.
- **Interactive queries are local only.** `MetricsQueryService` reads local stores. With more than one analytics-service instance, each would only report its own partitions. The RUNBOOK notes this.
- **`metrics.windowed`, `transactions.late` and `transactions.dlq` have no consumer in this repo.** They exist for audit and external consumers. The dashboard reads the state stores directly.

## Unused or half-used code

| Item | Notes |
|---|---|
| `DashboardUpdate.summaryWindows` | Computed (1-minute windows over 30 min) and pushed every second, but the dashboard never uses it. |
| `dashboard/src/lib/format.ts` → `formatClock` | Exported, never imported. |
| `dashboard/src/state/dashboardReducer.ts` → `lastMessageAt` | Written on every metrics push, never read. |
| `dashboard/src/index.css` → `.visually-hidden` | Not used by any component. |
| `dashboard/public/icons.svg` | Vite template sprite, not referenced. |
| `WebSocketConfig` → `/app` prefix | Configured, but there are no `@MessageMapping` handlers. |
| `AnalyticsProperties.liveFeedMaxPerSecond` | Bound, but `LiveFeedListener` reads the same key with `@Value` instead of using the record. |
| `producer-service/Dockerfile` | Creates `/var/lib/streams`, which only analytics-service uses (copied from that Dockerfile). |
| `scripts/lib.sh` → `BS` | Exported but not used by any script. |
