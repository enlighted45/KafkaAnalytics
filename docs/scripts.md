# `scripts/` — system tests against the running stack

[← Back to docs index](README.md)

Bash scripts (plus one Node script) that test the **deployed Docker Compose stack** end to end. They talk to the stack through the dashboard's nginx (`DASH`, default `http://localhost:3000`), so they exercise the same proxy paths a browser uses. They also run `docker` and the Kafka CLI tools inside the broker containers.

Prerequisites: the stack is running (`docker compose up -d --build`), with `docker`, `curl` and `python3` on the host. `smoke-test.sh` also needs `node` and `dashboard/node_modules` (it reuses the dashboard's STOMP library), so run `npm install` in `dashboard/` first.

Every script `source`s `lib.sh`, prints PASS/FAIL lines, and ends with `summary`, which exits 1 if any check failed.

| Script | Run | What it proves |
|---|---|---|
| [`smoke-test.sh`](#smoke-testsh) | `./scripts/smoke-test.sh` | containers healthy, 3-broker replication correct, data flowing, WebSocket pushes |
| [`failover-test.sh`](#failover-testsh) | `./scripts/failover-test.sh` | leader broker loss, controller loss, and a 2-of-3 outage are handled safely |
| [`accuracy-test.sh`](#accuracy-testsh) | `RUN=60 ./scripts/accuracy-test.sh` | exactly-once across an analytics-service restart; detected counts match injected ones |
| [`load-test.sh`](#load-testsh) | `RATE=500 DURATION=600 ./scripts/load-test.sh` | sustained throughput, p95 latency < 2 s, lag not growing |

---

## [`lib.sh`](../scripts/lib.sh)

Shared helpers. It sets `set -euo pipefail` and `cd`s to the repository root.

| Name | Kind | Description |
|---|---|---|
| `DASH` | var | base URL, default `http://localhost:3000` (override with an env var) |
| `BS` | var | `kafka-1:9092,kafka-2:9092,kafka-3:9092` (exported; not used by the current scripts) |
| `PASS`, `FAIL` | var | counters |
| `green`, `red`, `info` | fn | coloured output (`info` prints `==> …` in cyan) |
| `pass DESC` / `fail DESC` | fn | print and increment the counters |
| `check DESC CMD…` | fn | runs `CMD`; pass if it exits 0, otherwise fail |
| `summary` | fn | prints "ALL n CHECKS PASSED" or "n FAILED, m passed" and `exit 1` |
| `snap` | fn | `curl $DASH/api/dashboard/snapshot` (analytics snapshot JSON) |
| `gen` | fn | `curl $DASH/api/generator` (generator status JSON) |
| `pyq EXPR` | fn | jq-like: reads JSON from stdin into `d` and prints the Python expression `EXPR` |
| `total FIELD` | fn | `snap` → `metrics.totals.FIELD` |
| `genstat FIELD` | fn | `gen` → `stats.FIELD` |
| `kt ARGS…` | fn | `kafka-topics.sh --bootstrap-server localhost:9092 ARGS` inside `kafka-1` |
| `kexec CMD…` | fn | runs `CMD` in the first **running** broker container (works while some brokers are stopped) |
| `leaders_of TOPIC` | fn | one line per partition with the leader broker id (parsed from `--describe`) |
| `active_controller` | fn | `LeaderId` from `kafka-metadata-quorum.sh describe --status` |
| `wait_until SECS DESC CMD…` | fn | retries `CMD` every 2 s; passes when it succeeds, records a fail on timeout. It always returns 0, so the script continues. |
| `set_rate RATE` | fn | reads the current settings with `gen`, changes `ratePerSecond`, and `PUT`s them back |
| `under_replicated` | fn | number of under-replicated partitions in the cluster |

Finally it `export`s `DASH`, `BS` and the helper functions so `bash -c '…'` checks can use them.

---

## [`smoke-test.sh`](../scripts/smoke-test.sh)

1. **Containers**: `kafka-1/2/3`, `producer-service`, `analytics-service` and `dashboard` report health `healthy`; `kafka-init` exited with 0.
2. **Kafka cluster**: 3 brokers registered; the controller quorum has 3 voters; an active controller is elected. For each of the 5 topics: `ReplicationFactor: 3`, `min.insync.replicas=2`, and every partition's ISR has 3 members. `transactions.raw` leaders are spread over all 3 brokers.
3. **Applications**: the dashboard page is served (`<div id="root">`); the generator API is reachable through nginx and running; Streams reaches `RUNNING` within 90 s; `processed` increases over 15 s; dashboard TPS > 0; health shows 3 brokers up; `node scripts/ws-check.mjs` receives metrics, health and transactions over the WebSocket.

## [`ws-check.mjs`](../scripts/ws-check.mjs)

A Node ES module that imports `Client` from `../dashboard/node_modules/@stomp/stompjs/esm6/index.js`, the dashboard's own STOMP library. It connects to `ws://…/ws` (derived from `DASH`), subscribes to `/topic/metrics`, `/topic/health` and `/topic/transactions`, and JSON-parses each message.
- **`finish(ok)`**: clears the 15 s timeout, prints the message counts, deactivates the client and exits 0 or 1.
- It exits 0 as soon as every topic has received at least one message. A STOMP error or the 15 s timeout gives exit 1.

## [`failover-test.sh`](../scripts/failover-test.sh)

Runs with the generator on. A `trap restore_all EXIT` restarts all brokers whatever happens.

0. **Baseline**: waits for 0 under-replicated partitions and records `sendErrors`.
1. **Leader broker down**: picks the broker leading the most `transactions.raw` partitions and stops it. Waits until it leads nothing, checks there are no offline partitions, and waits for the dashboard to show it down. After 15 s: producer `acked` increased by > 100, stream `processed` increased by > 100, and `sendErrors` is unchanged. Restarts the broker and waits for ISR to be back to full.
2. **Active controller down**: stops the active controller, waits for a different node to become controller, checks processing continues, restarts it and waits for full replication.
3. **Two brokers down**: keeps one survivor (`(ctrl % 3) + 1`) and stops the other two. Inside the survivor, it sends a probe with `kafka-console-producer.sh` using `acks=all` and short timeouts, and checks the output shows a rejection (`NotEnoughReplicas`, timeout or expired). This is the intended behaviour with `min.insync.replicas=2`. Restarts both, waits for full replication, then checks the producer is sending again (+100 acks in 10 s) and `sendErrors` is still at baseline. The producer's 120 s `delivery.timeout.ms` rides out the outage, so nothing is lost.

## [`accuracy-test.sh`](../scripts/accuracy-test.sh)

Checks exactly-once and detection accuracy across an analytics-service restart. `RUN` (default 60 s) is the generating period.

- **`drain()`**: waits until the generator's delay queue is empty (`pending == 0`), then until the stream has caught up: `received` stable over 4 s and consumer lag 0.
- **`g JSON FIELD`** / **`tt JSON FIELD`**: read `stats.FIELD` from a generator status, or `metrics.totals.FIELD` from a snapshot.

Steps: stop the generator and drain, take baselines `s0` (generator) and `t0` (snapshot); start the generator; after `RUN/2` run `docker restart analytics-service`; after another `RUN/2` stop the generator; wait for the service to be healthy and Streams `RUNNING`; drain; take `s1`/`t1`. All values below are differences between the two baselines.

| Check | Condition |
|---|---|
| every acknowledged event received exactly once, despite the restart | `acked == received` |
| duplicates detected = duplicates injected | `dupD == dupI` |
| counted = received − duplicates − too late | `processed == received − dupD − lateDropped` |
| late detected ≤ late injected | short delays that stay inside their window aren't late at all |
| late detected > 0 | |
| out-of-order detected ≥ swapped pairs injected | late events also show up as out of order |

Finally it restarts the generator.

## [`load-test.sh`](../scripts/load-test.sh)

`RATE` (default 500/s) for `DURATION` (default 600 s). It saves the original rate and restores it on exit (`trap`).

1. `set_rate RATE`, start the generator, record `sendErrors`, wait 20 s.
2. Every 30 s, reads `latency.p95Ms`, `health.consumerLag` and `kpis.tps` from the snapshot, prints them, and tracks the worst p95.
3. Checks: average dashboard TPS ≥ 85 % of `RATE`; worst p95 < 2000 ms; median lag in the second half ≤ 2 × the first-half median + `RATE` (lag is not growing); no new send errors.

The [RUNBOOK](RUNBOOK.md) puts this host's measured catch-up capacity at about 550 events/s with exactly-once and RF 3, so rates much above 500/s are expected to fail the lag check on a 4-core machine.
