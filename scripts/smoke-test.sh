#!/usr/bin/env bash
# Smoke test of the deployed stack: containers, 3-broker replication, data flowing to the dashboard.
source "$(dirname "$0")/lib.sh"

info "Containers"
for c in kafka-1 kafka-2 kafka-3 producer-service analytics-service dashboard; do
  check "$c healthy" test "$(docker inspect -f '{{.State.Health.Status}}' $c 2>/dev/null)" = healthy
done
check "kafka-init completed successfully" test "$(docker inspect -f '{{.State.ExitCode}}' kafka-init)" = 0

info "Kafka cluster: 3 brokers, RF=3, 1 leader + 2 followers per partition"
check "3 brokers registered" test "$(kt --describe --topic transactions.raw >/dev/null; docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>/dev/null | grep -c '^kafka-')" -eq 3
check "controller quorum has 3 voters" test "$(docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status 2>/dev/null | grep -o '"id": [0-9]' | wc -l)" -eq 3
ctrl=$(active_controller); check "active controller elected (node $ctrl)" test -n "$ctrl"
for t in transactions.raw transactions.late transactions.dlq metrics.windowed alerts; do
  d=$(kt --describe --topic $t)
  check "$t: ReplicationFactor 3" grep -q 'ReplicationFactor: 3' <<<"$d"
  check "$t: min.insync.replicas=2" grep -q 'min.insync.replicas=2' <<<"$d"
  check "$t: every partition has 3 replicas in sync" bash -c "awk '/Partition:/ {for(i=1;i<=NF;i++) if(\$i==\"Isr:\") print \$(i+1)}' <<<'$d' | awk -F, 'NF!=3{bad=1} END{exit bad}'"
done
check "transactions.raw leaders spread across all 3 brokers" test "$(leaders_of transactions.raw | sort -u | wc -l)" -eq 3

info "Applications"
check "dashboard page served" bash -c "curl -sf $DASH/ | grep -q '<div id=\"root\">'"
check "generator API reachable through nginx" bash -c "gen >/dev/null"
check "generator running" test "$(gen | pyq "d['running']")" = True
wait_until 90 "Kafka Streams RUNNING" bash -c "test \"\$(snap | pyq \"d['metrics']['streamsState']\")\" = RUNNING"
p1=$(total processed); sleep 15; p2=$(total processed)
check "events processed in 15 s: $((p2-p1))" test $((p2-p1)) -gt 0
check "TPS > 0 on dashboard" python3 -c "import sys; sys.exit(0 if $(snap | pyq "d['metrics']['kpis']['tps']") > 0 else 1)"
check "cluster health shows 3 brokers up" test "$(snap | pyq "sum(1 for b in d['health']['brokers'] if b['up'])")" -eq 3
check "WebSocket pushes metrics, health and transactions" node scripts/ws-check.mjs
summary
