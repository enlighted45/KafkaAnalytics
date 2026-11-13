#!/usr/bin/env bash
# Broker / controller failover with the generator running.
#  1. stop the broker that leads the most partitions -> followers take over, no send errors, processing continues
#  2. stop the active KRaft controller -> a standby controller takes over
#  3. stop 2 of 3 brokers -> writes are refused (min.insync.replicas=2), restart -> no data loss
source "$(dirname "$0")/lib.sh"

restore_all() { docker start kafka-1 kafka-2 kafka-3 >/dev/null 2>&1 || true; }
trap restore_all EXIT


info "Baseline"
wait_until 120 "all partitions fully replicated before starting" bash -c "[ \"\$(under_replicated)\" -eq 0 ]" || true
err0=$(genstat sendErrors)

# ---------------------------------------------------------------- 1. leader broker down
victim=$(leaders_of transactions.raw | sort | uniq -c | sort -rn | awk 'NR==1{print $2}')
info "1. Stopping kafka-$victim (leads the most transactions.raw partitions: $(leaders_of transactions.raw | tr '\n' ' '))"
acked0=$(genstat acked); proc0=$(total processed)
docker stop kafka-$victim >/dev/null
wait_until 60 "no partition is led by kafka-$victim any more" bash -c "! (leaders_of transactions.raw) | grep -qx $victim"
check "no offline partitions (leaders: $(leaders_of transactions.raw | tr '\n' ' '))" bash -c "! (leaders_of transactions.raw) | grep -qE '^(none|-1)$'"
wait_until 60 "dashboard shows kafka-$victim down" bash -c "test \"\$(snap | pyq \"[b['up'] for b in d['health']['brokers'] if b['id']==$victim][0]\")\" = False"
sleep 15
acked1=$(genstat acked); proc1=$(total processed)
check "producer kept getting acks with 1 broker down (+$((acked1-acked0)))" test $((acked1-acked0)) -gt 100
check "stream kept processing with 1 broker down (+$((proc1-proc0)))" test $((proc1-proc0)) -gt 100
check "no producer send errors" test "$(genstat sendErrors)" -eq "$err0"

info "Restarting kafka-$victim"
docker start kafka-$victim >/dev/null
wait_until 180 "ISR back to 3 replicas everywhere (no under-replicated partitions)" bash -c "[ \"\$(under_replicated)\" -eq 0 ]"

# ---------------------------------------------------------------- 2. active controller down
ctrl=$(active_controller)
info "2. Stopping active controller kafka-$ctrl"
docker stop kafka-$ctrl >/dev/null
wait_until 60 "a standby controller became active" bash -c "c=\$(active_controller); [ -n \"\$c\" ] && [ \"\$c\" != $ctrl ] && [ \"\$c\" != -1 ]"
echo "      new active controller: kafka-$(active_controller)"
proc0=$(total processed); sleep 10; proc1=$(total processed)
check "processing continues after controller failover (+$((proc1-proc0)))" test $((proc1-proc0)) -gt 0
docker start kafka-$ctrl >/dev/null
wait_until 180 "cluster fully replicated again" bash -c "[ \"\$(under_replicated)\" -eq 0 ]"

# ---------------------------------------------------------------- 3. two brokers down
survivor=$(( (ctrl % 3) + 1 )); down=$(printf '%s\n' 1 2 3 | grep -vx $survivor | tr '\n' ' ')
info "3. Stopping 2 of 3 brokers ($down) - writes must be refused, not silently accepted"
for b in $down; do docker stop kafka-$b >/dev/null; done
sleep 8
out=$(docker exec kafka-$survivor bash -c "echo probe | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic transactions.raw --producer-property acks=all --producer-property delivery.timeout.ms=10000 --producer-property request.timeout.ms=5000 --producer-property max.block.ms=10000 2>&1" || true)
check "write with acks=all is rejected while only 1 broker is up" grep -qiE 'NotEnoughReplicas|TimeoutException|NOT_ENOUGH_REPLICAS|not enough|expired' <<<"$out"
for b in $down; do docker start kafka-$b >/dev/null; done
wait_until 180 "cluster fully replicated again" bash -c "[ \"\$(under_replicated)\" -eq 0 ]"
acked0=$(genstat acked); sleep 10; acked1=$(genstat acked)
check "producer recovered and is sending again (+$((acked1-acked0)))" test $((acked1-acked0)) -gt 100
check "no events failed delivery during the outages (sendErrors=$(genstat sendErrors))" test "$(genstat sendErrors)" -eq "$err0"
summary
