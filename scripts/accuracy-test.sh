#!/usr/bin/env bash
# Exactly-once + accuracy check: every event the generator sent is received by the stream exactly once,
# duplicates/late events match what was injected - with an analytics-service restart in the middle.
source "$(dirname "$0")/lib.sh"
RUN=${RUN:-60}

drain() { # wait until generator has no pending delayed events and the stream has caught up
  wait_until 180 "generator delayed queue empty" bash -c "[ \"\$(genstat pending)\" -eq 0 ]"
  wait_until 180 "stream caught up (received stable, lag 0)" bash -c '
    a=$(total received); sleep 4; b=$(total received)
    [ "$a" = "$b" ] && [ "$(snap | pyq "d[\"health\"][\"consumerLag\"]")" -eq 0 ]'
}

info "Stopping generator and draining"
curl -sf -X POST "$DASH/api/generator/stop" >/dev/null
drain
s0=$(gen); t0=$(snap)
g() { python3 -c "import json,sys; print(json.loads(sys.argv[1])['stats']['$2'])" "$1"; }
tt() { python3 -c "import json,sys; print(json.loads(sys.argv[1])['metrics']['totals']['$2'])" "$1"; }

info "Generating for ${RUN}s, restarting analytics-service halfway"
curl -sf -X POST "$DASH/api/generator/start" >/dev/null
sleep $((RUN/2))
docker restart analytics-service >/dev/null
sleep $((RUN/2))
curl -sf -X POST "$DASH/api/generator/stop" >/dev/null
wait_until 180 "analytics-service healthy after restart" bash -c "test \"\$(docker inspect -f '{{.State.Health.Status}}' analytics-service)\" = healthy"
wait_until 300 "Kafka Streams RUNNING" bash -c "test \"\$(snap | pyq \"d['metrics']['streamsState']\")\" = RUNNING"
drain
s1=$(gen); t1=$(snap)

sent=$(( $(g "$s1" acked) - $(g "$s0" acked) ))
recv=$(( $(tt "$t1" received) - $(tt "$t0" received) ))
dupI=$(( $(g "$s1" injectedDuplicates) - $(g "$s0" injectedDuplicates) ))
dupD=$(( $(tt "$t1" duplicates) - $(tt "$t0" duplicates) ))
lateI=$(( $(g "$s1" injectedLate) - $(g "$s0" injectedLate) ))
lateD=$(( $(tt "$t1" lateAccepted) + $(tt "$t1" lateDropped) - $(tt "$t0" lateAccepted) - $(tt "$t0" lateDropped) ))
oooI=$(( $(g "$s1" injectedOutOfOrder) - $(g "$s0" injectedOutOfOrder) ))
oooD=$(( $(tt "$t1" outOfOrder) - $(tt "$t0" outOfOrder) ))
proc=$(( $(tt "$t1" processed) - $(tt "$t0" processed) ))
drop=$(( $(tt "$t1" lateDropped) - $(tt "$t0" lateDropped) ))
echo "      acked by Kafka: $sent   received by stream: $recv   counted: $proc"
echo "      duplicates injected/detected: $dupI/$dupD   late injected/detected: $lateI/$lateD   out-of-order injected/detected: $oooI/$oooD"
check "every acknowledged event received exactly once despite restart ($sent = $recv)" test "$sent" -eq "$recv"
check "duplicates detected = duplicates injected ($dupI)" test "$dupD" -eq "$dupI"
check "counted = received - duplicates - too late" test "$proc" -eq $((recv - dupD - drop))
# Each input partition keeps its own event-time clock, so replay speed differences after the restart
# cannot make on-time events look late. Short delays (< rest of their window) are not late at all.
check "late detected <= late injected ($lateD <= $lateI)" test "$lateD" -le "$lateI"
check "late detected > 0" test "$lateD" -gt 0
check "out-of-order detected >= swapped pairs injected" test "$oooD" -ge "$oooI"
curl -sf -X POST "$DASH/api/generator/start" >/dev/null
summary
