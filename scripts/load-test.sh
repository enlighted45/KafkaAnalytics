#!/usr/bin/env bash
# Sustained load: RATE events/s for DURATION seconds; latency p95 must stay < 2 s and lag must not grow.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-500}; DURATION=${DURATION:-600}
orig=$(gen | pyq "d['settings']['ratePerSecond']")
trap 'set_rate $orig' EXIT

info "Load test: $RATE events/s for ${DURATION}s"
set_rate "$RATE"; curl -sf -X POST "$DASH/api/generator/start" >/dev/null
err0=$(genstat sendErrors); sleep 20
worst_p95=0; lags=(); tps=()
end=$((SECONDS+DURATION))
while [ $SECONDS -lt $end ]; do
  read -r p95 lag tp <<<"$(snap | pyq "f\"{d['metrics']['latency']['p95Ms']} {d['health']['consumerLag']} {d['metrics']['kpis']['tps']}\"")"
  lags+=("$lag"); tps+=("$tp"); [ "$p95" -gt "$worst_p95" ] && worst_p95=$p95
  printf '      t=%4ds  tps=%-7s p95=%5sms  lag=%s\n' $((DURATION-(end-SECONDS))) "$tp" "$p95" "$lag"
  sleep 30
done
n=${#lags[@]}; half=$((n/2))
first=$(printf '%s\n' "${lags[@]:0:$half}" | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}')
second=$(printf '%s\n' "${lags[@]:$half}" | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}')
avg_tps=$(printf '%s\n' "${tps[@]}" | awk '{s+=$1} END{printf "%.0f", s/NR}')
check "throughput ~${RATE}/s (avg dashboard tps $avg_tps)" test "$avg_tps" -ge $((RATE*85/100))
check "worst p95 end-to-end latency < 2000 ms (${worst_p95} ms)" test "$worst_p95" -lt 2000
check "consumer lag not growing (median 1st half $first, 2nd half $second)" test "$second" -le $((first*2 + RATE))
check "no producer send errors" test "$(genstat sendErrors)" -eq "$err0"
summary
