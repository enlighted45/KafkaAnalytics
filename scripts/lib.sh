#!/usr/bin/env bash
# Shared helpers for the system test scripts.
set -euo pipefail
cd "$(dirname "$0")/.."

DASH=${DASH:-http://localhost:3000}
BS=kafka-1:9092,kafka-2:9092,kafka-3:9092
PASS=0; FAIL=0

green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
info()  { printf '\033[36m==> %s\033[0m\n' "$*"; }
pass()  { green "  PASS  $*"; PASS=$((PASS+1)); }
fail()  { red   "  FAIL  $*"; FAIL=$((FAIL+1)); }
check() { local desc=$1; shift; if "$@"; then pass "$desc"; else fail "$desc"; fi; }
summary() { echo; if [ "$FAIL" -eq 0 ]; then green "ALL $PASS CHECKS PASSED"; else red "$FAIL FAILED, $PASS passed"; exit 1; fi; }

# jq-like access to the dashboard snapshot / generator status using python
snap() { curl -sf "$DASH/api/dashboard/snapshot"; }
gen()  { curl -sf "$DASH/api/generator"; }
pyq()  { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
total() { snap | pyq "d['metrics']['totals']['$1']"; }
genstat() { gen | pyq "d['stats']['$1']"; }

kt() { docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@" 2>/dev/null; }
# run a Kafka CLI on any running broker
kexec() { for b in kafka-1 kafka-2 kafka-3; do
            if [ "$(docker inspect -f '{{.State.Running}}' $b 2>/dev/null)" = true ]; then docker exec $b "$@" 2>/dev/null; return; fi
          done; }
leaders_of() { kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$1" \
               | awk '/Partition:/ {for(i=1;i<=NF;i++) if($i=="Leader:") print $(i+1)}'; }
active_controller() { kexec /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status \
                      | awk '/LeaderId:/ {print $2}'; }

wait_until() { # timeout_s description command...
  local t=$1 d=$2; shift 2; local end=$((SECONDS+t))
  until "$@"; do [ $SECONDS -ge $end ] && { fail "$d (timed out after ${t}s)"; return 0; }; sleep 2; done
  pass "$d"
}

set_rate() { # rate [late ooo dup high]
  curl -sf -X PUT "$DASH/api/generator/settings" -H 'Content-Type: application/json' -d "$(gen | python3 -c "
import json,sys; s=json.load(sys.stdin)['settings']; s['ratePerSecond']=$1
print(json.dumps(s))")" >/dev/null
}

under_replicated() { kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions | grep -c Partition || true; }

# make helpers available inside `bash -c` checks
export DASH BS
export -f snap gen pyq total genstat kt kexec leaders_of active_controller under_replicated
