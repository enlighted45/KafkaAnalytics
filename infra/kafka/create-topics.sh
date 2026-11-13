#!/bin/bash
# Creates all application topics with explicit config. Idempotent (--if-not-exists).
set -euo pipefail
BS="${BOOTSTRAP:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
KT=/opt/kafka/bin/kafka-topics.sh

echo "Waiting for 3 brokers..."
for i in $(seq 1 60); do
  n=$(/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BS" 2>/dev/null | grep -c '^kafka-' || true)
  [ "$n" -ge 3 ] && break
  sleep 2
done
echo "Brokers up: $n"

# Exactly-once (Kafka Streams) uses transactions. Transactions v2 (KIP-890, default on new 4.x clusters)
# produced OUT_OF_ORDER_SEQUENCE_NUMBER / CONCURRENT_TRANSACTIONS retry storms on the Streams changelogs
# under load in our tests, stalling processing. Pin the cluster to the mature transactions v1 protocol.
FEAT=/opt/kafka/bin/kafka-features.sh
tv=$($FEAT --bootstrap-server "$BS" describe | awk '$2=="transaction.version" {for(i=1;i<=NF;i++) if($i=="FinalizedVersionLevel:") print $(i+1)}')
if [ "${tv:-0}" -gt 1 ]; then
  $FEAT --bootstrap-server "$BS" downgrade --feature transaction.version=1
fi
echo "transaction.version: $($FEAT --bootstrap-server "$BS" describe | awk '$2=="transaction.version" {for(i=1;i<=NF;i++) if($i=="FinalizedVersionLevel:") print $(i+1)}')"

create() { # name partitions [extra --config args]
  local name=$1 parts=$2; shift 2
  $KT --bootstrap-server "$BS" --create --if-not-exists --topic "$name" \
      --partitions "$parts" --replication-factor 3 --config min.insync.replicas=2 "$@"
}
create transactions.raw   6 --config retention.ms=86400000
create transactions.late  3 --config retention.ms=604800000
create transactions.dlq   3 --config retention.ms=604800000
create metrics.windowed   3 --config retention.ms=3600000
create alerts             3 --config retention.ms=86400000

$KT --bootstrap-server "$BS" --describe --topic transactions.raw
echo "Topics ready."
