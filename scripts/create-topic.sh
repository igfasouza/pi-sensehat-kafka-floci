#!/usr/bin/env bash
set -euo pipefail

docker exec sense-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic sensehat \
  --partitions 3 \
  --replication-factor 1 \
  --config remote.storage.enable=true \
  --config segment.bytes=1048576 \
  --config local.retention.ms=60000 \
  --config retention.ms=-1

docker exec sense-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic sensehat
