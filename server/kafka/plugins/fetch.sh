#!/usr/bin/env bash
set -euo pipefail

# Downloads the Aiven RemoteStorageManager (core + S3 backend) and all its
# transitive runtime dependencies into ./tiered-storage/, which the Kafka
# container mounts at /opt/kafka/plugins/tiered-storage.

cd "$(dirname "$0")"
rm -rf tiered-storage
mkdir -p tiered-storage

mvn -q -f pom.xml package

echo "Plugin jars in $(pwd)/tiered-storage:"
ls -1 tiered-storage | wc -l
