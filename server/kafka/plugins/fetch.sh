#!/usr/bin/env bash
set -euo pipefail

# Downloads the Aiven RemoteStorageManager release artifacts plus the Iceberg
# GCS/Azure bundle jars into ./tiered-storage/, which the Kafka container
# mounts at /opt/kafka/plugins/tiered-storage.
#
# - core: the RSM itself + Iceberg + AWS SDK + Parquet + Hadoop deps
# - filesystem: storage-backend stub required by the RSM
# - iceberg-gcp-bundle: GCSFileIO + GCS SDK (shaded)
# - iceberg-azure-bundle: ADLSFileIO + Azure SDK (shaded)

VERSION="${TIERED_STORAGE_VERSION:-1.1.1}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.11.0}"
BASE_URL="https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/releases/download/v${VERSION}"
MAVEN_CENTRAL="https://repo1.maven.org/maven2"

cd "$(dirname "$0")"
rm -rf tiered-storage
mkdir -p tiered-storage

fetch_tgz() {
  local name="$1"
  local url="${BASE_URL}/${name}-${VERSION}.tgz"
  echo "-> ${url}"
  curl -fsSL "${url}" | tar -xz -C tiered-storage --strip-components=1
}

fetch_jar() {
  local group_path="$1"
  local artifact="$2"
  local version="$3"
  local url="${MAVEN_CENTRAL}/${group_path}/${artifact}/${version}/${artifact}-${version}.jar"
  echo "-> ${url}"
  curl -fsSL -o "tiered-storage/${artifact}-${version}.jar" "${url}"
}

fetch_tgz core
fetch_tgz filesystem
fetch_jar org/apache/iceberg iceberg-gcp-bundle "${ICEBERG_VERSION}"
fetch_jar org/apache/iceberg iceberg-azure-bundle "${ICEBERG_VERSION}"

echo "Plugin jars in $(pwd)/tiered-storage: $(ls -1 tiered-storage | wc -l)"
