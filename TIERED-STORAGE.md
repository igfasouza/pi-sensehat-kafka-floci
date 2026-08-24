# Kafka Tiered Storage -> Iceberg (Aiven, alpha)

Wires Apache Kafka's tiered-storage feature to the Aiven `RemoteStorageManager`
plugin in **Iceberg mode** (alpha, added in
[v1.1.1](https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/releases/tag/v1.1.1)).
Tiered segments are materialized as Parquet files inside an Iceberg table,
served by a REST catalog and stored on the object-store backend of your choice
(**AWS S3**, **GCS** or **Azure ADLS Gen2**).

Values on the topic must be **Avro** — the RSM uses the schema registered in
Karapace to build the Iceberg schema.

## 1. Fetch the plugin jars

The Aiven release tarballs are attached to the GitHub release (not on Maven
Central). The Iceberg GCS/Azure SDK bundles come from Maven Central. `fetch.sh`
downloads all of them into `server/kafka/plugins/tiered-storage/`:

```bash
cd server/kafka/plugins
./fetch.sh
```

## 2. Pick a backend and start the stack

The stack is split into a base compose file and one overlay per cloud, selected
via `COMPOSE_FILE` in `.env`:

```bash
cd server
cp .env.example .env
# edit .env to pick s3 / gcs / azure
docker compose up -d
```

| Backend | Overlay file           | Emulator                        | Warehouse URI                                                          |
|---------|------------------------|---------------------------------|------------------------------------------------------------------------|
| AWS S3  | `docker-compose.s3.yml`    | Floci (`:4566`)                | `s3://kafka-tiered/`                                                   |
| GCS     | `docker-compose.gcs.yml`   | fake-gcs-server (`:4443`)      | `gs://kafka-tiered/`                                                   |
| Azure   | `docker-compose.azure.yml` | Azurite (`:10000`)             | `abfss://kafka-tiered@devstoreaccount1.dfs.core.windows.net/`          |

Common to every backend: Kafka on `:9092`, Karapace (Schema Registry) on
`:8081`, Iceberg REST catalog on `:8181`.

To switch backends:

```bash
docker compose down -v            # wipe local state (metadata is per-backend)
# edit COMPOSE_FILE in .env
docker compose up -d
```

If you just want to hot-swap without editing `.env`:

```bash
docker compose -f docker-compose.yml -f docker-compose.gcs.yml up -d
```

## 3. Create the topic with remote storage enabled

```bash
./scripts/create-topic.sh
```

Same tiered-storage config regardless of backend:

- `remote.storage.enable=true`
- `segment.bytes=1 MiB`
- `local.retention.ms=60000`
- `retention.ms=-1`

## 4. Verify

Produce with `pi/` (Avro via Karapace), wait ~60 s, and hit the REST catalog:

```bash
curl -s http://localhost:8181/v1/namespaces/default/tables | jq
```

You should see a table per tiered partition. Broker logs:

```bash
docker logs sense-kafka 2>&1 | grep -iE 'iceberg|remote'
```

Backend-specific checks:

```bash
# S3 / Floci
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --region us-east-1 --endpoint-url http://localhost:4566 \
      s3 ls s3://kafka-tiered/ --recursive

# GCS / fake-gcs-server
curl -s 'http://localhost:4443/storage/v1/b/kafka-tiered/o' | jq '.items[].name'

# Azure / Azurite
az storage blob list --container-name kafka-tiered \
  --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;" \
  --query '[].name' -o tsv
```

## Broker configuration reference

The cloud-neutral bits live in [server/docker-compose.yml](server/docker-compose.yml):

```properties
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=io.aiven.kafka.tieredstorage.RemoteStorageManager
remote.log.storage.manager.class.path=/opt/kafka/plugins/tiered-storage/*
remote.log.storage.manager.impl.prefix=rsm.config.

rsm.config.chunk.size=4194304
rsm.config.storage.backend.class=io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage
rsm.config.storage.root=/tmp/tiered-stub
rsm.config.structure.provider.class=io.aiven.kafka.tieredstorage.iceberg.AvroSchemaRegistryStructureProvider
rsm.config.structure.provider.serde.schema.registry.url=http://karapace:8081
rsm.config.segment.format=iceberg
rsm.config.iceberg.namespace=default
rsm.config.iceberg.catalog.class=org.apache.iceberg.rest.RESTCatalog
rsm.config.iceberg.catalog.uri=http://iceberg-rest:8181
```

Backend-specific bits (from each overlay):

**S3** — `docker-compose.s3.yml`

```properties
rsm.config.iceberg.catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
rsm.config.iceberg.catalog.warehouse=s3://kafka-tiered/
rsm.config.iceberg.catalog.s3.endpoint=http://floci:4566
rsm.config.iceberg.catalog.s3.path-style-access=true
rsm.config.iceberg.catalog.s3.access-key-id=test
rsm.config.iceberg.catalog.s3.secret-access-key=test
rsm.config.iceberg.catalog.client.region=us-east-1
```

**GCS** — `docker-compose.gcs.yml`

```properties
rsm.config.iceberg.catalog.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO
rsm.config.iceberg.catalog.warehouse=gs://kafka-tiered/
rsm.config.iceberg.catalog.gcs.project-id=fake-project
rsm.config.iceberg.catalog.gcs.service.host=http://gcs:4443
rsm.config.iceberg.catalog.gcs.no-auth=true
```

**Azure** — `docker-compose.azure.yml`

```properties
rsm.config.iceberg.catalog.io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO
rsm.config.iceberg.catalog.warehouse=abfss://kafka-tiered@devstoreaccount1.dfs.core.windows.net/
rsm.config.iceberg.catalog.adls.connection-string.devstoreaccount1=DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<well-known-key>;BlobEndpoint=http://azurite:10000/devstoreaccount1;
```

## Notes / caveats

- **Alpha feature.** No schema evolution, no Kafka transactions, no per-topic
  config, possible duplicate visibility on replication. Fine for the lab.
- Values must be Avro. Non-Avro will break the RSM copy phase.
- Replication factor 1 for the metadata topic is fine here; production needs ≥3.
- **Azurite** only partially implements the ADLS Gen2 DFS API. The smoke path
  works, but some HNS-specific ops may fail — swap for a real account when
  going beyond the lab.
- If Kafka logs `NoClassDefFoundError`, `fetch.sh` didn't unpack the tarballs.
  Confirm `tiered-storage/` has 50+ jars (iceberg-*, parquet-*, hadoop-common,
  kafka-avro-serializer, iceberg-gcp-bundle, iceberg-azure-bundle).
- Bumping backends re-uses the same Kafka data dir. If you switch backends and
  see stale-metadata errors, `docker compose down -v` and rebuild.
