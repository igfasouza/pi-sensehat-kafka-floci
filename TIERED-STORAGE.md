# Kafka Tiered Storage -> Floci S3

Wires Apache Kafka's tiered-storage feature to the Aiven `RemoteStorageManager` plugin (S3 backend), pointed at the local Floci S3 endpoint.

## 1. Fetch the plugin jars

Runs Maven on the host to pull `io.aiven:tiered-storage-for-apache-kafka-core` + `...-s3` and all transitive runtime jars into `server/kafka/plugins/tiered-storage/`, which the Kafka container mounts read-only at `/opt/kafka/plugins/tiered-storage`.

```bash
cd server/kafka/plugins
./fetch.sh
```

The plugin version is pinned in [server/kafka/plugins/pom.xml](server/kafka/plugins/pom.xml) via `tiered.storage.version`. Bump it and re-run `fetch.sh` to upgrade.

## 2. Start the stack

```bash
cd server
cp .env.example .env
docker compose up -d
```

The `bucket-init` service waits for Floci and creates `s3://kafka-tiered`. If you already had the stack running, restart Kafka so it picks up the tiered-storage env vars:

```bash
docker compose up -d --force-recreate kafka
```

## 3. Create the topic with remote storage enabled

```bash
./scripts/create-topic.sh
```

The script sets:

- `remote.storage.enable=true`  — this segment stream ships to S3
- `segment.bytes=1 MiB`         — tiny segments so tiering triggers fast in the lab
- `local.retention.ms=60000`    — keep 1 minute on the broker's local disk
- `retention.ms=-1`             — keep forever in remote storage

## 4. Verify

Produce some events (`pi/`), then wait ~60 s and check the bucket:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
aws --region us-east-1 --endpoint-url http://localhost:4566 \
    s3 ls s3://kafka-tiered/ --recursive
```

You should see per-partition prefixes and uploaded segment/index chunks. Broker logs will show `RemoteLogManager` copying segments; grep for `Copied` or `remote-log`:

```bash
docker logs sense-kafka 2>&1 | grep -i remote
```

## Broker configuration reference

All wired via `KAFKA_*` env vars in [server/docker-compose.yml](server/docker-compose.yml). Translated back to `server.properties`:

```properties
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=io.aiven.kafka.tieredstorage.RemoteStorageManager
remote.log.storage.manager.class.path=/opt/kafka/plugins/tiered-storage/*
remote.log.storage.manager.impl.prefix=rsm.config.

remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
remote.log.metadata.manager.listener.name=PLAINTEXT
rlmm.config.remote.log.metadata.topic.replication.factor=1
rlmm.config.remote.log.metadata.topic.num.partitions=5

rsm.config.chunk.size=4194304
rsm.config.storage.backend.class=io.aiven.kafka.tieredstorage.storage.s3.S3Storage
rsm.config.storage.s3.bucket.name=kafka-tiered
rsm.config.storage.s3.region=us-east-1
rsm.config.storage.s3.endpoint.url=http://floci:4566
rsm.config.storage.s3.path.style.access.enabled=true
rsm.config.storage.aws.access.key.id=test
rsm.config.storage.aws.secret.access.key=test
```

## Notes / caveats

- Replication factor 1 for the metadata topic is fine for a single-broker lab; production needs ≥3.
- The plugin exposes many more knobs (encryption, chunk cache, compression, fetch chunk cache size). See the [Aiven plugin docs](https://github.com/aiven/tiered-storage-for-apache-kafka) for the full list.
- If Kafka logs `NoClassDefFoundError` for AWS SDK classes, the `fetch.sh` step didn't pull transitive deps — re-run it and confirm the `tiered-storage/` directory has 40+ jars.
- Property names above match the pinned plugin version. Verify against release notes before bumping `tiered.storage.version`.
