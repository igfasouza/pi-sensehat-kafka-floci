# Raspberry Pi 4 + Sense HAT + Kafka + Floci

Starter lab for Raspberry Pi 4 (8 GB):

Sense HAT -> Kafka -> consumers, with Floci providing a local S3-compatible endpoint for a later Kafka Tiered Storage step.

## Hardware
- Raspberry Pi 4, 8 GB
- Sense HAT
- Raspberry Pi OS 64-bit
- USB 3 SSD recommended for Kafka/Floci data

## 1. Install host dependencies

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk maven docker.io docker-compose-plugin
sudo usermod -aG docker,i2c,gpio,input $USER
```

Log out/in after group changes. The Sense HAT talks over I2C, so make sure I2C is enabled (`sudo raspi-config`).

## 2. Start infrastructure

```bash
cd server/kafka/plugins && ./fetch.sh && cd -
cd server
cp .env.example .env
docker compose up -d
```

Kafka: `localhost:9092`
Floci: `http://localhost:4566`

The `bucket-init` sidecar creates `s3://kafka-tiered` in Floci automatically.

## 3. Create Kafka topic

```bash
./scripts/create-topic.sh
```

## 4. Run Sense HAT producer on the Pi host

The producer is a Java app built with Maven. It uses the [pi4j-drivers `SenseHat`](https://github.com/igfasouza/pi4j-drivers/blob/main/src/main/java/com/pi4j/drivers/hat/raspberry/SenseHat.java) driver (resolved via JitPack).

```bash
cd pi
mvn -q package
sudo -E java -jar target/sense-producer.jar
```

`sudo` is typically required for I2C access unless your user has the right group membership. The producer sends temperature, humidity, pressure and accelerometer readings to `sensehat`.

### LED matrix status display

The producer drives the Sense HAT's 8x8 LED matrix as a live gauge so you can see the state of the pipeline without a terminal. Implemented in [pi/src/main/java/io/example/sensehat/LedStatus.java](pi/src/main/java/io/example/sensehat/LedStatus.java), refreshed once per successful publish.

Layout — three vertical bars, each 2 columns wide with a 1-column gap:

| Columns | Reading      | Color | Range (default) |
|---------|--------------|-------|-----------------|
| 0–1     | Temperature  | Red   | 15–35 °C        |
| 3–4     | Humidity     | Blue  | 0–100 %         |
| 6–7     | Pressure     | Green | 950–1050 mbar   |

Each bar lights 0–8 rows from the bottom, proportional to the current reading clamped to the range above. Values outside the range peg at empty or full.

On a Kafka publish failure the matrix is replaced by a red `E` (`sense.showCharacter('E', ...)`) and stays that way until the next successful publish redraws the bars — so a stuck `E` means the producer can't reach the broker. The matrix is cleared on shutdown.

Tune the ranges by editing the `TEMP_MIN`/`TEMP_MAX`, `HUMIDITY_MIN`/`HUMIDITY_MAX`, `PRESSURE_MIN`/`PRESSURE_MAX` constants in `LedStatus`. Note that the Sense HAT sits on top of the Pi CPU, so its temperature reading typically runs 5–10 °C above ambient.

## 5. Watch events

In another shell:

```bash
cd consumer
mvn -q package
java -jar target/live-consumer.jar
```

## 6. Check Floci S3

Install AWS CLI if needed, then:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
aws --region us-east-1 --endpoint-url http://localhost:4566 s3 mb s3://kafka-tiered

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
aws --region us-east-1 --endpoint-url http://localhost:4566 s3 ls
```

## Tiered Storage

Kafka is wired to the Aiven [`RemoteStorageManager`](https://github.com/aiven/tiered-storage-for-apache-kafka) plugin with an S3 backend that points at Floci. Fetch the plugin jars before first `docker compose up`:

```bash
cd server/kafka/plugins
./fetch.sh
```

`create-topic.sh` creates `sensehat` with `remote.storage.enable=true`, 1 MiB segments and 60 s local retention, so segments start shipping to `s3://kafka-tiered/` within a minute of producing. See [TIERED-STORAGE.md](TIERED-STORAGE.md) for the full config reference and troubleshooting.
