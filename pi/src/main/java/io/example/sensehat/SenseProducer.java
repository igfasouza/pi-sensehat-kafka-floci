package io.example.sensehat;

import com.pi4j.Pi4J;
import com.pi4j.context.Context;
import com.pi4j.drivers.hat.raspberry.SenseHat;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetAddress;
import java.util.Properties;

public final class SenseProducer {

    private static final String SCHEMA_JSON = """
            {
              "type": "record",
              "namespace": "io.example.sensehat",
              "name": "SenseReading",
              "fields": [
                {"name": "device",        "type": "string"},
                {"name": "timestamp_ms",  "type": "long", "logicalType": "timestamp-millis"},
                {"name": "temperature_c", "type": "double"},
                {"name": "humidity_pct",  "type": "double"},
                {"name": "pressure_mbar", "type": "double"},
                {"name": "accel_x",       "type": "double"},
                {"name": "accel_y",       "type": "double"},
                {"name": "accel_z",       "type": "double"}
              ]
            }
            """;

    public static void main(String[] args) throws Exception {
        String broker = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String schemaRegistry = env("SCHEMA_REGISTRY_URL", "http://localhost:8081");
        String topic = env("KAFKA_TOPIC", "sensehat");
        long intervalMs = (long) (Double.parseDouble(env("SENSE_INTERVAL_SECONDS", "1")) * 1000);
        String device = env("DEVICE_ID", InetAddress.getLocalHost().getHostName());

        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 10);

        Context pi4j = Pi4J.newAutoContext();
        SenseHat sense = new SenseHat(pi4j);
        LedStatus led = new LedStatus(sense);

        System.out.printf("Publishing %s readings to %s via %s (SR: %s)%n",
                device, topic, broker, schemaRegistry);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                producer.flush();
                led.close();
            }));
            while (!Thread.currentThread().isInterrupted()) {
                double[] accel = sense.readAccelerometer();
                double tempC = round(sense.getTemperature(), 2);
                double humidity = round(sense.getHumidity(), 2);
                double pressure = round(sense.getPressure(), 2);

                GenericRecord event = new GenericData.Record(schema);
                event.put("device", device);
                event.put("timestamp_ms", System.currentTimeMillis());
                event.put("temperature_c", tempC);
                event.put("humidity_pct", humidity);
                event.put("pressure_mbar", pressure);
                event.put("accel_x", round(accel[0], 4));
                event.put("accel_y", round(accel[1], 4));
                event.put("accel_z", round(accel[2], 4));

                try {
                    producer.send(new ProducerRecord<>(topic, device, event)).get();
                    led.render(tempC, humidity, pressure);
                    System.out.println(event);
                } catch (Exception e) {
                    led.signalError();
                    System.err.println("send failed: " + e.getMessage());
                }

                Thread.sleep(intervalMs);
            }
        } finally {
            led.close();
            pi4j.shutdown();
        }
    }

    private static double round(double value, int decimals) {
        double factor = Math.pow(10, decimals);
        return Math.round(value * factor) / factor;
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
