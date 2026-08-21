package io.example.sensehat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pi4j.Pi4J;
import com.pi4j.context.Context;
import com.pi4j.drivers.hat.raspberry.SenseHat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetAddress;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public final class SenseProducer {

    public static void main(String[] args) throws Exception {
        String broker = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("KAFKA_TOPIC", "sensehat");
        long intervalMs = (long) (Double.parseDouble(env("SENSE_INTERVAL_SECONDS", "1")) * 1000);
        String device = env("DEVICE_ID", InetAddress.getLocalHost().getHostName());

        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 10);

        Context pi4j = Pi4J.newAutoContext();
        SenseHat sense = new SenseHat(pi4j);
        LedStatus led = new LedStatus(sense);

        System.out.printf("Publishing %s readings to %s via %s%n", device, topic, broker);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                producer.flush();
                led.close();
            }));
            while (!Thread.currentThread().isInterrupted()) {
                double[] accel = sense.readAccelerometer();
                double tempC = round(sense.getTemperature(), 2);
                double humidity = round(sense.getHumidity(), 2);
                double pressure = round(sense.getPressure(), 2);

                Map<String, Object> event = new LinkedHashMap<>();
                event.put("device", device);
                event.put("timestamp", Instant.now().toString());
                event.put("temperature_c", tempC);
                event.put("humidity_pct", humidity);
                event.put("pressure_mbar", pressure);
                event.put("accel", Map.of(
                        "x", round(accel[0], 4),
                        "y", round(accel[1], 4),
                        "z", round(accel[2], 4)));

                String payload = mapper.writeValueAsString(event);
                try {
                    producer.send(new ProducerRecord<>(topic, device, payload)).get();
                    led.render(tempC, humidity, pressure);
                    System.out.println(payload);
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
