/**
 * This program periodically pulls new Seattle 911 call records from the open data API
 * and publishes them into a Kafka topic for the speed layer.
 */
package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class StreamFire911IntoKafka {

    static String bootstrapServers =
            "boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196";

    static final String TOPIC = "mpcs53014_lucashou_fire911_v2";

    static final String DATASET_URL =
            "https://data.seattle.gov/api/v3/views/kzjm-xkqj/query.json";

    static final String API_KEY_ID = System.getenv("SEATTLE_API_KEY_ID");
    static final String API_SECRET = System.getenv("SEATTLE_API_SECRET_KEY");

    // Seattle timezone
    static final ZoneId SEATTLE_TZ = ZoneId.of("America/Los_Angeles");

    // Cutoff is interpreted as Seattle local time
    static final String CUTOFF_STR = "2025-11-30T20:23:00";
    static final Instant CUTOFF = LocalDateTime
            .parse(CUTOFF_STR, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .atZone(SEATTLE_TZ)
            .toInstant();

    static final DateTimeFormatter QUERY_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    // Task does the periodic polling logic and Kafka publishing logic
    static class Task extends TimerTask {
        private final Client client;
        private final KafkaProducer<String, String> producer;
        private final ObjectMapper mapper = new ObjectMapper();
        private Instant latestSeen = CUTOFF;

        // Initializes the HTTP client and Kafka producer using the shared client properties file
        Task() {
            client = ClientBuilder.newClient();
            client.register(JacksonFeature.class);

            Properties props = new Properties();
            try (FileInputStream fis = new FileInputStream("/home/hadoop/kafka.client.properties")) {
                props.load(fis);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load Kafka client properties", e);
            }

            props.put("bootstrap.servers", bootstrapServers);
            props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");

            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);

            producer = new KafkaProducer<>(props);
        }

        // Queries the Seattle 911 API for calls with datetime greater than the latestSeen value.
        // Convert latestSeen to a Seattle-local time string that matches
        // the dataset's "datetime" format.
        private JsonNode fetchNewCalls() {
            String since = LocalDateTime
                    .ofInstant(latestSeen, SEATTLE_TZ)
                    .format(QUERY_FMT);

            String sql = "select * where datetime > '" + since + "' " +
                    "order by datetime limit 5000";

            Invocation.Builder bldr = client
                    .target(DATASET_URL)
                    .queryParam("query", sql)
                    .request("application/json");

            if (API_KEY_ID != null && !API_KEY_ID.isEmpty()
                    && API_SECRET != null && !API_SECRET.isEmpty()) {
                String creds = API_KEY_ID + ":" + API_SECRET;
                String encoded = Base64.getEncoder()
                        .encodeToString(creds.getBytes(StandardCharsets.UTF_8));
                bldr = bldr.header("Authorization", "Basic " + encoded);
            }

            try {
                String json = bldr.get(String.class);
                return mapper.readTree(json);
            } catch (Exception e) {
                System.err.println("Error fetching 911 calls: " + e.getMessage());
                return null;
            }
        }

        private Instant parseInstant(String dtStr) {
            if (dtStr == null) return null;
            try {
                LocalDateTime ldt = LocalDateTime.parse(dtStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                return ldt.atZone(SEATTLE_TZ).toInstant();
            } catch (Exception e) {
                System.err.println("Failed to parse datetime: " + dtStr + " -> " + e.getMessage());
                return null;
            }
        }

        // run() is called on each timer tick, pulls any new events, filters them after the cutoff,
        // and publishes them to the Kafka topic while updating latestSeen
        @Override
        public void run() {
            Instant now = Instant.now();
            LocalDateTime latestLocal = LocalDateTime.ofInstant(latestSeen, SEATTLE_TZ);

            System.out.println("Polling Seattle 911 at " + now
                    + " (Seattle local: " + LocalDateTime.ofInstant(now, SEATTLE_TZ) + ")"
                    + " latestSeen=" + latestSeen
                    + " (Seattle local: " + latestLocal + ")");

            JsonNode root = fetchNewCalls();
            if (root == null || !root.isArray()) {
                System.out.println("No data (null or non-array)");
                return;
            }
            if (root.size() == 0) {
                System.out.println("No new calls since " + latestSeen
                        + " (Seattle local: " + latestLocal + ")");
                return;
            }

            System.out.println("Fetched " + root.size() + " new 911 calls");

            for (JsonNode call : root) {
                try {
                    String type = call.path("type").asText(null);
                    String datetime = call.path("datetime").asText(null);
                    String latitude = call.path("latitude").asText(null);
                    String longitude = call.path("longitude").asText(null);
                    String incidentNumber = call.path("incident_number").asText(null);

                    if (incidentNumber == null || incidentNumber.isEmpty()
                            || datetime == null) {
                        continue;
                    }

                    Instant eventTime = parseInstant(datetime);
                    if (eventTime == null || !eventTime.isAfter(CUTOFF)) {
                        continue;
                    }

                    ObjectNode out = mapper.createObjectNode();
                    out.put("incident_number", incidentNumber);
                    if (type != null) {
                        out.put("type", type);
                    }
                    // Store the original Seattle-local datetime string in Kafka
                    out.put("datetime", datetime);
                    if (latitude != null && !latitude.isEmpty()) {
                        out.put("latitude", latitude);
                    }
                    if (longitude != null && !longitude.isEmpty()) {
                        out.put("longitude", longitude);
                    }

                    String value = mapper.writeValueAsString(out);

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, incidentNumber, value);

                    producer.send(record, (meta, ex) -> {
                        if (ex == null) {
                            System.out.println("Record " + incidentNumber
                                    + " sent at offset " + meta.offset());
                        } else {
                            System.err.println("Error sending to Kafka");
                            ex.printStackTrace(System.err);
                        }
                    });

                    // Advance latestSeen if this event is newer
                    if (eventTime.isAfter(latestSeen)) {
                        latestSeen = eventTime;
                    }

                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // main() starts the periodic Task on a fixed 5 minute interval
    public static void main(String[] args) {
        if (args.length > 0) {
            bootstrapServers = args[0];
        }
        System.out.println("Using Kafka bootstrap servers: " + bootstrapServers);
        System.out.println("Starting Seattle 911 → Kafka producer; cutoff T (Instant) = " + CUTOFF
                + " (Seattle local: " + LocalDateTime.ofInstant(CUTOFF, SEATTLE_TZ) + ")");

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new Task(), 0, 300_000);
    }
}