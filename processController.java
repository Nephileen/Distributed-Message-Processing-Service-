package uk.ac.ed.acp.cw2.controller;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static uk.ac.ed.acp.cw2.controller.KafkaUtils.getKafkaProperties;


@RestController
public class processController {

    private static final Logger logger = LoggerFactory.getLogger(processController.class);
    private final RuntimeEnvironment environment = RuntimeEnvironment.getEnvironment();

    @PostMapping("/processMessages")
    public ResponseEntity<String> processMessages(@RequestBody Map<String, Object> requestData) {
        try {
            String readTopic = (String) requestData.get("readTopic");
            String writeQueueGood = (String) requestData.get("writeQueueGood");
            String writeQueueBad = (String) requestData.get("writeQueueBad");
            int messageCount = (int) requestData.get("messageCount");

            //to check if its valid inputs
            if (readTopic == null || writeQueueGood == null || writeQueueBad == null || messageCount <= 0) {
                return ResponseEntity.badRequest().body("Missing or invalid input fields.");
            }

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaProperties(environment));
            consumer.subscribe(Collections.singletonList(readTopic));
            consumer.poll(Duration.ofMillis(100)); // wake up consumer

            List<JSONObject> goodPackets = new ArrayList<>();
            List<JSONObject> badPackets = new ArrayList<>();

            double runningTotal = 0.0;
            double badTotalVal = 0.0;
            AtomicInteger count = new AtomicInteger();

            outer:
            while (count.get() < messageCount) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (count.incrementAndGet() > messageCount) break outer;

                    JSONObject message = new JSONObject(record.value());

                    // Try using message.get("key") if key is not part of metadata
                    String key = message.optString("key", null);
                    message.put("comment", message.optString("comment", ""));

                    if (key == null || !message.has("value")) {
                        logger.warn("Missing key or value → {}", message);
                        badPackets.add(message);
                        continue;
                    }

                    double val = message.getDouble("value");

                    if (key.length() == 3 || key.length() == 4) {
                        runningTotal += val;
                        message.put("runningTotalValue", runningTotal);

                        String uuid = storeInAcpStorage(message);
                        message.put("uuid", uuid);
                        goodPackets.add(message);
                    } else {
                        badPackets.add(message);
                        badTotalVal += val;
                    }
                }
            }

            consumer.close();

            sendPackets(writeQueueGood, goodPackets);
            sendPackets(writeQueueBad, badPackets);

            // TOTAL messages
            JSONObject goodTotal = new JSONObject();
            goodTotal.put("uid", getUid(goodPackets));
            goodTotal.put("key", "TOTAL");
            goodTotal.put("comment", "");
            goodTotal.put("value", runningTotal);
            sendPackets(writeQueueGood, Collections.singletonList(goodTotal));

            JSONObject badTotal = new JSONObject();
            badTotal.put("uid", getUid(badPackets));
            badTotal.put("key", "TOTAL");
            badTotal.put("comment", "");
            badTotal.put("value", badTotalVal);

            sendPackets(writeQueueBad, Collections.singletonList(badTotal));
            return ResponseEntity.ok().build();

        } catch (Exception e) {
            logger.error("Failed to process messages: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error during processing: " + e.getMessage());
        }
    }

    private String getUid(List<JSONObject> packets) {
        return packets.isEmpty() ? "unknown" : packets.get(0).optString("uid", "unknown");
    }

    private String storeInAcpStorage(JSONObject message) {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String baseUrl = System.getenv("ACP_STORAGE_SERVICE");
            String endpoint = baseUrl + "/api/v1/blob";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(message.toString(), headers);

            ResponseEntity<String> response = restTemplate.exchange(endpoint, HttpMethod.POST, entity, String.class);
            return response.getBody().replace("\"", "");
        } catch (Exception e) {
            logger.error("Error storing in ACPstorage!!", e);
            return "error-uuid";
        }
    }

    private void sendPackets(String queueName, List<JSONObject> packets) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(environment.getRabbitmqHost());
            factory.setPort(environment.getRabbitmqPort());

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.queueDeclare(queueName, false, false, false, null);

                for (JSONObject packet : packets) {
                    channel.basicPublish("", queueName, null, packet.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to send to queue: {}", queueName, e);
        }
    }
}
