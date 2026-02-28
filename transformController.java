package uk.ac.ed.acp.cw2.controller;

import com.rabbitmq.client.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@RestController
public class transformController {

    private static final Logger logger = LoggerFactory.getLogger(transformController.class);

    @Autowired
    private StringRedisTemplate redisTemplate;

    private final RuntimeEnvironment environment = RuntimeEnvironment.getEnvironment();

    @PostMapping("/transformMessages")
    public ResponseEntity<String> transformMessages(@RequestBody Map<String, Object> body) {
        String readQueue = (String) body.get("readQueue");
        String writeQueue = (String) body.get("writeQueue");
        int messageCount = (int) body.get("messageCount");

        int totalMessagesWritten = 0;
        int totalMessagesProcessed = 0;
        int totalRedisUpdates = 0;
        double totalValueWritten = 0.0;
        double totalAdded = 0.0;

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(environment.getRabbitmqHost());
            factory.setPort(environment.getRabbitmqPort());

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                 channel.queueDeclare(writeQueue, false, false, false, null);

                for (int i = 0; i < messageCount; i++) {
                    GetResponse response = channel.basicGet(readQueue, true);
                    if (response == null) continue;

                    String messageBody = new String(response.getBody(), StandardCharsets.UTF_8);
                    System.out.println("Received message: " + messageBody);

                    JSONObject message = new JSONObject(messageBody);
                    totalMessagesProcessed++;

                    if (message.has("version") && message.has("value")) {
                        // Normal message
                        String key = message.optString("key", null);
                        if (key == null) {
                            logger.warn("Skipping the message missing 'key': {}", message);
                            continue;
                        }

                        int version = message.getInt("version");
                        double value = message.getDouble("value");

                        String redisVal = redisTemplate.opsForValue().get(key);
                        boolean shouldUpdate = redisVal == null || Integer.parseInt(redisVal) < version;

                        if (shouldUpdate) {
                            redisTemplate.opsForValue().set(key, String.valueOf(version));

                            message.put("value", value + 10.5);
                            totalAdded += 10.5;
                            totalRedisUpdates++;
                        }

                        totalValueWritten += message.getDouble("value");
                        channel.basicPublish("", writeQueue, null, message.toString().getBytes(StandardCharsets.UTF_8));
                        totalMessagesWritten++;

                    } else if (message.has("key")) {
                        // Tombstone message
                        String key = message.getString("key");
                        redisTemplate.delete(key);

                        JSONObject summary = new JSONObject();
                        summary.put("totalMessagesWritten", totalMessagesWritten);
                        summary.put("totalMessagesProcessed", totalMessagesProcessed);
                        summary.put("totalRedisUpdates", totalRedisUpdates);
                        summary.put("totalValueWritten", totalValueWritten);
                        summary.put("totalAdded", totalAdded);

                        channel.basicPublish("", writeQueue, null, summary.toString().getBytes(StandardCharsets.UTF_8));
                        totalMessagesWritten++;

                    } else {
                        logger.warn("Message is not valid bc missing required fields): {}", message);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error in /transformMessages: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("Failed to transform messages: " + e.getMessage());
        }

        return ResponseEntity.ok().build();
    }
}
