package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;


@RestController
public class RabbitMqController {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqController.class);
    private final RuntimeEnvironment environment;

    public RabbitMqController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    @GetMapping("/rabbitMq/{queueName}/{timeoutInMsec}")
    public ResponseEntity<List<String>> receiveMessagesFromQueue(
            @PathVariable String queueName,
            @PathVariable int timeoutInMsec) {

        if (timeoutInMsec <= 0) {
            return ResponseEntity.badRequest().body(List.of("timeoutInMsec must be greater than 0"));
        }

        String rabbitMqHost = System.getenv("RABBITMQ_HOST");
        int rabbitMqPort = Integer.parseInt(System.getenv("RABBITMQ_PORT"));
        List<String> receivedMessages = new ArrayList<>();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMqHost);
        factory.setPort(rabbitMqPort);

        long startTime = System.currentTimeMillis();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            while (System.currentTimeMillis() - startTime < timeoutInMsec) {
                GetResponse response = channel.basicGet(queueName, true);

                if (response != null) {
                    String message = new String(response.getBody());
                    receivedMessages.add(message);
                    logger.info("Received message from {}: {}", queueName, message);
                } else {
                    Thread.sleep(5);
                }
            }

            return ResponseEntity.ok(receivedMessages);

        } catch (Exception e) {
            logger.error("Error while receiving messages from {}: {}", queueName, e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(List.of("Error: " + e.getMessage()));
        }
    }

    @PutMapping("/rabbitMq/{queueName}/{messageCount}")
    public ResponseEntity<String> sendMessagesToQueue(
            @PathVariable String queueName,
            @PathVariable int messageCount) {

        if (messageCount <= 0) {
            return ResponseEntity.badRequest().body("messageCount must be greater than 0");
        }

        String rabbitMqHost = System.getenv("RABBITMQ_HOST");
        int rmqPort = Integer.parseInt(System.getenv("RABBITMQ_PORT"));
        String studentNum = "s2792472";

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMqHost);
        factory.setPort(rmqPort);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);
            ObjectMapper objectMapper = new ObjectMapper();

            for (int i = 0; i < messageCount; i++) {
                Map<String, Object> newMessage = new HashMap<>();
                newMessage.put("uid", studentNum);
                newMessage.put("counter", i);
                String jsonMessage = objectMapper.writeValueAsString(newMessage);
                channel.basicPublish("", queueName, null, jsonMessage.getBytes());
            }

            return ResponseEntity.ok().build();

        } catch (IOException | TimeoutException e) {
            logger.error("RabbitMQ connection/publish error to queue {}: {}", queueName, e.getMessage());
            return ResponseEntity.internalServerError()
                    .body("Connection error while sending messages to queue: " + queueName);
        }
    }
}
