package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.time.Duration;
import java.util.*;

import org.apache.kafka.common.TopicPartition;

import java.util.stream.Collectors;
import static uk.ac.ed.acp.cw2.controller.KafkaUtils.getKafkaProperties;


@RestController()
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final RuntimeEnvironment environment;

    public KafkaController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    @PutMapping("/kafka/{writeTopic}/{messageCount}")
    public ResponseEntity<String> sendKafkaMessages(
            @PathVariable String writeTopic,
            @PathVariable int messageCount) {

        if (messageCount <= 0) {
            return ResponseEntity.badRequest().body("messageCount must be greater than 0");
        }

        logger.info("Writing {} messages to Kafka topic: {}", messageCount, writeTopic);

        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS"); // e.g. "localhost:9092"
        String studentId = "s2792472";

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ObjectMapper objectMapper = new ObjectMapper();

            for (int i = 0; i < messageCount; i++) {
                Map<String, Object> message = new HashMap<>();
                message.put("uid", studentId);
                message.put("counter", i);

                String json = objectMapper.writeValueAsString(message);
                producer.send(new ProducerRecord<>(writeTopic, json));
                logger.info("Kafka - Sent to topic {} :: {}", writeTopic, json);
            }

            producer.flush(); // Ensure everything is sent
            return ResponseEntity.ok().build();

        } catch (Exception e) {
            logger.error("Failed to send messages to Kafka: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body("Kafka send error: " + e.getMessage());
        }
    }

    @GetMapping("/kafka/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<List<String>> getMessKafka(
            @PathVariable String readTopic,
            @PathVariable int timeoutInMsec) {

        if (timeoutInMsec <= 0) {
            return ResponseEntity.badRequest().body(Collections.singletonList("timeoutInMsec must be greater than 0"));
        }

        logger.info("RN Reading from Kafka topic '{}' for {} ms", readTopic, timeoutInMsec);
        Properties kafkaProps = getKafkaProperties(environment);
        List<String> mess = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
            // warming up all partitions for the topic
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(readTopic);
            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(pi -> new TopicPartition(readTopic, pi.partition()))
                    .collect(Collectors.toList());

            logger.debug("Assigned to partitions: {}", partitions);

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            long upperBound = System.currentTimeMillis() + timeoutInMsec;
            int emptyNum = 0;
            int maxEmpty = 2;
            while (System.currentTimeMillis() < upperBound && emptyNum < maxEmpty) {
                long tLeft = upperBound - System.currentTimeMillis();
                long pollMs = Math.min(50, tLeft);
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(pollMs));

                if (recs.isEmpty()) {
                    emptyNum++;
                } else {
                    emptyNum = 0;
                    for (ConsumerRecord<String, String> r : recs) {
                        mess.add(r.value());
                    }
                }
            }

            long elapsed = System.currentTimeMillis() - (upperBound - timeoutInMsec);
            logger.info("Retrieved {} messages from '{}' in {} ms", mess.size(), readTopic, elapsed);
            return ResponseEntity.ok(mess);

        } catch (Exception e) {
            logger.error("did not read from Kafka topic", e);
            return ResponseEntity.status(500).body(Collections.emptyList());
        }
    }
}
