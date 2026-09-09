package com.tutorial.csvsparkkafka.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutorial.csvsparkkafka.model.Employee;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static com.tutorial.csvsparkkafka.config.KafkaTopicConfig.CSV_EMPLOYEE_TOPIC;

/**
 * Thin wrapper around Spring's KafkaTemplate.
 *
 * We serialize the Employee object to a JSON String ourselves (rather than
 * configuring a JsonSerializer<Employee> at the Kafka producer-factory level)
 * so that the topic carries plain, human-readable JSON. That makes it easy
 * to inspect messages with generic tools like `kafka-console-consumer.sh`
 * without needing any Java-specific deserializer on the reading side.
 */
@Component
@RequiredArgsConstructor // Lombok generates a constructor for the final fields below (constructor injection)
@Slf4j
public class CsvRecordProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper; // Spring Boot auto-configures this bean for us

    /**
     * Publishes one Employee record to the "csv-employee-records" topic.
     *
     * We use the Employee's id as the Kafka message KEY. This matters:
     * Kafka guarantees ordering only *within* a partition, and the partition
     * is chosen (by default) via a hash of the key. Keying by employee id
     * means all messages for the same employee always land on the same
     * partition and are processed in order relative to each other.
     */
    public void publish(Employee employee) {
        try {
            String payload = objectMapper.writeValueAsString(employee);
            kafkaTemplate.send(CSV_EMPLOYEE_TOPIC, employee.getId(), payload);
            log.info("Published employee id={} to topic '{}'", employee.getId(), CSV_EMPLOYEE_TOPIC);
        } catch (JsonProcessingException e) {
            // In production you'd likely route failures like this to a dead-letter
            // mechanism instead of just logging — kept simple here for the tutorial.
            log.error("Failed to serialize employee id={} to JSON", employee.getId(), e);
        }
    }
}
