package com.tutorial.csvsparkkafka.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutorial.csvsparkkafka.model.Employee;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

import static com.tutorial.csvsparkkafka.config.KafkaTopicConfig.CSV_EMPLOYEE_TOPIC;

@Component
@RequiredArgsConstructor
@Slf4j
public class CsvRecordConsumer {

    private final ObjectMapper objectMapper;

    private final AtomicLong processedCount = new AtomicLong(0);

    @KafkaListener(topics = CSV_EMPLOYEE_TOPIC, groupId = "csv-import-consumer-group")
    public void consume(String message) {
        try {
            Employee employee = objectMapper.readValue(message, Employee.class);
            log.info("Consumed employee id={} name={} dept={}",
                    employee.getId(), employee.getName(), employee.getDepartment());

            processedCount.incrementAndGet();
        } catch (Exception e) {
            log.error("Failed to process Kafka message: {}", message, e);
        }
    }

    public long getProcessedCount() {
        return processedCount.get();
    }
}
