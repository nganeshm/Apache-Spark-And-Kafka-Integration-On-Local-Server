package com.tutorial.csvsparkkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String CSV_EMPLOYEE_TOPIC = "csv-employee-records";

    @Bean
    public NewTopic csvEmployeeTopic() {
        return TopicBuilder.name(CSV_EMPLOYEE_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
