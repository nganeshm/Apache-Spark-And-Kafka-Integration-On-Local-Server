package com.tutorial.csvsparkkafka.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class SparkConfig {

    @Value("${spark.app-name:csv-spark-kafka-tutorial}")
    private String appName;

    @Value("${spark.master:local[*]}")
    private String masterUrl;

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName(appName)
                .master(masterUrl)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
    }
}
