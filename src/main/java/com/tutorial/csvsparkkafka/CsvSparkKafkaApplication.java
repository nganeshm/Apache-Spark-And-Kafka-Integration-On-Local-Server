package com.tutorial.csvsparkkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *
 * The end-to-end flow this project demonstrates:
 *
 *   1) Client uploads a CSV file to  POST /api/csv/upload
 *   2) CsvUploadController saves it temporarily and hands it to CsvImportService
 *   3) CsvImportService uses a Spark SparkSession to read + lightly transform
 *      the CSV into a Dataset<Row> (Spark's in-memory, distributed table)
 *   4) Each row is converted to an Employee POJO and published as a JSON
 *      message onto a Kafka topic via KafkaTemplate (CsvRecordProducer)
 *   5) A @KafkaListener (CsvRecordConsumer) picks the message back up
 *      asynchronously and "processes" it (here: logs it + updates an
 *      in-memory counter you can query at GET /api/csv/status)
 *
 * Why combine Spark + Kafka at all?
 *   Spark gives you a powerful, distributed way to parse/clean/transform large
 *   CSV files (schema inference, filtering, aggregation) BEFORE the data ever
 *   hits Kafka. Kafka then decouples "the file got imported" from "the data
 *   got consumed/persisted downstream" — other services can subscribe to the
 *   same topic independently. This pattern scales from a demo CSV to a
 *   multi-gigabyte nightly batch load without changing the architecture.
 */
@SpringBootApplication
public class CsvSparkKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(CsvSparkKafkaApplication.class, args);
    }
}
