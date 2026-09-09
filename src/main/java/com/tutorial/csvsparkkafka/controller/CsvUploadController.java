package com.tutorial.csvsparkkafka.controller;

import com.tutorial.csvsparkkafka.kafka.CsvRecordConsumer;
import com.tutorial.csvsparkkafka.service.CsvImportService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.KafkaException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * REST entry point for this tutorial.
 *
 * Endpoints:
 *   POST /api/csv/upload  -> upload a CSV file (multipart/form-data, field name "file")
 *   GET  /api/csv/status  -> see how many records the Kafka consumer has processed so far
 *
 * Try it with curl:
 *   curl -F "file=@src/main/resources/sample-data/employees.csv" http://localhost:8080/api/csv/upload
 *   curl http://localhost:8080/api/csv/status
 */
@RestController
@RequestMapping("/api/csv")
@RequiredArgsConstructor
@Slf4j
public class CsvUploadController {

    private final CsvImportService csvImportService;
    private final CsvRecordConsumer csvRecordConsumer;

    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> uploadCsv(HttpServletRequest request) {

        MultipartFile file = resolveUploadedFile(request);
        if (file == null || file.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "CSV file is required",
                    "hint", "POST multipart/form-data with the file in a form field named 'file'. "
                            + "Example: curl -F \"file=@src/main/resources/sample-data/employees.csv\" "
                            + "http://localhost:8080/api/csv/upload"
            ));
        }

        Path tempFile = null;
        try {
            // Spark reads files by PATH (it can read local paths, HDFS, S3, etc.),
            // so we first persist the uploaded multipart file to a temp location
            // on disk before handing the path to Spark.
            tempFile = Files.createTempFile("csv-upload-", ".csv");
            file.transferTo(tempFile);
            log.info("Saved uploaded file to temporary path: {}", tempFile);

            long publishedCount = csvImportService.importCsvAndPublish(tempFile.toAbsolutePath().toString());

            return ResponseEntity.ok(Map.of(
                    "message", "CSV processed and published to Kafka",
                    "recordsPublished", publishedCount
            ));

        } catch (KafkaException e) {
            log.error("Kafka broker is not reachable", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                    "error", "Kafka is not reachable at localhost:9092",
                    "hint", "Start a local broker: docker compose up -d"
            ));
        } catch (IOException e) {
            log.error("Failed to handle uploaded CSV file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to process uploaded file: " + e.getMessage()));
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {
                    // temp file cleanup is best-effort
                }
            }
        }
    }

    /**
     * Prefers the conventional form field "file", but also accepts any other
     * multipart file part (Postman/IntelliJ HTTP Client often use a different name).
     */
    private MultipartFile resolveUploadedFile(HttpServletRequest request) {
        if (!(request instanceof MultipartHttpServletRequest multipartRequest)) {
            return null;
        }
        MultipartFile named = multipartRequest.getFile("file");
        if (named != null && !named.isEmpty()) {
            return named;
        }
        return multipartRequest.getFileMap().values().stream()
                .filter(part -> part != null && !part.isEmpty())
                .findFirst()
                .orElse(named);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        // NOTE: publishing (producer) and consuming happen asynchronously.
        // Right after upload returns, the consumer count may not have caught
        // up yet — that's expected Kafka behavior, not a bug.
        return ResponseEntity.ok(Map.of(
                "recordsConsumedSoFar", csvRecordConsumer.getProcessedCount()
        ));
    }
}
