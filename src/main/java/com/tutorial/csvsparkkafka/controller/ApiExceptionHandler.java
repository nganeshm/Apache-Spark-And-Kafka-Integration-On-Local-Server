package com.tutorial.csvsparkkafka.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;

import java.util.Map;

/**
 * Turns Spring's opaque default 400 body (timestamp/status/error/path only)
 * into a message that tells the client how to call POST /api/csv/upload.
 *
 * The usual cause of that default 400 is a request that is not
 * multipart/form-data, or one whose file part is not named "file".
 */
@RestControllerAdvice
@Slf4j
public class ApiExceptionHandler {

    private static final String UPLOAD_HINT =
            "POST multipart/form-data with the file in a form field named 'file'. "
                    + "Example: curl -F \"file=@src/main/resources/sample-data/employees.csv\" "
                    + "http://localhost:8080/api/csv/upload";

    @ExceptionHandler({
            MultipartException.class,
            MissingServletRequestPartException.class
    })
    public ResponseEntity<Map<String, Object>> handleMultipart(Exception ex) {
        log.warn("Rejected CSV upload: {}", ex.getMessage());
        return ResponseEntity.badRequest().body(Map.of(
                "error", "CSV file is required",
                "hint", UPLOAD_HINT
        ));
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<Map<String, Object>> handleTooLarge(MaxUploadSizeExceededException ex) {
        return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(Map.of(
                "error", "Uploaded file is too large",
                "hint", "Maximum size is 50MB (see spring.servlet.multipart in application.yml)"
        ));
    }
}
