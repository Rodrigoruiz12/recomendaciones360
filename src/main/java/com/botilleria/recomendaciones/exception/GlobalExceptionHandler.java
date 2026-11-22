package com.botilleria.recomendaciones.exception;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleException(Exception ex, HttpServletRequest request) {
        Throwable root = unwrap(ex);

        if (root instanceof UnknownTopicOrPartitionException || root instanceof NoSuchElementException) {
            return buildResponse(HttpStatus.NOT_FOUND, "Topico no encontrado en Kafka: " + root.getMessage(), request);
        }
        if (root instanceof TopicExistsException) {
            return buildResponse(HttpStatus.CONFLICT, "El topico ya existe en Kafka: " + root.getMessage(), request);
        }
        if (root instanceof IllegalArgumentException || root instanceof InvalidTopicException) {
            return buildResponse(HttpStatus.BAD_REQUEST, root.getMessage(), request);
        }

        return buildResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Error en ms-recomendaciones: " + root.getMessage(),
                request
        );
    }

    private Throwable unwrap(Throwable ex) {
        Throwable current = ex;
        while (current instanceof ExecutionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private ResponseEntity<Map<String, Object>> buildResponse(HttpStatus status, String message, HttpServletRequest request) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", LocalDateTime.now().toString());
        body.put("status", status.value());
        body.put("error", status.getReasonPhrase());
        body.put("message", message);
        body.put("path", request.getRequestURI());
        return ResponseEntity.status(status).body(body);
    }
}
