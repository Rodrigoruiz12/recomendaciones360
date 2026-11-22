package com.botilleria.recomendaciones;

import com.botilleria.recomendaciones.exception.GlobalExceptionHandler;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class GlobalExceptionHandlerTest {

    @Test
    void debeDevolver404CuandoTopicoNoExisteEnKafka() {
        GlobalExceptionHandler handler = new GlobalExceptionHandler();
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/kafka/topics/no.existe");

        ExecutionException wrapped = new ExecutionException(new UnknownTopicOrPartitionException("This server does not host this topic-partition."));
        ResponseEntity<Map<String, Object>> resp = handler.handleException(wrapped, req);

        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
        assertEquals(404, resp.getBody().get("status"));
    }

    @Test
    void debeDevolver409CuandoTopicoYaExisteEnKafka() {
        GlobalExceptionHandler handler = new GlobalExceptionHandler();
        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/api/kafka/topics");

        ExecutionException wrapped = new ExecutionException(new TopicExistsException("Topic already exists."));
        ResponseEntity<Map<String, Object>> resp = handler.handleException(wrapped, req);

        assertEquals(HttpStatus.CONFLICT, resp.getStatusCode());
        assertEquals(409, resp.getBody().get("status"));
    }
}
