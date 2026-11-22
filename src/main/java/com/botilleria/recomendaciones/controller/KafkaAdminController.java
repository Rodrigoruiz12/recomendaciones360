package com.botilleria.recomendaciones.controller;

import com.botilleria.recomendaciones.service.KafkaAdminService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Controlador REST Administrador de Topicos Kafka (AdminClient).
 * Accesible en /api/kafka/topics y /api/recomendaciones/kafka/topics.
 */
@RestController
@RequestMapping({"/api/kafka", "/api/recomendaciones/kafka"})
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping("/cluster")
    public ResponseEntity<Map<String, Object>> obtenerCluster() throws Exception {
        return ResponseEntity.ok(kafkaAdminService.obtenerInfoCluster());
    }

    @GetMapping("/topics")
    public ResponseEntity<List<Map<String, Object>>> listarTopicos() throws Exception {
        return ResponseEntity.ok(kafkaAdminService.listarTopicos());
    }

    @GetMapping("/topics/{nombre}")
    public ResponseEntity<Map<String, Object>> describirTopico(@PathVariable String nombre) throws Exception {
        return ResponseEntity.ok(kafkaAdminService.describirTopico(nombre));
    }

    @PostMapping("/topics")
    @SuppressWarnings("unchecked")
    public ResponseEntity<Map<String, Object>> crearTopico(@RequestBody Map<String, Object> body) throws Exception {
        String nombre = String.valueOf(body.getOrDefault("nombre", "")).trim();
        int particiones = body.get("particiones") instanceof Number n ? n.intValue() : 3;
        short replicas = body.get("factorReplicacion") instanceof Number n ? n.shortValue() : 3;
        Map<String, String> configs = body.get("configuraciones") instanceof Map<?, ?> map
                ? (Map<String, String>) map
                : Map.of();
        return ResponseEntity.ok(kafkaAdminService.crearTopico(nombre, particiones, replicas, configs));
    }

    @PutMapping("/topics/{nombre}/config")
    public ResponseEntity<Map<String, Object>> modificarConfiguracion(
            @PathVariable String nombre,
            @RequestBody Map<String, String> configuraciones) throws Exception {
        return ResponseEntity.ok(kafkaAdminService.modificarConfiguracion(nombre, configuraciones));
    }

    @PutMapping("/topics/{nombre}/partitions")
    public ResponseEntity<Map<String, Object>> aumentarParticiones(
            @PathVariable String nombre,
            @RequestBody Map<String, Integer> body) throws Exception {
        int particiones = body.getOrDefault("particiones", 3);
        return ResponseEntity.ok(kafkaAdminService.aumentarParticiones(nombre, particiones));
    }

    @DeleteMapping("/topics/{nombre}")
    public ResponseEntity<Map<String, Object>> eliminarTopico(@PathVariable String nombre) throws Exception {
        return ResponseEntity.ok(kafkaAdminService.eliminarTopico(nombre));
    }
}
