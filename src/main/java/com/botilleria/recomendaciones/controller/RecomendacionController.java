package com.botilleria.recomendaciones.controller;

import com.botilleria.recomendaciones.model.EventoAnalitico;
import com.botilleria.recomendaciones.model.RecomendacionItem;
import com.botilleria.recomendaciones.service.DynamoDbService;
import com.botilleria.recomendaciones.service.RecomendacionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/recomendaciones")
public class RecomendacionController {

    private final RecomendacionService recomendacionService;
    private final DynamoDbService dynamoDbService;
    private final ObjectMapper objectMapper;

    @Autowired(required = false)
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.enabled:true}")
    private boolean kafkaEnabled;

    @Value("${app.kafka.topics.productos:productos.eventos}")
    private String topicProductos;

    @Value("${app.kafka.topics.carrito:carrito.eventos}")
    private String topicCarrito;

    public RecomendacionController(
            RecomendacionService recomendacionService,
            DynamoDbService dynamoDbService,
            ObjectMapper objectMapper) {
        this.recomendacionService = recomendacionService;
        this.dynamoDbService = dynamoDbService;
        this.objectMapper = objectMapper;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("servicio", "ms-recomendaciones");
        status.put("estado", "UP");
        status.put("kafkaHabilitado", kafkaEnabled);
        status.put("topicos", List.of(topicProductos, topicCarrito, "pagos.alertas"));
        status.put("dynamoDbActivo", dynamoDbService.isActivo());
        status.put("dynamoDbTabla", dynamoDbService.getTableName());
        status.put("websocketEndpoint", "/ws-analytics (topic: /topic/metricas)");
        status.put("timestamp", Instant.now().toString());
        return status;
    }

    @GetMapping("/usuario/{usuarioId}")
    public Map<String, Object> obtenerPorUsuario(
            @PathVariable String usuarioId,
            @RequestParam(defaultValue = "4") int limite) {
        List<RecomendacionItem> recs = recomendacionService.obtenerRecomendacionesUsuario(usuarioId, limite);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("usuarioId", usuarioId);
        resp.put("total", recs.size());
        resp.put("recomendaciones", recs);
        return resp;
    }

    @GetMapping("/populares")
    public List<RecomendacionItem> obtenerPopulares(@RequestParam(defaultValue = "4") int limite) {
        return recomendacionService.obtenerPopulares(limite);
    }

    @GetMapping("/metricas")
    public Map<String, Object> obtenerMetricas() {
        return recomendacionService.obtenerMetricasEnTiempoReal();
    }

    @GetMapping("/historial/{usuarioId}")
    public List<EventoAnalitico> obtenerHistorial(
            @PathVariable String usuarioId,
            @RequestParam(defaultValue = "20") int limite) {
        return recomendacionService.obtenerHistorialUsuario(usuarioId, limite);
    }

    /**
     * Permite registrar un evento de navegacion directo desde Frontend o Postman.
     * Si Kafka esta activo intenta publicarlo al topico correspondiente; si no, lo procesa localmente.
     */
    @PostMapping("/evento")
    public ResponseEntity<Map<String, Object>> registrarEventoManual(@RequestBody EventoAnalitico evento) {
        String topico = (evento.getOrigenTopico() != null && !evento.getOrigenTopico().isBlank())
                ? evento.getOrigenTopico()
                : ((evento.getTipoEvento() != null && evento.getTipoEvento().toUpperCase().contains("CARRITO"))
                        ? topicCarrito : topicProductos);
        evento.setOrigenTopico(topico);

        boolean enviadoPorKafka = false;
        if (kafkaEnabled && kafkaTemplate != null) {
            try {
                String json = objectMapper.writeValueAsString(evento);
                kafkaTemplate.send(topico, evento.getUsuarioId() != null ? evento.getUsuarioId() : "anonimo", json);
                enviadoPorKafka = true;
            } catch (Exception e) {
                System.err.println("[ms-recomendaciones] Fallback local al registrar evento: " + e.getMessage());
            }
        }

        if (!enviadoPorKafka) {
            recomendacionService.registrarEvento(evento);
        }

        return ResponseEntity.ok(Map.of(
                "registrado", true,
                "viaKafka", enviadoPorKafka,
                "topico", topico,
                "evento", evento
        ));
    }
}
