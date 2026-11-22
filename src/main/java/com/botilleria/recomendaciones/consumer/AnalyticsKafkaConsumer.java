package com.botilleria.recomendaciones.consumer;

import com.botilleria.recomendaciones.model.EventoAnalitico;
import com.botilleria.recomendaciones.service.RecomendacionService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Consumidor de los topicos de Apache Kafka:
 * - productos.eventos (clickstream de catalogo y detalle de producto)
 * - carrito.eventos (productos agregados/quitados y cupones)
 * - pagos.alertas (pagos aprobados y bloqueos antifraude de ms-pagos)
 */
@Component
public class AnalyticsKafkaConsumer {

    private final RecomendacionService recomendacionService;
    private final ObjectMapper objectMapper;

    public AnalyticsKafkaConsumer(RecomendacionService recomendacionService, ObjectMapper objectMapper) {
        this.recomendacionService = recomendacionService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.productos:productos.eventos}",
            groupId = "${spring.kafka.consumer.group-id:pedidos360-analytics-group}",
            autoStartup = "${app.kafka.enabled:true}"
    )
    public void consumirEventosProductos(String mensajeJson) {
        procesarMensaje(mensajeJson, "productos.eventos", "PRODUCTO_VISTO");
    }

    @KafkaListener(
            topics = "${app.kafka.topics.carrito:carrito.eventos}",
            groupId = "${spring.kafka.consumer.group-id:pedidos360-analytics-group}",
            autoStartup = "${app.kafka.enabled:true}"
    )
    public void consumirEventosCarrito(String mensajeJson) {
        procesarMensaje(mensajeJson, "carrito.eventos", "CARRITO_ACTUALIZADO");
    }

    @KafkaListener(
            topics = "${app.kafka.topics.pagos:pagos.alertas}",
            groupId = "${spring.kafka.consumer.group-id:pedidos360-analytics-group}",
            autoStartup = "${app.kafka.enabled:true}"
    )
    public void consumirAlertasPagos(String mensajeJson) {
        procesarMensaje(mensajeJson, "pagos.alertas", "EVENTO_PAGO");
    }

    private void procesarMensaje(String mensajeJson, String topico, String tipoPorDefecto) {
        try {
            Map<String, Object> data = objectMapper.readValue(mensajeJson, new TypeReference<>() {});

            EventoAnalitico evento = new EventoAnalitico();
            evento.setEventoId(UUID.randomUUID().toString());
            evento.setOrigenTopico(topico);
            evento.setTipoEvento(str(data, "tipoEvento", tipoPorDefecto));
            evento.setUsuarioId(str(data, "usuarioId", "anonimo"));
            evento.setProductoNombre(str(data, "productoNombre", str(data, "nombre", null)));
            evento.setCategoria(str(data, "categoria", null));
            evento.setTimestamp(str(data, "timestamp", Instant.now().toString()));
            evento.setMetadata(data);

            Object prodId = data.get("productoId");
            if (prodId != null) {
                try {
                    evento.setProductoId(Long.valueOf(prodId.toString()));
                } catch (NumberFormatException ignored) {}
            }

            Object precio = data.get("precio");
            if (precio != null) {
                try {
                    evento.setPrecio(Integer.valueOf(precio.toString()));
                } catch (NumberFormatException ignored) {}
            }

            Object cantidad = data.get("cantidad");
            if (cantidad != null) {
                try {
                    evento.setCantidad(Integer.valueOf(cantidad.toString()));
                } catch (NumberFormatException ignored) {}
            }

            System.out.println("[KAFKA <- " + topico + "] Procesado evento " + evento.getTipoEvento()
                    + " | usuario=" + evento.getUsuarioId()
                    + " | producto=" + evento.getProductoNombre());

            recomendacionService.registrarEvento(evento);
        } catch (Exception e) {
            System.err.println("[KAFKA <- " + topico + "] Error parseando mensaje: " + e.getMessage());
        }
    }

    private String str(Map<String, Object> map, String key, String fallback) {
        Object val = map.get(key);
        if (val == null) return fallback;
        String s = val.toString();
        return s.isBlank() ? fallback : s;
    }
}
