package com.botilleria.recomendaciones.service;

import com.botilleria.recomendaciones.model.EventoAnalitico;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.*;

/**
 * Persistencia NoSQL en Amazon DynamoDB (+2 puntos extra).
 * Usa DefaultCredentialsProvider (compatible con LabInstanceProfile en EC2 de AWS Learner Lab).
 * Si DynamoDB no esta habilitado o no responde, realiza fallback transparente sin botar el servicio.
 */
@Service
public class DynamoDbService {

    private final boolean enabled;
    private final String tableName;
    private final String region;
    private DynamoDbClient client;

    public DynamoDbService(
            @Value("${app.dynamodb.enabled:false}") boolean enabled,
            @Value("${app.dynamodb.table-name:pedidos360_eventos}") String tableName,
            @Value("${app.dynamodb.region:us-east-1}") String region) {
        this.enabled = enabled;
        this.tableName = tableName;
        this.region = region;
        if (enabled) {
            try {
                this.client = DynamoDbClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(DefaultCredentialsProvider.create())
                        .build();
                System.out.println("[ms-recomendaciones] Cliente Amazon DynamoDB inicializado para tabla: " + tableName);
            } catch (Exception e) {
                System.err.println("[ms-recomendaciones] No se pudo inicializar DynamoDB (usando fallback local): " + e.getMessage());
                this.client = null;
            }
        }
    }

    public boolean isActivo() {
        return enabled && client != null;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRegion() {
        return region;
    }

    public boolean guardarEvento(EventoAnalitico evento) {
        if (!isActivo()) {
            return false;
        }
        try {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("usuarioId", AttributeValue.fromS(nonNull(evento.getUsuarioId(), "anonimo")));
            item.put("timestamp", AttributeValue.fromS(nonNull(evento.getTimestamp(), String.valueOf(System.currentTimeMillis()))));
            item.put("eventoId", AttributeValue.fromS(nonNull(evento.getEventoId(), UUID.randomUUID().toString())));
            item.put("tipoEvento", AttributeValue.fromS(nonNull(evento.getTipoEvento(), "INTERACCION")));
            item.put("origenTopico", AttributeValue.fromS(nonNull(evento.getOrigenTopico(), "desconocido")));
            if (evento.getProductoId() != null) {
                item.put("productoId", AttributeValue.fromN(String.valueOf(evento.getProductoId())));
            }
            if (evento.getProductoNombre() != null && !evento.getProductoNombre().isBlank()) {
                item.put("productoNombre", AttributeValue.fromS(evento.getProductoNombre()));
            }
            if (evento.getCategoria() != null && !evento.getCategoria().isBlank()) {
                item.put("categoria", AttributeValue.fromS(evento.getCategoria()));
            }
            if (evento.getPrecio() != null) {
                item.put("precio", AttributeValue.fromN(String.valueOf(evento.getPrecio())));
            }

            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
            client.putItem(request);
            return true;
        } catch (Exception e) {
            System.err.println("[ms-recomendaciones] Aviso al escribir en DynamoDB: " + e.getMessage());
            return false;
        }
    }

    public List<EventoAnalitico> obtenerHistorialUsuario(String usuarioId, int limit) {
        if (!isActivo()) {
            return List.of();
        }
        try {
            QueryRequest request = QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression("usuarioId = :uid")
                    .expressionAttributeValues(Map.of(":uid", AttributeValue.fromS(usuarioId)))
                    .scanIndexForward(false)
                    .limit(limit)
                    .build();

            QueryResponse response = client.query(request);
            List<EventoAnalitico> eventos = new ArrayList<>();
            for (Map<String, AttributeValue> row : response.items()) {
                EventoAnalitico ev = new EventoAnalitico();
                ev.setUsuarioId(str(row, "usuarioId"));
                ev.setTimestamp(str(row, "timestamp"));
                ev.setEventoId(str(row, "eventoId"));
                ev.setTipoEvento(str(row, "tipoEvento"));
                ev.setOrigenTopico(str(row, "origenTopico"));
                ev.setProductoNombre(str(row, "productoNombre"));
                ev.setCategoria(str(row, "categoria"));
                if (row.containsKey("productoId") && row.get("productoId").n() != null) {
                    ev.setProductoId(Long.parseLong(row.get("productoId").n()));
                }
                if (row.containsKey("precio") && row.get("precio").n() != null) {
                    ev.setPrecio(Integer.parseInt(row.get("precio").n()));
                }
                eventos.add(ev);
            }
            return eventos;
        } catch (Exception e) {
            System.err.println("[ms-recomendaciones] Aviso al consultar DynamoDB: " + e.getMessage());
            return List.of();
        }
    }

    private String nonNull(String value, String fallback) {
        return (value != null && !value.isBlank()) ? value : fallback;
    }

    private String str(Map<String, AttributeValue> row, String key) {
        return row.containsKey(key) ? row.get(key).s() : null;
    }
}
