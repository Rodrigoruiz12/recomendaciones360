# recomendaciones360 (`ms-recomendaciones`) — Pedidos360 EV3

Microservicio de Analítica en Tiempo Real con **Apache Kafka**, **Administrador de Tópicos Kafka (`AdminClient`)**, **DLT/Reintentos**, **Motor de Recomendaciones**, **Amazon DynamoDB (+2 pts)** y **Dashboard WebSockets (+3 pts)** para Pedidos360.

## Funcionalidades
- **Consumidor Apache Kafka + DLT:** Escucha `productos.eventos`, `carrito.eventos` y `pagos.alertas`. Reintenta errores transitorios (2x cada 1s) y deriva payloads inválidos automáticamente a `<topico>.DLT` (`KafkaErrorConfig`).
- **Administrador de Kafka (`AdminClient`):** Gestión completa de tópicos y configuraciones desde REST (`/api/kafka/topics` y `/api/recomendaciones/kafka/topics`).
- **Recomendaciones Dinámicas:** Analiza el historial de navegación del usuario y sugiere productos similares del catálogo de Pedidos360.
- **Amazon DynamoDB (+2 pts):** Persistencia NoSQL de eventos de clickstream (`DYNAMODB_ENABLED=true`, tabla `pedidos360_eventos`, compatible con `LabInstanceProfile` en EC2).
- **WebSockets STOMP (+3 pts):** Endpoint `/ws-analytics` → `/topic/metricas` con 5+ métricas en vivo.
- **Clúster Kafka:** Incluye [`docker-compose-kafka.yml`](docker-compose-kafka.yml) con Zookeeper + 3 Brokers (3 particiones, factor de replicación 3) + Kafka-UI (`:8080`).

## Endpoints Principales (Puerto `8089`)
### Recomendaciones y Métricas
- `GET  /api/recomendaciones/health`
- `GET  /api/recomendaciones/usuario/{usuarioId}`
- `GET  /api/recomendaciones/populares`
- `GET  /api/recomendaciones/metricas`
- `GET  /api/recomendaciones/historial/{usuarioId}`
- `POST /api/recomendaciones/evento`
- `WS   /ws-analytics` → `/topic/metricas`

### Administrador de Kafka (`AdminClient`)
*(También disponibles bajo `/api/recomendaciones/kafka/...`)*
- `GET    /api/kafka/cluster` — Estado del clúster y brokers activos.
- `GET    /api/kafka/topics` — Lista tópicos, particiones, réplicas e ISR.
- `GET    /api/kafka/topics/{nombre}` — Detalle y configuraciones de un tópico.
- `POST   /api/kafka/topics` — Crea tópico (`{ nombre, particiones: 3, factorReplicacion: 3, configuraciones: {} }`).
- `PUT    /api/kafka/topics/{nombre}/config` — Modifica configuraciones (`{ "retention.ms": "86400000" }`).
- `PUT    /api/kafka/topics/{nombre}/partitions` — Aumenta particiones (`{ "particiones": 6 }`).
- `DELETE /api/kafka/topics/{nombre}` — Elimina un tópico.

## Docker en EC2
```bash
sudo docker build -t recomendaciones360 .
sudo docker run -d --name ms-recomendaciones --restart unless-stopped -p 8089:8089 \
  -e KAFKA_BOOTSTRAP_SERVERS=<IP_KAFKA>:9092,<IP_KAFKA>:9093,<IP_KAFKA>:9094 \
  -e DYNAMODB_ENABLED=true \
  -e DYNAMODB_TABLE=pedidos360_eventos \
  -e AWS_REGION=us-east-1 \
  recomendaciones360
```
