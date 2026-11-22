# recomendaciones360 (`ms-recomendaciones`) — Pedidos360 EV3

Microservicio de Analítica en Tiempo Real con **Apache Kafka**, **Motor de Recomendaciones**, **Amazon DynamoDB (+2 pts)** y **Dashboard WebSockets (+3 pts)** para Pedidos360.

## Funcionalidades
- **Consumidor Apache Kafka:** Escucha los tópicos `productos.eventos`, `carrito.eventos` y `pagos.alertas`.
- **Recomendaciones Dinámicas:** Analiza el historial de navegación del usuario y sugiere productos similares del catálogo de Pedidos360.
- **Amazon DynamoDB (+2 pts):** Persistencia NoSQL de eventos de clickstream (`DYNAMODB_ENABLED=true`, tabla `pedidos360_eventos`, compatible con `LabInstanceProfile` en EC2).
- **WebSockets STOMP (+3 pts):** Endpoint `/ws-analytics` que transmite métricas en vivo por `/topic/metricas` (total de eventos, eventos último minuto, top productos más vistos, actividad de carrito, alertas de fraude y usuarios activos).
- **Seguridad JWT Multi-Proveedor:** Valida tokens de **Azure Entra ID** y **AWS Cognito**.

## Endpoints Principales (Puerto `8089`)
- `GET  /api/recomendaciones/health` — Estado del servicio, Kafka y DynamoDB (público).
- `GET  /api/recomendaciones/usuario/{usuarioId}` — Recomendaciones personalizadas.
- `GET  /api/recomendaciones/populares` — Productos más populares/vistos.
- `GET  /api/recomendaciones/metricas` — Snapshot de métricas en tiempo real.
- `GET  /api/recomendaciones/historial/{usuarioId}` — Historial de navegación del usuario.
- `POST /api/recomendaciones/evento` — Registra evento de clickstream (vía Kafka o fallback local).
- `WS   /ws-analytics` → suscribirse a `/topic/metricas`.

## Docker en EC2
```bash
docker build -t recomendaciones360 .
docker run -d --name ms-recomendaciones -p 8089:8089 \
  -e KAFKA_BOOTSTRAP_SERVERS=<IP_PRIVADA_KAFKA>:9092 \
  -e DYNAMODB_ENABLED=true \
  -e DYNAMODB_TABLE=pedidos360_eventos \
  -e AWS_REGION=us-east-1 \
  recomendaciones360
```
