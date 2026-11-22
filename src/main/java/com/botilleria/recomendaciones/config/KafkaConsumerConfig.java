package com.botilleria.recomendaciones.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConsumerConfig {

    @Value("${app.kafka.topics.productos:productos.eventos}")
    private String topicProductos;

    @Value("${app.kafka.topics.carrito:carrito.eventos}")
    private String topicCarrito;

    @Value("${app.kafka.topics.pagos:pagos.alertas}")
    private String topicPagos;

    @Value("${app.kafka.topics.auditoria:auditoria.eventos}")
    private String topicAuditoria;

    @Value("${app.kafka.topic-partitions:3}")
    private int partitions;

    @Value("${app.kafka.topic-replicas:3}")
    private int replicas;

    @Bean
    public NewTopic productosEventosTopic() {
        return TopicBuilder.name(topicProductos).partitions(partitions).replicas(replicas).build();
    }

    @Bean
    public NewTopic carritoEventosTopic() {
        return TopicBuilder.name(topicCarrito).partitions(partitions).replicas(replicas).build();
    }

    @Bean
    public NewTopic pagosAlertasTopic() {
        return TopicBuilder.name(topicPagos).partitions(partitions).replicas(replicas).build();
    }

    @Bean
    public NewTopic auditoriaEventosTopic() {
        return TopicBuilder.name(topicAuditoria).partitions(partitions).replicas(replicas).build();
    }
}
