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

    @Bean
    public NewTopic productosEventosTopic() {
        return TopicBuilder.name(topicProductos).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic carritoEventosTopic() {
        return TopicBuilder.name(topicCarrito).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic pagosAlertasTopic() {
        return TopicBuilder.name(topicPagos).partitions(1).replicas(1).build();
    }
}
