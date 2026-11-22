package com.botilleria.recomendaciones.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * Configuracion de WebSockets (STOMP) para el Dashboard de Metricas en Tiempo Real (+3 pts).
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // Endpoint WebSocket nativo y con fallback SockJS
        registry.addEndpoint("/ws-analytics")
                .setAllowedOriginPatterns("*");
        registry.addEndpoint("/ws-analytics")
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }
}
