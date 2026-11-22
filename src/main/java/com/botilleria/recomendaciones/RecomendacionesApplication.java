package com.botilleria.recomendaciones;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class RecomendacionesApplication {

    public static void main(String[] args) {
        SpringApplication.run(RecomendacionesApplication.class, args);
    }
}
