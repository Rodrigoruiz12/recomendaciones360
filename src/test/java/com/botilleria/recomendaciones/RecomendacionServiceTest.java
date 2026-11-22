package com.botilleria.recomendaciones;

import com.botilleria.recomendaciones.model.EventoAnalitico;
import com.botilleria.recomendaciones.model.RecomendacionItem;
import com.botilleria.recomendaciones.service.DynamoDbService;
import com.botilleria.recomendaciones.service.RecomendacionService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecomendacionServiceTest {

    @Test
    void debeRecomendarProductosSimilaresYActualizarMetricas() {
        DynamoDbService dynamoOff = new DynamoDbService(false, "pedidos360_eventos", "us-east-1");
        RecomendacionService service = new RecomendacionService(dynamoOff);

        EventoAnalitico vistaPisco = new EventoAnalitico();
        vistaPisco.setUsuarioId("user-1");
        vistaPisco.setTipoEvento("PRODUCTO_VISTO");
        vistaPisco.setOrigenTopico("productos.eventos");
        vistaPisco.setProductoId(1L);
        vistaPisco.setProductoNombre("Pisco Alto del Carmen 35°");
        service.registrarEvento(vistaPisco);

        List<RecomendacionItem> recs = service.obtenerRecomendacionesUsuario("user-1", 4);
        assertFalse(recs.isEmpty());
        // Debe priorizar otro Pisco similar (Pisco Mistral)
        assertEquals("Piscos", recs.get(0).getCategoria());

        Map<String, Object> metricas = service.obtenerMetricasEnTiempoReal();
        assertEquals(1L, metricas.get("totalEventos"));
        assertEquals(1L, metricas.get("totalVistasProducto"));
    }
}
