package com.botilleria.recomendaciones.model;

import java.time.Instant;
import java.util.Map;

public class EventoAnalitico {
    private String eventoId;
    private String usuarioId;
    private String tipoEvento;      // PRODUCTO_VISTO | BUSQUEDA | CARRITO_AGREGADO | CARRITO_ELIMINADO | CUPON_APLICADO | FRAUDE_BLOQUEADO | PAGO_APROBADO
    private String origenTopico;    // productos.eventos | carrito.eventos | pagos.alertas
    private Long productoId;
    private String productoNombre;
    private String categoria;
    private Integer precio;
    private Integer cantidad;
    private String timestamp;
    private Map<String, Object> metadata;

    public EventoAnalitico() {
        this.timestamp = Instant.now().toString();
    }

    public String getEventoId() { return eventoId; }
    public void setEventoId(String eventoId) { this.eventoId = eventoId; }

    public String getUsuarioId() { return usuarioId; }
    public void setUsuarioId(String usuarioId) { this.usuarioId = usuarioId; }

    public String getTipoEvento() { return tipoEvento; }
    public void setTipoEvento(String tipoEvento) { this.tipoEvento = tipoEvento; }

    public String getOrigenTopico() { return origenTopico; }
    public void setOrigenTopico(String origenTopico) { this.origenTopico = origenTopico; }

    public Long getProductoId() { return productoId; }
    public void setProductoId(Long productoId) { this.productoId = productoId; }

    public String getProductoNombre() { return productoNombre; }
    public void setProductoNombre(String productoNombre) { this.productoNombre = productoNombre; }

    public String getCategoria() { return categoria; }
    public void setCategoria(String categoria) { this.categoria = categoria; }

    public Integer getPrecio() { return precio; }
    public void setPrecio(Integer precio) { this.precio = precio; }

    public Integer getCantidad() { return cantidad; }
    public void setCantidad(Integer cantidad) { this.cantidad = cantidad; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
}
