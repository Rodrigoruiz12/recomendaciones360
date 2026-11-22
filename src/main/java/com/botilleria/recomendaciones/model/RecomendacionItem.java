package com.botilleria.recomendaciones.model;

public class RecomendacionItem {
    private Long productoId;
    private String nombre;
    private Integer precio;
    private Integer inventario;
    private String imagen;
    private String categoria;
    private double puntaje;
    private String motivo;

    public RecomendacionItem() {}

    public RecomendacionItem(ProductoResumen p, double puntaje, String motivo) {
        this.productoId = p.getIdentificador();
        this.nombre = p.getNombre();
        this.precio = p.getPrecio();
        this.inventario = p.getInventario();
        this.imagen = p.getImagen();
        this.categoria = p.getCategoria();
        this.puntaje = puntaje;
        this.motivo = motivo;
    }

    public Long getProductoId() { return productoId; }
    public void setProductoId(Long productoId) { this.productoId = productoId; }

    public String getNombre() { return nombre; }
    public void setNombre(String nombre) { this.nombre = nombre; }

    public Integer getPrecio() { return precio; }
    public void setPrecio(Integer precio) { this.precio = precio; }

    public Integer getInventario() { return inventario; }
    public void setInventario(Integer inventario) { this.inventario = inventario; }

    public String getImagen() { return imagen; }
    public void setImagen(String imagen) { this.imagen = imagen; }

    public String getCategoria() { return categoria; }
    public void setCategoria(String categoria) { this.categoria = categoria; }

    public double getPuntaje() { return puntaje; }
    public void setPuntaje(double puntaje) { this.puntaje = puntaje; }

    public String getMotivo() { return motivo; }
    public void setMotivo(String motivo) { this.motivo = motivo; }
}
