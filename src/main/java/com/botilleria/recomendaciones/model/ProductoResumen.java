package com.botilleria.recomendaciones.model;

public class ProductoResumen {
    private Long identificador;
    private String nombre;
    private Integer precio;
    private Integer inventario;
    private String imagen;
    private String categoria;

    public ProductoResumen() {}

    public ProductoResumen(Long identificador, String nombre, Integer precio, Integer inventario, String imagen, String categoria) {
        this.identificador = identificador;
        this.nombre = nombre;
        this.precio = precio;
        this.inventario = inventario;
        this.imagen = imagen;
        this.categoria = categoria;
    }

    public Long getIdentificador() { return identificador; }
    public void setIdentificador(Long identificador) { this.identificador = identificador; }

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
}
