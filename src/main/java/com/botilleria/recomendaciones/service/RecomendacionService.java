package com.botilleria.recomendaciones.service;

import com.botilleria.recomendaciones.model.EventoAnalitico;
import com.botilleria.recomendaciones.model.ProductoResumen;
import com.botilleria.recomendaciones.model.RecomendacionItem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class RecomendacionService {

    private static final int MAX_HISTORIAL_USUARIO = 100;
    private static final int MAX_FEED_GLOBAL = 30;

    private final DynamoDbService dynamoDbService;

    @Autowired(required = false)
    private JdbcTemplate jdbcTemplate;

    @Autowired(required = false)
    private SimpMessagingTemplate messagingTemplate;

    // Estructuras concurrentes en memoria para analitica en tiempo real
    private final AtomicLong totalEventos = new AtomicLong(0);
    private final AtomicLong totalVistasProducto = new AtomicLong(0);
    private final AtomicLong totalEventosCarrito = new AtomicLong(0);
    private final AtomicLong totalAlertasFraude = new AtomicLong(0);

    private final ConcurrentHashMap<Long, AtomicLong> vistasPorProducto = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, String> nombresProducto = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> eventosPorCategoria = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Deque<EventoAnalitico>> historialPorUsuario = new ConcurrentHashMap<>();
    private final Deque<EventoAnalitico> ultimosEventosGlobales = new ConcurrentLinkedDeque<>();
    private final Deque<EventoAnalitico> ultimasAlertasFraude = new ConcurrentLinkedDeque<>();
    private final Deque<Instant> timestampsUltimoMinuto = new ConcurrentLinkedDeque<>();

    public RecomendacionService(DynamoDbService dynamoDbService) {
        this.dynamoDbService = dynamoDbService;
    }

    public void registrarEvento(EventoAnalitico evento) {
        if (evento.getEventoId() == null || evento.getEventoId().isBlank()) {
            evento.setEventoId(UUID.randomUUID().toString());
        }
        if (evento.getTimestamp() == null || evento.getTimestamp().isBlank()) {
            evento.setTimestamp(Instant.now().toString());
        }
        if (evento.getUsuarioId() == null || evento.getUsuarioId().isBlank()) {
            evento.setUsuarioId("anonimo");
        }
        if (evento.getCategoria() == null || evento.getCategoria().isBlank()) {
            evento.setCategoria(inferirCategoria(evento.getProductoNombre()));
        }

        // 1. Actualizar contadores en tiempo real
        totalEventos.incrementAndGet();
        timestampsUltimoMinuto.addLast(Instant.now());
        limpiarVentanaUltimoMinuto();

        String tipo = evento.getTipoEvento() != null ? evento.getTipoEvento().toUpperCase(Locale.ROOT) : "EVENTO";
        if (tipo.contains("PRODUCTO") || tipo.contains("VISTA") || tipo.contains("BUSQUEDA")) {
            totalVistasProducto.incrementAndGet();
        }
        if (tipo.contains("CARRITO") || tipo.contains("CUPON") || tipo.contains("ENVIO")) {
            totalEventosCarrito.incrementAndGet();
        }
        if (tipo.contains("FRAUDE")) {
            totalAlertasFraude.incrementAndGet();
            ultimasAlertasFraude.addFirst(evento);
            while (ultimasAlertasFraude.size() > 15) {
                ultimasAlertasFraude.pollLast();
            }
        }

        if (evento.getProductoId() != null) {
            vistasPorProducto.computeIfAbsent(evento.getProductoId(), k -> new AtomicLong(0)).incrementAndGet();
            if (evento.getProductoNombre() != null && !evento.getProductoNombre().isBlank()) {
                nombresProducto.put(evento.getProductoId(), evento.getProductoNombre());
            }
        }

        if (evento.getCategoria() != null && !evento.getCategoria().isBlank()) {
            eventosPorCategoria.computeIfAbsent(evento.getCategoria(), k -> new AtomicLong(0)).incrementAndGet();
        }

        // 2. Guardar historial por usuario en memoria
        Deque<EventoAnalitico> hist = historialPorUsuario.computeIfAbsent(
                evento.getUsuarioId(), k -> new ConcurrentLinkedDeque<>());
        hist.addFirst(evento);
        while (hist.size() > MAX_HISTORIAL_USUARIO) {
            hist.pollLast();
        }

        // 3. Feed global en tiempo real
        ultimosEventosGlobales.addFirst(evento);
        while (ultimosEventosGlobales.size() > MAX_FEED_GLOBAL) {
            ultimosEventosGlobales.pollLast();
        }

        // 4. Persistir en Amazon DynamoDB si esta habilitado (+2 pts)
        dynamoDbService.guardarEvento(evento);

        // 5. Empujar metricas actualizadas por WebSocket a /topic/metricas (+3 pts)
        emitirMetricasWebSocket();
    }

    public List<RecomendacionItem> obtenerRecomendacionesUsuario(String usuarioId, int limite) {
        List<ProductoResumen> catalogo = cargarCatalogoProductos();
        if (catalogo.isEmpty()) {
            return List.of();
        }

        List<EventoAnalitico> historial = obtenerHistorialUsuario(usuarioId, 30);

        Map<String, Integer> afinidadCategoria = new HashMap<>();
        Set<Long> vistosRecientes = new HashSet<>();
        Map<String, String> ultimoProductoPorCategoria = new HashMap<>();

        int peso = historial.size();
        for (EventoAnalitico ev : historial) {
            String cat = (ev.getCategoria() != null && !ev.getCategoria().isBlank())
                    ? ev.getCategoria()
                    : inferirCategoria(ev.getProductoNombre());
            int factor = (ev.getTipoEvento() != null && ev.getTipoEvento().toUpperCase(Locale.ROOT).contains("CARRITO")) ? 3 : 2;
            afinidadCategoria.merge(cat, Math.max(1, peso) * factor, Integer::sum);
            if (ev.getProductoId() != null) {
                vistosRecientes.add(ev.getProductoId());
            }
            if (ev.getProductoNombre() != null && !ev.getProductoNombre().isBlank()) {
                ultimoProductoPorCategoria.putIfAbsent(cat, ev.getProductoNombre());
            }
            peso--;
        }

        List<RecomendacionItem> puntuados = new ArrayList<>();
        for (ProductoResumen prod : catalogo) {
            if (prod.getInventario() != null && prod.getInventario() <= 0) {
                continue;
            }
            double score = 1.0;
            String motivo = "Destacado del catalogo Pedidos360";

            int scoreCat = afinidadCategoria.getOrDefault(prod.getCategoria(), 0);
            if (scoreCat > 0) {
                score += scoreCat * 2.5;
                String ref = ultimoProductoPorCategoria.get(prod.getCategoria());
                motivo = (ref != null)
                        ? "Similar a \"" + ref + "\" (" + prod.getCategoria() + ")"
                        : "Basado en tu interes en " + prod.getCategoria();
            }

            long popularidad = vistasPorProducto.getOrDefault(prod.getIdentificador(), new AtomicLong(0)).get();
            if (popularidad > 0) {
                score += popularidad * 1.5;
                if (scoreCat == 0) {
                    motivo = "Tendencia en Pedidos360 (" + popularidad + " vistas)";
                }
            }

            // Priorizamos productos similares que aun no ha visto, sin descartar los vistos si el catalogo es pequeno
            if (vistosRecientes.contains(prod.getIdentificador())) {
                score *= 0.6;
            }

            puntuados.add(new RecomendacionItem(prod, Math.round(score * 100.0) / 100.0, motivo));
        }

        return puntuados.stream()
                .sorted(Comparator.comparingDouble(RecomendacionItem::getPuntaje).reversed())
                .limit(limite)
                .collect(Collectors.toList());
    }

    public List<RecomendacionItem> obtenerPopulares(int limite) {
        return obtenerRecomendacionesUsuario("__global__", limite);
    }

    public List<EventoAnalitico> obtenerHistorialUsuario(String usuarioId, int limite) {
        if (dynamoDbService.isActivo() && !"__global__".equals(usuarioId)) {
            List<EventoAnalitico> desdeDynamo = dynamoDbService.obtenerHistorialUsuario(usuarioId, limite);
            if (!desdeDynamo.isEmpty()) {
                return desdeDynamo;
            }
        }
        Deque<EventoAnalitico> local = historialPorUsuario.get(usuarioId);
        if (local == null) {
            return List.of();
        }
        return local.stream().limit(limite).collect(Collectors.toList());
    }

    public Map<String, Object> obtenerMetricasEnTiempoReal() {
        limpiarVentanaUltimoMinuto();

        List<Map<String, Object>> topProductos = vistasPorProducto.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                .limit(5)
                .map(entry -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("productoId", entry.getKey());
                    m.put("nombre", nombresProducto.getOrDefault(entry.getKey(), "Producto #" + entry.getKey()));
                    m.put("vistas", entry.getValue().get());
                    return m;
                })
                .collect(Collectors.toList());

        List<Map<String, Object>> topCategorias = eventosPorCategoria.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                .limit(5)
                .map(entry -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("categoria", entry.getKey());
                    m.put("interacciones", entry.getValue().get());
                    return m;
                })
                .collect(Collectors.toList());

        Map<String, Object> metricas = new LinkedHashMap<>();
        metricas.put("timestamp", Instant.now().toString());
        // Metrica 1: Volumen de clics / eventos
        metricas.put("totalEventos", totalEventos.get());
        metricas.put("eventosUltimoMinuto", timestampsUltimoMinuto.size());
        metricas.put("totalVistasProducto", totalVistasProducto.get());
        // Metrica 2: Actividad de carrito
        metricas.put("totalEventosCarrito", totalEventosCarrito.get());
        // Metrica 3: Alertas de fraude recibidas
        metricas.put("totalAlertasFraude", totalAlertasFraude.get());
        // Metrica 4: Ranking de productos mas vistos
        metricas.put("productosMasVistos", topProductos);
        // Metrica 5: Categorias mas populares y usuarios activos
        metricas.put("categoriasPopulares", topCategorias);
        metricas.put("usuariosActivos", historialPorUsuario.size());
        metricas.put("ultimosEventos", ultimosEventosGlobales.stream().limit(10).collect(Collectors.toList()));
        metricas.put("ultimasAlertasFraude", ultimasAlertasFraude.stream().limit(5).collect(Collectors.toList()));
        metricas.put("dynamoDbActivo", dynamoDbService.isActivo());
        return metricas;
    }

    public void emitirMetricasWebSocket() {
        if (messagingTemplate == null) {
            return;
        }
        try {
            messagingTemplate.convertAndSend("/topic/metricas", obtenerMetricasEnTiempoReal());
        } catch (Exception e) {
            System.err.println("[ms-recomendaciones] Aviso WebSocket: " + e.getMessage());
        }
    }

    public List<ProductoResumen> cargarCatalogoProductos() {
        if (jdbcTemplate != null) {
            try {
                List<ProductoResumen> rows = jdbcTemplate.query(
                        "SELECT identificador, nombre, precio, inventario, imagen FROM productos ORDER BY identificador ASC",
                        (rs, rowNum) -> {
                            String nombre = rs.getString("nombre");
                            return new ProductoResumen(
                                    rs.getLong("identificador"),
                                    nombre,
                                    rs.getObject("precio") != null ? rs.getInt("precio") : 0,
                                    rs.getObject("inventario") != null ? rs.getInt("inventario") : 0,
                                    rs.getString("imagen"),
                                    inferirCategoria(nombre)
                            );
                        }
                );
                if (!rows.isEmpty()) {
                    rows.forEach(p -> nombresProducto.putIfAbsent(p.getIdentificador(), p.getNombre()));
                    return rows;
                }
            } catch (Exception e) {
                System.err.println("[ms-recomendaciones] Aviso al leer catalogo de Supabase: " + e.getMessage());
            }
        }
        return catalogoFallback();
    }

    public String inferirCategoria(String nombreProducto) {
        if (nombreProducto == null || nombreProducto.isBlank()) {
            return "Destilados";
        }
        String lower = nombreProducto.toLowerCase(Locale.ROOT);
        if (lower.contains("pisco") || lower.contains("mistral") || lower.contains("alto del carmen") || lower.contains("capel")) {
            return "Piscos";
        }
        if (lower.contains("cerveza") || lower.contains("corona") || lower.contains("heineken") || lower.contains("kunstmann") || lower.contains("royal") || lower.contains("pack")) {
            return "Cervezas";
        }
        if (lower.contains("vino") || lower.contains("cabernet") || lower.contains("carmenere") || lower.contains("merlot") || lower.contains("casillero") || lower.contains("espumante")) {
            return "Vinos";
        }
        if (lower.contains("whisky") || lower.contains("whiskey") || lower.contains("johnnie") || lower.contains("jack") || lower.contains("ballantine")) {
            return "Whiskies";
        }
        if (lower.contains("ron") || lower.contains("havana") || lower.contains("barcelo") || lower.contains("bacardi")) {
            return "Rones";
        }
        if (lower.contains("vodka") || lower.contains("gin") || lower.contains("tequila") || lower.contains("fernet")) {
            return "Destilados";
        }
        if (lower.contains("coca") || lower.contains("bebida") || lower.contains("tonica") || lower.contains("jugo") || lower.contains("hielo")) {
            return "Bebidas y Mixers";
        }
        return "Licoreria General";
    }

    private void limpiarVentanaUltimoMinuto() {
        Instant limite = Instant.now().minusSeconds(60);
        while (!timestampsUltimoMinuto.isEmpty() && timestampsUltimoMinuto.peekFirst().isBefore(limite)) {
            timestampsUltimoMinuto.pollFirst();
        }
    }

    private List<ProductoResumen> catalogoFallback() {
        return List.of(
                new ProductoResumen(1L, "Pisco Alto del Carmen 35°", 8990, 50, "", "Piscos"),
                new ProductoResumen(2L, "Pisco Mistral 35°", 9990, 40, "", "Piscos"),
                new ProductoResumen(3L, "Cerveza Kunstmann Torobayo Pack 6", 11990, 35, "", "Cervezas"),
                new ProductoResumen(4L, "Vino Casillero del Diablo Cabernet", 7490, 45, "", "Vinos"),
                new ProductoResumen(5L, "Ron Havana Club Añejo 750cc", 12990, 30, "", "Rones"),
                new ProductoResumen(6L, "Coca-Cola 2.5L", 2990, 80, "", "Bebidas y Mixers")
        );
    }
}
