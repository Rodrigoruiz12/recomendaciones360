package com.botilleria.recomendaciones.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Servicio Administrador de Apache Kafka (AdminClient):
 * Permite inspeccionar el cluster, listar, crear, modificar configuraciones/particiones y eliminar topicos via REST.
 */
@Service
public class KafkaAdminService {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private AdminClient crearCliente() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        return AdminClient.create(props);
    }

    public Map<String, Object> obtenerInfoCluster() throws Exception {
        try (AdminClient admin = crearCliente()) {
            DescribeClusterResult cluster = admin.describeCluster();
            String clusterId = cluster.clusterId().get(5, TimeUnit.SECONDS);
            Node controller = cluster.controller().get(5, TimeUnit.SECONDS);
            Collection<Node> nodes = cluster.nodes().get(5, TimeUnit.SECONDS);

            List<Map<String, Object>> brokers = nodes.stream().map(n -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("id", n.id());
                m.put("host", n.host());
                m.put("port", n.port());
                return m;
            }).toList();

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("clusterId", clusterId);
            resp.put("bootstrapServers", bootstrapServers);
            resp.put("controllerId", controller != null ? controller.id() : null);
            resp.put("totalBrokers", brokers.size());
            resp.put("brokers", brokers);
            return resp;
        }
    }

    public List<Map<String, Object>> listarTopicos() throws Exception {
        try (AdminClient admin = crearCliente()) {
            Set<String> names = admin.listTopics(new ListTopicsOptions().listInternal(false))
                    .names().get(5, TimeUnit.SECONDS);
            if (names.isEmpty()) {
                return List.of();
            }
            Map<String, TopicDescription> descMap = admin.describeTopics(names)
                    .allTopicNames().get(5, TimeUnit.SECONDS);

            List<Map<String, Object>> resultado = new ArrayList<>();
            for (TopicDescription desc : descMap.values()) {
                resultado.add(mapearDescripcion(desc, null));
            }
            resultado.sort(Comparator.comparing(m -> String.valueOf(m.get("nombre"))));
            return resultado;
        }
    }

    public Map<String, Object> describirTopico(String nombre) throws Exception {
        try (AdminClient admin = crearCliente()) {
            TopicDescription desc = admin.describeTopics(List.of(nombre))
                    .allTopicNames().get(5, TimeUnit.SECONDS).get(nombre);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
            Config config = admin.describeConfigs(List.of(resource))
                    .all().get(5, TimeUnit.SECONDS).get(resource);

            Map<String, String> configs = new LinkedHashMap<>();
            if (config != null) {
                config.entries().stream()
                        .filter(e -> e.value() != null && !e.isDefault())
                        .forEach(e -> configs.put(e.name(), e.value()));
            }
            return mapearDescripcion(desc, configs);
        }
    }

    public Map<String, Object> crearTopico(String nombre, int particiones, short factorReplicacion, Map<String, String> configs) throws Exception {
        if (nombre == null || nombre.isBlank()) {
            throw new IllegalArgumentException("El nombre del topico es obligatorio.");
        }
        try (AdminClient admin = crearCliente()) {
            NewTopic newTopic = new NewTopic(nombre.trim(), particiones, factorReplicacion);
            if (configs != null && !configs.isEmpty()) {
                newTopic.configs(configs);
            }
            admin.createTopics(List.of(newTopic)).all().get(5, TimeUnit.SECONDS);
            return Map.of(
                    "creado", true,
                    "nombre", nombre.trim(),
                    "particiones", particiones,
                    "factorReplicacion", factorReplicacion,
                    "configuraciones", configs != null ? configs : Map.of()
            );
        }
    }

    public Map<String, Object> modificarConfiguracion(String nombre, Map<String, String> configs) throws Exception {
        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException("Debe enviar al menos una propiedad de configuracion (ej. retention.ms).");
        }
        try (AdminClient admin = crearCliente()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
            Collection<AlterConfigOp> ops = configs.entrySet().stream()
                    .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                    .toList();
            admin.incrementalAlterConfigs(Map.of(resource, ops)).all().get(5, TimeUnit.SECONDS);
            return Map.of(
                    "actualizado", true,
                    "nombre", nombre,
                    "configuracionesAplicadas", configs
            );
        }
    }

    public Map<String, Object> aumentarParticiones(String nombre, int nuevoTotalParticiones) throws Exception {
        if (nuevoTotalParticiones < 1) {
            throw new IllegalArgumentException("El numero de particiones debe ser >= 1.");
        }
        try (AdminClient admin = crearCliente()) {
            admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(nuevoTotalParticiones)))
                    .all().get(5, TimeUnit.SECONDS);
            return Map.of(
                    "actualizado", true,
                    "nombre", nombre,
                    "particiones", nuevoTotalParticiones
            );
        }
    }

    public Map<String, Object> eliminarTopico(String nombre) throws Exception {
        try (AdminClient admin = crearCliente()) {
            admin.deleteTopics(List.of(nombre)).all().get(5, TimeUnit.SECONDS);
            return Map.of(
                    "eliminado", true,
                    "nombre", nombre
            );
        }
    }

    private Map<String, Object> mapearDescripcion(TopicDescription desc, Map<String, String> configs) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nombre", desc.name());
        m.put("particiones", desc.partitions().size());
        int replicas = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();
        m.put("factorReplicacion", replicas);
        List<Map<String, Object>> parts = new ArrayList<>();
        for (TopicPartitionInfo p : desc.partitions()) {
            Map<String, Object> pm = new LinkedHashMap<>();
            pm.put("particion", p.partition());
            pm.put("lider", p.leader() != null ? p.leader().id() : null);
            pm.put("replicas", p.replicas().stream().map(Node::id).toList());
            pm.put("isr", p.isr().stream().map(Node::id).toList());
            parts.add(pm);
        }
        m.put("detalleParticiones", parts);
        if (configs != null) {
            m.put("configuracionesPersonalizadas", configs);
        }
        return m;
    }
}
