package net.java21.data2flow.pipeline.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import net.java21.data2flow.contracts.message.MessageCodec;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * core-api 내부 API 대역(design/testing/backend.md "외부 HTTP 대역"). pipeline이 부르는 API-ING-21, API-DEV-120~125·130,
 * API-SCR-32·34를 메모리 상태로 흉내 내고 호출을 기록한다. {@link #down(boolean)}이면 503(core 장애).
 */
public final class CoreApiStub {

    private static final Pattern DEVICE = Pattern.compile("/internal/core/sources/(\\d+)/devices/([^/]+)");
    private static final Pattern RUNTIME = Pattern.compile("/internal/core/devices/(\\d+)/runtime");

    private final JsonMapper mapper = MessageCodec.newMapper();
    private final HttpServer server;
    private final Map<Long, ObjectNode> sources = new ConcurrentHashMap<>();
    private final Map<String, ObjectNode> devices = new ConcurrentHashMap<>();
    private final Map<Long, ObjectNode> attributes = new ConcurrentHashMap<>();
    private final Map<String, ObjectNode> metrics = new ConcurrentHashMap<>();
    private final Map<String, String> aliases = new ConcurrentHashMap<>();
    private final AtomicLong metricVersion = new AtomicLong(1);
    private final Map<Long, ObjectNode> bundles = new ConcurrentHashMap<>();
    private final AtomicLong deviceIds = new AtomicLong(1000);
    private final Map<Long, AtomicInteger> autoRegistered = new ConcurrentHashMap<>();
    private final Map<Long, Integer> quotas = new ConcurrentHashMap<>();
    private final List<String> calls = new CopyOnWriteArrayList<>();
    private final List<JsonNode> autoRegisterRequests = new CopyOnWriteArrayList<>();
    private final List<JsonNode> unverifiedRequests = new CopyOnWriteArrayList<>();
    private final List<JsonNode> gatewayTouches = new CopyOnWriteArrayList<>();
    private final List<JsonNode> deployAcks = new CopyOnWriteArrayList<>();
    private volatile boolean down;

    public CoreApiStub() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.createContext("/", this::handle);
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
        reset();
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public void stop() {
        server.stop(0);
    }

    /** 기본 데이터: 조직 1, 소스 3(chirpstack-v4·자동 등록), 측정 항목(아카데미 6종) */
    public synchronized void reset() {
        sources.clear();
        devices.clear();
        attributes.clear();
        metrics.clear();
        aliases.clear();
        bundles.clear();
        autoRegistered.clear();
        quotas.clear();
        calls.clear();
        autoRegisterRequests.clear();
        unverifiedRequests.clear();
        gatewayTouches.clear();
        deployAcks.clear();
        down = false;
        metricVersion.set(1);
        source(3, 1, "chirpstack-v4", "AUTO_REGISTER", null);
        metric("temperature", "℃", -20.0, 60.0);
        metric("humidity", "%", 0.0, 100.0);
        metric("co2", "ppm", 0.0, 10000.0);
        metric("battery", "%", 0.0, 100.0);
        metric("pressure", "hPa", 300.0, 1100.0);
        metric("LAeq", "dB", 0.0, 140.0);
        metric("LAI", "dB", 0.0, 140.0);
        metric("LAImax", "dB", 0.0, 140.0);
        metric("tvoc", null, null, null);
        metric("illumination", "lux", null, null);
        metric("infrared", null, null, null);
        metric("infrared_and_visible", null, null, null);
        metric("activity", null, null, null);
        ObjectNode door = metric("door", null, null, null);
        door.put("valueType", "ENUM");
        door.putObject("enumMap").put("open", 1).put("close", 0);
        door.put("stateType", true);
        alias("magnet_status", "door");
        alias("illuminance", "illumination");
    }

    public void down(boolean value) {
        this.down = value;
    }

    public ObjectNode source(long sourceId, long organizationId, String decoderKey, String policy, Long scriptId) {
        ObjectNode s = mapper.createObjectNode();
        s.put("sourceId", Long.toString(sourceId));
        s.put("organizationId", Long.toString(organizationId));
        s.put("sourceType", "MQTT_SUBSCRIBE");
        ObjectNode decoder = s.putObject("decoder");
        decoder.put("key", decoderKey);
        if (scriptId != null) {
            decoder.put("scriptId", Long.toString(scriptId));
        }
        decoder.putObject("config");
        s.put("unknownDevicePolicy", policy);
        s.put("autoRegisterHourlyLimit", 100);
        s.put("contextVersion", 1);
        sources.put(sourceId, s);
        return s;
    }

    public void sourceDecoderConfig(long sourceId, JsonNode config) {
        ((ObjectNode) sources.get(sourceId).get("decoder")).set("config", config);
    }

    public void sourceLimit(long sourceId, int perHour) {
        sources.get(sourceId).put("autoRegisterHourlyLimit", perHour);
        quotas.put(sourceId, perHour);
    }

    /** 등록된 기기. 외부 ID는 소문자 */
    public ObjectNode device(long deviceId, long organizationId, long sourceId, String externalId, String status, Long modelId,
                             Long spaceId, Integer intervalSec) {
        ObjectNode d = mapper.createObjectNode();
        d.put("deviceId", Long.toString(deviceId));
        d.put("organizationId", Long.toString(organizationId));
        d.put("sourceId", Long.toString(sourceId));
        d.put("externalId", externalId.toLowerCase());
        d.put("name", "device-" + deviceId);
        d.put("status", status);
        if (modelId != null) {
            d.put("modelId", Long.toString(modelId));
        }
        if (spaceId != null) {
            d.put("spaceId", Long.toString(spaceId));
        }
        if (intervalSec != null) {
            d.put("expectedIntervalSec", intervalSec);
        }
        d.put("updatedAt", "2026-10-03T00:00:00Z");
        devices.put(sourceId + "|" + externalId.toLowerCase(), d);
        return d;
    }

    public void attributes(long deviceId, JsonNode attrs) {
        ObjectNode a = mapper.createObjectNode();
        a.set("attributes", attrs);
        attributes.put(deviceId, a);
    }

    public ObjectNode metric(String key, String unit, Double min, Double max) {
        ObjectNode m = mapper.createObjectNode();
        m.put("id", Integer.toString(metrics.size() + 1));
        m.put("key", key);
        if (unit != null) {
            m.put("unit", unit);
        }
        m.put("valueType", "NUMBER");
        if (min != null) {
            m.put("validMin", min);
        }
        if (max != null) {
            m.put("validMax", max);
        }
        m.put("status", "VERIFIED");
        metrics.put(key, m);
        metricVersion.incrementAndGet();
        return m;
    }

    public void alias(String alias, String key) {
        aliases.put(alias, key);
        metricVersion.incrementAndGet();
    }

    /** 실행 번들 스크립트 하나 더하기 */
    public void script(long organizationId, long scriptId, String kind, long versionId, int versionNo, String code,
                       String failurePolicy, String targetType, long targetId, JsonNode config) {
        ObjectNode bundle = bundles.computeIfAbsent(organizationId, o -> {
            ObjectNode b = mapper.createObjectNode();
            b.put("bundleVersion", 0);
            b.putArray("scripts");
            return b;
        });
        ArrayNode scripts = (ArrayNode) bundle.get("scripts");
        for (int i = 0; i < scripts.size(); i++) {
            if (scripts.get(i).get("scriptId").asString().equals(Long.toString(scriptId))) {
                scripts.remove(i);
                break;
            }
        }
        ObjectNode s = scripts.addObject();
        s.put("scriptId", Long.toString(scriptId));
        s.put("organizationId", Long.toString(organizationId));
        s.put("kind", kind);
        s.put("versionId", Long.toString(versionId));
        s.put("versionNo", versionNo);
        s.put("code", code);
        s.set("config", config == null ? mapper.createObjectNode() : config);
        ObjectNode binding = s.putArray("bindings").addObject();
        binding.put("targetType", targetType);
        binding.put("targetId", Long.toString(targetId));
        binding.put("failurePolicy", failurePolicy);
        binding.put("enabled", true);
        bundle.put("bundleVersion", bundle.get("bundleVersion").asLong() + 1);
    }

    public List<String> calls() {
        return List.copyOf(calls);
    }

    public long callCount(String prefix) {
        return calls.stream().filter(c -> c.startsWith(prefix)).count();
    }

    public List<JsonNode> autoRegisterRequests() {
        return List.copyOf(autoRegisterRequests);
    }

    public List<JsonNode> unverifiedRequests() {
        return List.copyOf(unverifiedRequests);
    }

    public List<JsonNode> gatewayTouches() {
        return List.copyOf(gatewayTouches);
    }

    public List<JsonNode> deployAcks() {
        return List.copyOf(deployAcks);
    }

    public ObjectNode findDevice(long sourceId, String externalId) {
        return devices.get(sourceId + "|" + externalId.toLowerCase());
    }

    private void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        calls.add(method + " " + path);
        try {
            if (down) {
                send(exchange, 503, error("SERVICE_UNAVAILABLE"));
                return;
            }
            if ("GET".equals(method) && path.equals("/internal/core/ingest-context")) {
                ObjectNode s = sources.get(Long.parseLong(query.get("sourceId")));
                send(exchange, s == null ? 404 : 200, s == null ? error("RESOURCE_NOT_FOUND") : ok(s));
                return;
            }
            Matcher device = DEVICE.matcher(path);
            if ("GET".equals(method) && device.matches()) {
                String ext = URLDecoder.decode(device.group(2), StandardCharsets.UTF_8);
                ObjectNode d = devices.get(device.group(1) + "|" + ext.toLowerCase());
                send(exchange, d == null ? 404 : 200, d == null ? error("DEVICE_NOT_FOUND") : ok(d));
                return;
            }
            if ("POST".equals(method) && path.equals("/internal/core/devices/auto-register")) {
                autoRegister(exchange, mapper.readTree(requestBody));
                return;
            }
            Matcher runtime = RUNTIME.matcher(path);
            if ("GET".equals(method) && runtime.matches()) {
                long id = Long.parseLong(runtime.group(1));
                ObjectNode r = attributes.getOrDefault(id, mapper.createObjectNode());
                r.put("deviceId", id);
                send(exchange, 200, ok(r));
                return;
            }
            if ("GET".equals(method) && path.equals("/internal/core/devices")) {
                ObjectNode list = mapper.createObjectNode();
                list.set("header", header(true, "SUCCESS"));
                list.put("page", 1);
                list.put("size", 100);
                list.put("totalPages", 1);
                ArrayNode responses = list.putArray("responses");
                devices.values().forEach(responses::add);
                list.put("totalCount", devices.size());
                send(exchange, 200, list);
                return;
            }
            if ("GET".equals(method) && path.equals("/internal/core/metrics")) {
                long version = metricVersion.get();
                if (query.containsKey("sinceVersion") && Long.parseLong(query.get("sinceVersion")) == version) {
                    send(exchange, 204, null);
                    return;
                }
                ObjectNode body = mapper.createObjectNode();
                body.put("version", version);
                ArrayNode list = body.putArray("metrics");
                metrics.values().forEach(list::add);
                ObjectNode a = body.putObject("aliases");
                aliases.forEach(a::put);
                send(exchange, 200, ok(body));
                return;
            }
            if ("POST".equals(method) && path.equals("/internal/core/metrics/register-unverified")) {
                JsonNode req = mapper.readTree(requestBody);
                unverifiedRequests.add(req);
                ObjectNode body = mapper.createObjectNode();
                ArrayNode registered = body.putArray("registered");
                body.putArray("existing");
                for (JsonNode k : req.path("keys")) {
                    String key = k.get("key").asString();
                    if (!metrics.containsKey(key)) {
                        ObjectNode m = metric(key, null, null, null);
                        m.put("status", "UNVERIFIED");
                        registered.addObject().put("key", key).put("metricId", m.get("id").asString())
                                .put("status", "UNVERIFIED");
                    }
                }
                send(exchange, 200, ok(body));
                return;
            }
            if ("POST".equals(method) && path.equals("/internal/core/gateways/touch")) {
                gatewayTouches.add(mapper.readTree(requestBody));
                send(exchange, 204, null);
                return;
            }
            if ("GET".equals(method) && path.equals("/internal/core/scripts/runtime-bundle")) {
                ObjectNode bundle = bundles.get(Long.parseLong(query.get("organizationId")));
                if (bundle == null) {
                    bundle = mapper.createObjectNode();
                    bundle.put("bundleVersion", 0);
                    bundle.putArray("scripts");
                }
                send(exchange, 200, ok(bundle.deepCopy()));
                return;
            }
            if ("POST".equals(method) && path.equals("/internal/core/scripts/deploy-acks")) {
                deployAcks.add(mapper.readTree(requestBody));
                send(exchange, 204, null);
                return;
            }
            send(exchange, 404, error("RESOURCE_NOT_FOUND"));
        } catch (RuntimeException e) {
            send(exchange, 500, error("INTERNAL_ERROR"));
        }
    }

    private synchronized void autoRegister(HttpExchange exchange, JsonNode req) throws IOException {
        autoRegisterRequests.add(req);
        long sourceId = req.get("sourceId").asLong();
        String ext = req.get("externalId").asString().toLowerCase();
        ObjectNode existing = devices.get(sourceId + "|" + ext);
        if (existing != null) {
            ObjectNode r = mapper.createObjectNode();
            r.put("deviceId", existing.get("deviceId").asString());
            r.put("status", existing.get("status").asString());
            r.put("created", false);
            send(exchange, 200, ok(r));
            return;
        }
        ObjectNode source = sources.get(sourceId);
        if (source != null && "REJECT".equals(source.path("unknownDevicePolicy").asString())) {
            send(exchange, 409, error("DEVICE_REJECTED"));
            return;
        }
        int limit = quotas.getOrDefault(sourceId, Integer.MAX_VALUE);
        if (autoRegistered.computeIfAbsent(sourceId, s -> new AtomicInteger()).get() >= limit) {
            send(exchange, 429, error("DEVICE_AUTOREG_LIMIT"));
            return;
        }
        autoRegistered.get(sourceId).incrementAndGet();
        long id = deviceIds.incrementAndGet();
        ObjectNode d = device(id, req.get("organizationId").asLong(), sourceId, ext, "PENDING", null, null, null);
        if (req.hasNonNull("name")) {
            d.put("name", req.get("name").asString());
        }
        ObjectNode r = mapper.createObjectNode();
        r.put("deviceId", Long.toString(id));
        r.put("status", "PENDING");
        r.put("created", true);
        send(exchange, 200, ok(r));
    }

    public int autoRegisteredCount(long sourceId) {
        AtomicInteger n = autoRegistered.get(sourceId);
        return n == null ? 0 : n.get();
    }

    private ObjectNode ok(JsonNode response) {
        ObjectNode body = mapper.createObjectNode();
        body.set("header", header(true, "SUCCESS"));
        body.set("response", response);
        return body;
    }

    private ObjectNode error(String code) {
        ObjectNode body = mapper.createObjectNode();
        body.set("header", header(false, code));
        return body;
    }

    private ObjectNode header(boolean ok, String code) {
        ObjectNode h = mapper.createObjectNode();
        h.put("isSuccessful", ok);
        h.put("resultCode", code);
        h.put("resultMessage", code);
        return h;
    }

    private void send(HttpExchange exchange, int status, JsonNode body) throws IOException {
        byte[] bytes = body == null ? new byte[0] : mapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, status == 204 ? -1 : bytes.length);
        if (status != 204) {
            exchange.getResponseBody().write(bytes);
        }
        exchange.close();
    }

    private static Map<String, String> query(String raw) {
        if (raw == null || raw.isEmpty()) {
            return Map.of();
        }
        Map<String, String> q = new HashMap<>();
        for (String part : raw.split("&")) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                q.put(part.substring(0, eq), URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(q));
    }

    /** 아직 쓰지 않는 상태 정리용 */
    public List<String> sourceIds() {
        return new ArrayList<>(sources.keySet().stream().map(String::valueOf).toList());
    }
}
