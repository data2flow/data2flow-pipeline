package net.java21.data2flow.pipeline.device.service;

import net.java21.data2flow.contracts.http.InternalHttpClients;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.domain.AutoRegisterResult;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import net.java21.data2flow.pipeline.device.domain.DeviceRuntime;
import net.java21.data2flow.pipeline.device.domain.SourceContext;
import net.java21.data2flow.pipeline.device.domain.UnknownDevicePolicy;
import net.java21.data2flow.pipeline.metric.domain.MetricCatalog;
import net.java21.data2flow.pipeline.metric.domain.MetricDefinition;
import net.java21.data2flow.pipeline.script.domain.FailurePolicy;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * core-api 내부 API 클라이언트(ADR-021: 토큰 없음, {@code X-CALLER-SERVICE: data2flow-pipeline}, HTTP 80). 응답은 공통 봉투
 * {@code {header, response}}(목록은 {@code responses})이고, 모르는 필드는 무시한다(core가 필드를 먼저 더해도 깨지지 않음).
 *
 * <p>호출하는 API(정본: design/api/DEV-api.md §12, ING-api.md §2, SCR-api.md §2):
 * API-ING-21, API-DEV-120~125·130, API-SCR-32·34.
 */
public class CoreApiClient implements CoreDirectory {

    public static final String CALLER_HEADER = "X-CALLER-SERVICE";
    public static final String CALLER = "data2flow-pipeline";

    private final HttpClient http;
    private final String baseUrl;
    private final PipelineProperties.Core settings;
    private final JsonMapper mapper = MessageCodec.newMapper();

    public CoreApiClient(PipelineProperties.Core settings) {
        this.settings = settings;
        this.baseUrl = settings.baseUrl().endsWith("/") ? settings.baseUrl().substring(0, settings.baseUrl().length() - 1)
                : settings.baseUrl();
        this.http = InternalHttpClients.create(settings.connectTimeout());
    }

    @Override
    public Optional<SourceContext> ingestContext(long sourceId) {
        Response r = get("/internal/core/ingest-context?sourceId=" + sourceId);
        if (r.status == 404) {
            return Optional.empty();
        }
        JsonNode body = r.response();
        JsonNode decoder = body.path("decoder");
        String type = text(decoder, "type");
        String key = text(decoder, "key");
        if (key == null) {
            key = text(body, "decoderKey");
        }
        if (key == null) {
            key = type == null || "builtin".equalsIgnoreCase(type) ? null : type;
        }
        Long scriptId = longOrNull(decoder, "scriptId");
        if (scriptId == null) {
            scriptId = longOrNull(body, "decodeScriptId");
        }
        if ("script".equalsIgnoreCase(type) && key == null) {
            key = "script";
        }
        JsonNode config = decoder.has("config") ? decoder.get("config") : body.path("decoderConfig");
        return Optional.of(new SourceContext(
                lng(body, "sourceId", sourceId),
                lng(body, "organizationId", 0),
                key == null ? "chirpstack-v4" : key,
                scriptId,
                config.isMissingNode() || config.isNull() ? mapper.createObjectNode() : config,
                UnknownDevicePolicy.parse(text(body, "unknownDevicePolicy")),
                (int) lng(body, "autoRegisterHourlyLimit", lng(body, "autoregLimitPerHour", 100)),
                longOrNull(body, "defaultModelId"),
                longOrNull(body, "defaultSpaceId"),
                lng(body, "contextVersion", 0)));
    }

    @Override
    public Optional<DeviceInfo> findDevice(long sourceId, String externalId) {
        Response r = get("/internal/core/sources/" + sourceId + "/devices/" + encode(externalId));
        if (r.status == 404) {
            return Optional.empty();
        }
        return Optional.of(device(r.response(), sourceId, externalId));
    }

    @Override
    public AutoRegisterResult autoRegister(AutoRegisterCommand command) {
        ObjectNode body = mapper.createObjectNode();
        body.put("organizationId", command.organizationId());
        body.put("sourceId", command.sourceId());
        body.put("externalId", command.externalId());
        if (command.name() != null) {
            body.put("name", command.name());
        }
        body.set("sourceMeta", command.sourceMeta() == null ? mapper.createObjectNode() : command.sourceMeta());
        body.put("firstSeenAt", command.firstSeenAt().toString());
        ArrayNode metrics = body.putArray("metrics");
        command.metricKeys().forEach(metrics::add);
        Response r = post("/internal/core/devices/auto-register", body);
        if (r.status == 429) {
            return new AutoRegisterResult(null, false, AutoRegisterResult.Outcome.QUOTA_EXCEEDED);
        }
        if (r.status == 409) {
            return new AutoRegisterResult(null, false, AutoRegisterResult.Outcome.REJECTED);
        }
        r.requireSuccess();
        JsonNode response = r.response();
        if (response.path("rejected").asBoolean(false)) {
            // API-ING-20(대체됨) 모양: 200 {created:false, rejected:true, reason:"ING_AUTO_REGISTER_QUOTA"}
            boolean quota = "ING_AUTO_REGISTER_QUOTA".equals(text(response, "reason"));
            return new AutoRegisterResult(null, false,
                    quota ? AutoRegisterResult.Outcome.QUOTA_EXCEEDED : AutoRegisterResult.Outcome.REJECTED);
        }
        return new AutoRegisterResult(lng(response, "deviceId", 0), response.path("created").asBoolean(false),
                AutoRegisterResult.Outcome.REGISTERED);
    }

    @Override
    public DeviceRuntime deviceRuntime(long deviceId) {
        Response r = get("/internal/core/devices/" + deviceId + "/runtime");
        if (r.status == 404) {
            return new DeviceRuntime(deviceId, mapper.createObjectNode());
        }
        JsonNode attributes = r.response().path("attributes");
        return new DeviceRuntime(deviceId, attributes.isObject() ? attributes : mapper.createObjectNode());
    }

    @Override
    public DevicePage listDevices(Instant updatedAfter, int page, int size) {
        StringBuilder path = new StringBuilder("/internal/core/devices?page=").append(page).append("&size=").append(size);
        if (updatedAfter != null) {
            path.append("&updatedAfter=").append(encode(updatedAfter.toString()));
        }
        Response r = get(path.toString());
        r.requireSuccess();
        List<DeviceInfo> devices = new ArrayList<>();
        JsonNode list = r.body.path("responses");
        for (JsonNode d : list) {
            devices.add(device(d, lng(d, "sourceId", 0), text(d, "externalId")));
        }
        return new DevicePage(devices, (int) lng(r.body, "page", page), (int) lng(r.body, "totalPages", page));
    }

    @Override
    public Optional<MetricCatalog> metrics(long organizationId, Long sinceVersion) {
        String path = "/internal/core/metrics?organizationId=" + organizationId
                + (sinceVersion == null ? "" : "&sinceVersion=" + sinceVersion);
        Response r = get(path);
        if (r.status == 204) {
            return Optional.empty();
        }
        r.requireSuccess();
        JsonNode body = r.response();
        Map<String, MetricDefinition> metrics = new LinkedHashMap<>();
        for (JsonNode m : body.path("metrics")) {
            Map<String, Double> enumMap = new LinkedHashMap<>();
            m.path("enumMap").properties().forEach(e -> enumMap.put(e.getKey().toLowerCase(), e.getValue().asDouble()));
            String key = text(m, "key");
            if (key != null) {
                metrics.put(key, new MetricDefinition(lng(m, "id", 0), key, text(m, "unit"), text(m, "valueType"),
                        doubleOrNull(m, "validMin"), doubleOrNull(m, "validMax"),
                        m.hasNonNull("status") ? m.get("status").asString() : "VERIFIED", enumMap,
                        m.path("stateType").asBoolean(false)));
            }
        }
        Map<String, String> aliases = new LinkedHashMap<>();
        JsonNode aliasNode = body.path("aliases");
        if (aliasNode.isObject()) {
            aliasNode.properties().forEach(e -> aliases.put(e.getKey(), e.getValue().asString()));
        } else if (aliasNode.isArray()) {
            for (JsonNode a : aliasNode) {
                aliases.put(text(a, "alias"), text(a, "key") != null ? text(a, "key") : text(a, "metricKey"));
            }
        }
        return Optional.of(new MetricCatalog(lng(body, "version", 0), metrics, aliases));
    }

    @Override
    public List<String> registerUnverified(long organizationId, List<UnverifiedKey> keys) {
        ObjectNode body = mapper.createObjectNode();
        body.put("organizationId", organizationId);
        ArrayNode list = body.putArray("keys");
        for (UnverifiedKey k : keys) {
            ObjectNode item = list.addObject();
            item.put("key", k.key());
            item.put("deviceId", k.deviceId());
            item.put("sampleValue", k.sampleValue());
        }
        Response r = post("/internal/core/metrics/register-unverified", body);
        r.requireSuccess();
        List<String> done = new ArrayList<>();
        for (JsonNode n : r.response().path("registered")) {
            done.add(n.isString() ? n.asString() : text(n, "key"));
        }
        for (JsonNode n : r.response().path("existing")) {
            done.add(n.isString() ? n.asString() : text(n, "key"));
        }
        return done;
    }

    @Override
    public void touchGateways(List<GatewayTouch> items) {
        ObjectNode body = mapper.createObjectNode();
        ArrayNode list = body.putArray("items");
        for (GatewayTouch t : items) {
            ObjectNode item = list.addObject();
            item.put("sourceId", t.sourceId());
            item.put("gatewayEui", t.gatewayEui());
            item.put("seenAt", t.seenAt().toString());
        }
        post("/internal/core/gateways/touch", body).requireSuccess();
    }

    @Override
    public RuntimeBundle runtimeBundle(long organizationId) {
        Response r = get("/internal/core/scripts/runtime-bundle?organizationId=" + organizationId);
        if (r.status == 404) {
            return RuntimeBundle.EMPTY;
        }
        r.requireSuccess();
        JsonNode body = r.response();
        List<RuntimeBundle.Script> scripts = new ArrayList<>();
        for (JsonNode s : body.path("scripts")) {
            List<RuntimeBundle.Binding> bindings = new ArrayList<>();
            for (JsonNode b : s.path("bindings")) {
                bindings.add(new RuntimeBundle.Binding(text(b, "targetType"), lng(b, "targetId", 0),
                        b.path("enabled").asBoolean(true),
                        b.hasNonNull("failurePolicy") ? FailurePolicy.parse(b.get("failurePolicy").asString()) : null));
            }
            String status = text(s, "status");
            List<String> moduleRefs = new ArrayList<>();
            for (JsonNode m : s.path("moduleRefs")) {
                if (m.isString()) {
                    moduleRefs.add(m.asString());
                } else if (text(m, "name") != null) {
                    moduleRefs.add(text(m, "name") + "@" + lng(m, "version", lng(m, "versionNo", 0)));
                }
            }
            String capture = text(s, "logCaptureUntil");
            scripts.add(new RuntimeBundle.Script(lng(s, "scriptId", 0),
                    "DECODE".equalsIgnoreCase(text(s, "kind")) ? ScriptKind.DECODE : ScriptKind.TRANSFORM,
                    lng(s, "versionId", 0), (int) lng(s, "versionNo", 0), text(s, "code"),
                    s.path("config").isObject() ? s.get("config") : mapper.createObjectNode(),
                    FailurePolicy.parse(text(s, "failurePolicy")),
                    status == null || "ENABLED".equalsIgnoreCase(status), bindings, moduleRefs,
                    lng(s, "configRevision", lng(s, "configVersion", 0)),
                    capture == null ? null : Instant.parse(capture)));
        }
        List<RuntimeBundle.Module> modules = new ArrayList<>();
        for (JsonNode m : body.path("modules")) {
            modules.add(new RuntimeBundle.Module(text(m, "name"), (int) lng(m, "versionNo", lng(m, "version", 0)),
                    text(m, "code")));
        }
        List<RuntimeBundle.Formula> formulas = new ArrayList<>();
        for (JsonNode f : body.path("formulaMetrics")) {
            if (text(f, "status") != null && !"ACTIVE".equalsIgnoreCase(text(f, "status"))) {
                continue;
            }
            formulas.add(new RuntimeBundle.Formula(lng(f, "id", 0), text(f, "resultKey"), text(f, "unit"),
                    text(f, "expression"), text(f, "compiledJs"), text(f, "targetType"), lng(f, "targetId", 0)));
        }
        return new RuntimeBundle(lng(body, "bundleVersion", 0), scripts, modules, formulas);
    }

    @Override
    public void deployAck(String instance, long scriptId, long versionId, Instant appliedAt) {
        ObjectNode body = mapper.createObjectNode();
        body.put("instance", instance);
        body.put("scriptId", Long.toString(scriptId));
        body.put("versionId", Long.toString(versionId));
        body.put("appliedAt", appliedAt.toString());
        post("/internal/core/scripts/deploy-acks", body).requireSuccess();
    }

    @Override
    public Optional<List<net.java21.data2flow.pipeline.retention.domain.RetentionPolicies>> retentionPolicies() {
        Response r = get("/internal/core/retention-policies");
        if (r.status == 404) {
            return Optional.empty();
        }
        JsonNode body = r.response();
        List<net.java21.data2flow.pipeline.retention.domain.RetentionPolicies> out = new ArrayList<>();
        JsonNode orgs = body.has("organizations") ? body.get("organizations") : body.path("responses");
        for (JsonNode o : orgs) {
            List<net.java21.data2flow.pipeline.retention.domain.RetentionPolicies.Item> items = new ArrayList<>();
            JsonNode list = o.has("effective") ? o.get("effective") : o.path("policies");
            for (JsonNode i : list) {
                items.add(new net.java21.data2flow.pipeline.retention.domain.RetentionPolicies.Item(text(i, "scope"),
                        text(i, "scopeRef"), text(i, "dataClass"), (int) lng(i, "retainDays", 0),
                        intOrNull(i, "compressAfterDays"), i.path("archiveBeforeDelete").asBoolean(false),
                        text(i, "storeMode")));
            }
            out.add(new net.java21.data2flow.pipeline.retention.domain.RetentionPolicies(lng(o, "organizationId", 0),
                    lng(o, "version", 0), items));
        }
        return Optional.of(out);
    }

    @Override
    public void registerArchive(ArchiveFile file) {
        ObjectNode body = mapper.createObjectNode();
        body.put("organizationId", Long.toString(file.organizationId()));
        body.put("dataClass", file.dataClass());
        body.put("rangeFrom", file.rangeFrom().toString());
        body.put("rangeTo", file.rangeTo().toString());
        body.put("objectKey", file.objectKey());
        body.put("format", "PARQUET");
        body.put("rowsCount", file.rowsCount());
        body.put("bytes", file.bytes());
        body.put("checksum", file.checksum());
        post("/internal/core/archive-files", body).requireSuccess();
    }

    private DeviceInfo device(JsonNode d, long sourceId, String externalId) {
        JsonNode model = d.path("model");
        return new DeviceInfo(lng(d, "deviceId", lng(d, "id", 0)), lng(d, "organizationId", 0),
                lng(d, "sourceId", sourceId),
                text(d, "externalId") != null ? text(d, "externalId") : externalId,
                text(d, "name"), text(d, "status") == null ? "ACTIVE" : text(d, "status"),
                longOrNull(d, "modelId"), text(d, "modelCode") != null ? text(d, "modelCode") : text(model, "code"),
                longOrNull(d, "spaceId"), d.path("virtual").asBoolean(false), d.path("ignored").asBoolean(false),
                intOrNull(d, "expectedIntervalSec"), doubleOrNull(d, "offlineMultiplier"),
                intOrNull(d, "modelExpectedIntervalSec") != null ? intOrNull(d, "modelExpectedIntervalSec")
                        : intOrNull(model, "defaultIntervalSec"),
                doubleOrNull(d, "modelOfflineMultiplier") != null ? doubleOrNull(d, "modelOfflineMultiplier")
                        : doubleOrNull(model, "defaultOfflineMultiplier"),
                text(d, "timezone"));
    }

    private Response get(String path) {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET());
    }

    private Response post(String path, JsonNode body) {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(body))));
    }

    private Response send(HttpRequest.Builder builder) {
        HttpRequest request = builder.timeout(settings.readTimeout())
                .header(CALLER_HEADER, CALLER)
                .header("Accept", "application/json")
                .header("Accept-Language", "ko")
                .build();
        try {
            HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() >= 500) {
                throw new CoreUnavailableException("core-api " + response.statusCode() + " " + request.uri().getPath(), null);
            }
            JsonNode body = response.body() == null || response.body().length == 0 ? mapper.createObjectNode()
                    : mapper.readTree(response.body());
            return new Response(response.statusCode(), body, request.uri().getPath());
        } catch (IOException e) {
            throw new CoreUnavailableException("core-api에 연결할 수 없습니다: " + request.uri().getPath(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CoreUnavailableException("core-api 호출이 중단되었습니다", e);
        } catch (RuntimeException e) {
            if (e instanceof CoreUnavailableException unavailable) {
                throw unavailable;
            }
            throw new CoreUnavailableException("core-api 응답을 읽을 수 없습니다: " + request.uri().getPath(), e);
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return v == null || v.isNull() || v.isMissingNode() ? null : v.asString();
    }

    private static Long longOrNull(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        if (v == null || v.isNull()) {
            return null;
        }
        if (v.isNumber()) {
            return v.asLong();
        }
        try {
            return Long.parseLong(v.asString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 숫자 또는 숫자 글자(core는 ID를 JSON 문자열로 준다, api-rules) */
    private static long lng(JsonNode node, String field, long defaultValue) {
        Long v = longOrNull(node, field);
        return v == null ? defaultValue : v;
    }

    private static Integer intOrNull(JsonNode node, String field) {
        Long v = longOrNull(node, field);
        return v == null ? null : v.intValue();
    }

    private static Double doubleOrNull(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return v == null || !v.isNumber() ? null : v.asDouble();
    }

    private record Response(int status, JsonNode body, String path) {

        JsonNode response() {
            requireSuccess();
            JsonNode r = body.get("response");
            return r == null || r.isNull() ? body : r;
        }

        void requireSuccess() {
            if (status >= 400) {
                String code = body.path("header").path("resultCode").asString("");
                throw new CoreRejectedException(status, code, path);
            }
        }
    }

    /** core-api가 요청을 거부했다(4xx). 일시 장애가 아니므로 다시 시도해도 결과가 같다 */
    public static class CoreRejectedException extends RuntimeException {
        private final int status;
        private final String resultCode;

        public CoreRejectedException(int status, String resultCode, String path) {
            super("core-api " + status + " " + resultCode + " " + path);
            this.status = status;
            this.resultCode = resultCode;
        }

        public int status() {
            return status;
        }

        public String resultCode() {
            return resultCode;
        }
    }
}
