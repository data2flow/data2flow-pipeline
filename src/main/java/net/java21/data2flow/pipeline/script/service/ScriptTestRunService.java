package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.common.PayloadEncoding;
import net.java21.data2flow.pipeline.script.domain.ScriptErrorCode;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import net.java21.data2flow.pipeline.script.dto.ScriptTestRunRequest;
import net.java21.data2flow.pipeline.script.dto.ScriptTestRunResponse;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 테스트 실행(SCR-03.02, API-SCR-31). 운영과 같은 샌드박스·제한으로 실행하고 결과만 돌려준다. 아무것도 저장하지 않고
 * 스트림에도 발행하지 않는다(BR-SCR-08).
 */
@Service
public class ScriptTestRunService {

    private final ScriptSandbox sandbox;
    private final ScriptOutputValidator validator;
    private final ObjectProvider<RawInputLoader> rawInputs;
    private final ObjectProvider<ScriptConfigLookup> configs;
    private final Clock clock;
    private final JsonMapper mapper = MessageCodec.newMapper();

    public ScriptTestRunService(ScriptSandbox sandbox, ScriptOutputValidator validator,
                                ObjectProvider<RawInputLoader> rawInputs, ObjectProvider<ScriptConfigLookup> configs,
                                Clock clock) {
        this.sandbox = sandbox;
        this.validator = validator;
        this.rawInputs = rawInputs;
        this.configs = configs;
        this.clock = clock;
    }

    public ScriptTestRunResponse run(ScriptTestRunRequest request) {
        Instant now = clock.instant();
        JsonNode input = request.kind() == ScriptKind.DECODE ? decodeInput(request, now) : request.input();
        if (input == null || input.isNull()) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
        }
        ObjectNode ctx = context(request);
        ScriptOutcome outcome = sandbox.run(request.kind(), request.code(), "script.js", mapper.writeValueAsString(input),
                mapper.writeValueAsString(ctx), now);
        List<ScriptTestRunResponse.Log> logs = outcome.logs().stream()
                .map(l -> new ScriptTestRunResponse.Log(now.toString(), l)).toList();
        if (!outcome.ok()) {
            return failure(outcome, logs, outcome.failure().code().name(), outcome.failure().message(),
                    outcome.failure().line(), outcome.failure().col());
        }
        try {
            ScriptTestRunResponse.Diff diff;
            if (request.kind() == ScriptKind.DECODE) {
                validator.toDecodedUplink(outcome.output());
                diff = diff(Map.of(), metrics(outcome.output()));
            } else if (outcome.returnedNull()) {
                diff = diff(metrics(input), Map.of());
            } else {
                validator.transformMetrics(outcome.output());
                diff = diff(metrics(input), metrics(outcome.output()));
            }
            return new ScriptTestRunResponse(true, outcome.output(), diff, logs, outcome.durationMs(),
                    outcome.outputBytes(), null);
        } catch (ScriptOutputValidator.OutputContractException e) {
            return failure(outcome, logs, ScriptErrorCode.SCRIPT_OUTPUT_INVALID.name(), e.getMessage(), null, null);
        }
    }

    private ScriptTestRunResponse failure(ScriptOutcome outcome, List<ScriptTestRunResponse.Log> logs, String code,
                                          String message, Integer line, Integer col) {
        return new ScriptTestRunResponse(false, outcome.output(), null, logs, outcome.durationMs(), outcome.outputBytes(),
                new ScriptTestRunResponse.Error(code, message, line, col, null));
    }

    private JsonNode decodeInput(ScriptTestRunRequest request, Instant now) {
        if (request.rawMessageId() != null) {
            RawInputLoader loader = rawInputs.getIfAvailable();
            Optional<RawInputLoader.RawInput> raw = loader == null || request.organizationId() == null
                    ? Optional.empty() : loader.load(request.organizationId(), request.rawMessageId());
            RawInputLoader.RawInput r = raw.orElseThrow(() -> new BusinessException(CommonErrorCode.RESOURCE_NOT_FOUND));
            return ScriptInputs.decodeInput(mapper, r.topic(), r.payload(), r.receivedAt(), r.sourceId(), r.sourceType(),
                    null);
        }
        JsonNode in = request.input();
        if (in == null || !in.isObject()) {
            return null;
        }
        byte[] payload = payloadBytes(in.get("payload"));
        Instant receivedAt = in.hasNonNull("receivedAt") ? Instant.parse(in.get("receivedAt").asString()) : now;
        JsonNode source = in.get("source");
        JsonNode sourceConfig = source != null && source.has("config") ? source.get("config") : null;
        ObjectNode built = ScriptInputs.decodeInput(mapper, in.hasNonNull("topic") ? in.get("topic").asString() : null,
                payload, receivedAt, 0, "TEST", sourceConfig);
        if (source != null && source.hasNonNull("code")) {
            ((ObjectNode) built.get("source")).put("code", source.get("code").asString());
        }
        return built;
    }

    /** payload: JSON 객체·배열이면 그 JSON, 문자열이면 base64로 풀고 실패하면 UTF-8 글자 */
    private byte[] payloadBytes(JsonNode payload) {
        if (payload == null || payload.isNull()) {
            return new byte[0];
        }
        if (payload.isObject() || payload.isArray()) {
            return mapper.writeValueAsBytes(payload);
        }
        String text = payload.asString();
        try {
            byte[] decoded = Base64.getDecoder().decode(text.strip());
            if (decoded.length > 0 && PayloadEncoding.detect(decoded) != PayloadEncoding.TEXT) {
                return decoded;
            }
            return decoded.length > 0 && looksLikeBase64(text) ? decoded : text.getBytes(StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
    }

    private static boolean looksLikeBase64(String text) {
        String t = text.strip();
        return t.length() % 4 == 0 && t.matches("[A-Za-z0-9+/]+={0,2}");
    }

    private ObjectNode context(ScriptTestRunRequest request) {
        ObjectNode ctx = mapper.createObjectNode();
        JsonNode given = request.context();
        if (given != null && given.isObject()) {
            given.properties().forEach(e -> ctx.set(e.getKey(), e.getValue()));
        }
        if (!ctx.has("config") && request.scriptId() != null && request.organizationId() != null) {
            ScriptConfigLookup lookup = configs.getIfAvailable();
            if (lookup != null) {
                lookup.config(request.organizationId(), request.scriptId()).ifPresent(c -> ctx.set("config", c));
            }
        }
        JsonNode last = ctx.get("last");
        if (last != null && last.isObject()) {
            ObjectNode wrapped = mapper.createObjectNode();
            last.properties().forEach(e -> {
                if (e.getValue().isNumber()) {
                    ObjectNode v = wrapped.putObject(e.getKey());
                    v.set("value", e.getValue());
                } else {
                    wrapped.set(e.getKey(), e.getValue());
                }
            });
            ctx.set("last", wrapped);
        }
        return ctx;
    }

    private static Map<String, Double> metrics(JsonNode message) {
        Map<String, Double> values = new LinkedHashMap<>();
        JsonNode metrics = message == null ? null : message.get("metrics");
        if (metrics != null && metrics.isArray()) {
            for (JsonNode m : metrics) {
                if (m.hasNonNull("key")) {
                    JsonNode v = m.get("value");
                    values.put(m.get("key").asString(), v != null && v.isNumber() ? v.asDouble() : null);
                }
            }
        }
        return values;
    }

    private static ScriptTestRunResponse.Diff diff(Map<String, Double> before, Map<String, Double> after) {
        List<String> added = new ArrayList<>();
        List<String> removed = new ArrayList<>();
        List<ScriptTestRunResponse.Change> changed = new ArrayList<>();
        after.forEach((k, v) -> {
            if (!before.containsKey(k)) {
                added.add(k);
            } else if (v != null && !v.equals(before.get(k))) {
                changed.add(new ScriptTestRunResponse.Change(k, before.get(k), v));
            }
        });
        before.keySet().stream().filter(k -> !after.containsKey(k)).forEach(removed::add);
        return new ScriptTestRunResponse.Diff(added, removed, changed);
    }
}
