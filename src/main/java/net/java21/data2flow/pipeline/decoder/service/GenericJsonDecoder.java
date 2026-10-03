package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.decoder.DecodeException;
import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import net.java21.data2flow.contracts.message.decoder.DecoderKeys;
import net.java21.data2flow.contracts.message.decoder.PayloadDecoder;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 기본 디코더 {@code generic-json}(ING-02.03): 코드 없이 매핑 설정만으로 새 JSON 형식을 받는다.
 *
 * <pre>{@code
 * {"deviceIdFrom": "topic[1]" | "$.id",
 *  "timePath": "$.ts", "timeFormat": "AUTO|EPOCH_S|EPOCH_MS|ISO8601",   // 없으면 수신 시각(DSC domain-model §2.2). timeFrom은 예전 이름(별칭)
 *  "metrics": [{"path": "$.temp", "key": "temperature", "unit": "℃"}]   // 또는 {"$.temp": "temperature"}
 *  "items": {"path": "$.sensors[*]", "keyFrom": "$.name", "valueFrom": "$.value", "unitFrom": "$.unit"}}
 * }</pre>
 * 경로가 없는 항목은 건너뛴다. 설정 검증은 {@link GenericJsonMappingValidator}(소스 저장 때 같은 규칙).
 */
public class GenericJsonDecoder implements PayloadDecoder {

    private static final Pattern TOPIC_INDEX = Pattern.compile("topic\\[(\\d+)]");

    private final JsonMapper mapper = MessageCodec.newMapper();

    @Override
    public String key() {
        return DecoderKeys.GENERIC_JSON;
    }

    @Override
    public String version() {
        return "1";
    }

    @Override
    public DecodedUplink decode(RawEnvelope raw, JsonNode config) throws DecodeException {
        List<String> errors = GenericJsonMappingValidator.validate(config);
        if (!errors.isEmpty()) {
            throw IngestDecodeException.failed(key(), "매핑 설정 오류: " + String.join("; ", errors));
        }
        JsonNode root;
        try {
            root = mapper.readTree(raw.payload());
        } catch (RuntimeException e) {
            throw new IngestDecodeException(key(), IngestDecodeException.RESULT_CODE,
                    "JSON을 해석하지 못했습니다: " + ChirpStackV4Decoder.firstLine(e.getMessage()), null, e);
        }
        String externalId = externalId(raw.topic(), root, config.get("deviceIdFrom").asString());
        if (externalId == null || externalId.isBlank()) {
            throw new IngestDecodeException(key(), IngestDecodeException.EXTERNAL_ID_MISSING,
                    "기기 ID를 찾을 수 없습니다: " + config.get("deviceIdFrom").asString());
        }
        String timePath = timePath(config);
        Instant measuredAt = timePath == null ? null
                : time(JsonPaths.first(root, timePath), config.path("timeFormat").asString("AUTO"));
        List<DecodedValue> values = new ArrayList<>();
        JsonNode metrics = config.get("metrics");
        if (metrics != null && metrics.isArray()) {
            for (JsonNode m : metrics) {
                add(values, JsonPaths.first(root, m.get("path").asString()), m.get("key").asString(),
                        m.hasNonNull("unit") ? m.get("unit").asString() : null);
            }
        } else if (metrics != null && metrics.isObject()) {
            for (Map.Entry<String, JsonNode> e : metrics.properties()) {
                add(values, JsonPaths.first(root, e.getKey()), e.getValue().asString(), null);
            }
        }
        JsonNode items = config.get("items");
        if (items != null && items.isObject()) {
            for (JsonNode item : JsonPaths.select(root, items.get("path").asString())) {
                JsonNode key = JsonPaths.first(item, items.path("keyFrom").asString("$.key"));
                JsonNode unit = items.hasNonNull("unitFrom") ? JsonPaths.first(item, items.get("unitFrom").asString()) : null;
                if (key != null && key.isValueNode()) {
                    add(values, JsonPaths.first(item, items.path("valueFrom").asString("$.value")), key.asString(),
                            unit == null ? null : unit.asString());
                }
            }
        }
        return new DecodedUplink(externalId, measuredAt, values, null, Map.of());
    }

    private static void add(List<DecodedValue> values, JsonNode node, String key, String unit) {
        if (node == null || node.isNull() || !node.isValueNode()) {
            return; // 경로 미존재 항목은 건너뜀
        }
        Object value = node.isNumber() ? node.asDouble() : node.isBoolean() ? (Object) node.asBoolean() : node.asString();
        values.add(new DecodedValue(key, value, unit));
    }

    private static String externalId(String topic, JsonNode root, String from) {
        Matcher m = TOPIC_INDEX.matcher(from);
        if (m.matches()) {
            String[] parts = (topic == null ? "" : topic).split("/");
            int index = Integer.parseInt(m.group(1));
            return index < parts.length ? parts[index] : null;
        }
        JsonNode node = JsonPaths.first(root, from);
        return node == null || !node.isValueNode() ? null : node.asString();
    }

    /** 시각 형식(DSC domain-model §2.2 {@code timeFormat}) */
    static final java.util.Set<String> TIME_FORMATS = java.util.Set.of("AUTO", "EPOCH_S", "EPOCH_MS", "ISO8601");

    /** 측정 시각 경로: 문서 이름 {@code timePath}, 예전 이름 {@code timeFrom}도 읽는다 */
    static String timePath(JsonNode config) {
        if (config.hasNonNull("timePath")) {
            return config.get("timePath").asString();
        }
        return config.hasNonNull("timeFrom") ? config.get("timeFrom").asString() : null;
    }

    private static Instant time(JsonNode node, String format) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber() || "EPOCH_S".equals(format) || "EPOCH_MS".equals(format)) {
            long v;
            try {
                v = node.isNumber() ? node.asLong() : Long.parseLong(node.asString().trim());
            } catch (NumberFormatException e) {
                return null;
            }
            return switch (format) {
                case "EPOCH_S" -> Instant.ofEpochSecond(v);
                case "EPOCH_MS" -> Instant.ofEpochMilli(v);
                default -> v < 100_000_000_000L ? Instant.ofEpochSecond(v) : Instant.ofEpochMilli(v);
            };
        }
        try {
            return Instant.parse(node.asString());
        } catch (DateTimeParseException e) {
            try {
                return OffsetDateTime.parse(node.asString()).toInstant();
            } catch (DateTimeParseException e2) {
                return null;
            }
        }
    }
}
