package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 스크립트 반환값 계약(BR-SCR-06, SCR-api §3). 어기면 {@link OutputContractException}(SCRIPT_OUTPUT_INVALID).
 *
 * <ul>
 *   <li>DECODE: {@code externalId}(1~128자)와 {@code metrics[]} 필수, 측정 항목 100개 이하</li>
 *   <li>TRANSFORM: 표준 메시지(바꿀 수 있는 것은 metrics·measuredAt·link·meta) 또는 null</li>
 *   <li>측정 키 {@code ^[A-Za-z][A-Za-z0-9_]{0,63}$}(대소문자 섞임 허용: WS302 {@code LAeq}·{@code LAI}·{@code LAImax}), 값은 유한한 숫자
 *       (불린은 1/0)</li>
 * </ul>
 */
public final class ScriptOutputValidator {

    /** 측정 키 형식(BR-SCR-06). 대소문자를 섞어 쓸 수 있다 */
    public static final Pattern METRIC_KEY = Pattern.compile("^[A-Za-z][A-Za-z0-9_]{0,63}$");

    private final int maxMetrics;

    public ScriptOutputValidator(int maxMetrics) {
        this.maxMetrics = maxMetrics;
    }

    /** DECODE 반환값 → 기기 식별 전 표준 형태 */
    public DecodedUplink toDecodedUplink(JsonNode output) {
        if (output == null || !output.isObject()) {
            throw new OutputContractException("decode는 객체를 반환해야 합니다");
        }
        JsonNode externalId = output.get("externalId");
        if (externalId == null || !externalId.isString() || externalId.asString().isBlank()
                || externalId.asString().length() > DecodedUplink.MAX_EXTERNAL_ID_LENGTH) {
            throw new OutputContractException("externalId가 필요합니다(1~128자 문자열)");
        }
        List<DecodedValue> values = new ArrayList<>();
        for (Metric m : metrics(output, true)) {
            values.add(new DecodedValue(m.key(), m.value(), m.unit()));
        }
        Instant measuredAt = instant(output.get("measuredAt"), "measuredAt");
        CanonicalTelemetry.Link link = link(output.get("link"));
        Map<String, String> tags = tags(output.get("meta"));
        return new DecodedUplink(externalId.asString(), measuredAt, values, link, tags);
    }

    /**
     * TRANSFORM 반환값의 측정 항목. null 반환은 호출하는 쪽이 먼저 걸러야 한다.
     */
    public List<Metric> transformMetrics(JsonNode output) {
        if (output == null || !output.isObject()) {
            throw new OutputContractException("transform은 표준 메시지(객체) 또는 null을 반환해야 합니다");
        }
        return metrics(output, true);
    }

    /** TRANSFORM이 바꾼 측정 시각. 없거나 형식이 틀리면 예외, 필드가 없으면 null */
    public Instant transformMeasuredAt(JsonNode output) {
        return instant(output.get("measuredAt"), "measuredAt");
    }

    private List<Metric> metrics(JsonNode output, boolean required) {
        JsonNode metrics = output.get("metrics");
        if (metrics == null || metrics.isNull()) {
            if (required) {
                throw new OutputContractException("metrics 배열이 필요합니다");
            }
            return List.of();
        }
        if (!metrics.isArray()) {
            throw new OutputContractException("metrics는 배열이어야 합니다");
        }
        if (metrics.size() > maxMetrics) {
            throw new OutputContractException("metrics는 " + maxMetrics + "개 이하여야 합니다: " + metrics.size());
        }
        Map<String, Metric> byKey = new LinkedHashMap<>();
        for (int i = 0; i < metrics.size(); i++) {
            JsonNode m = metrics.get(i);
            if (m == null || !m.isObject()) {
                throw new OutputContractException("metrics[" + i + "]는 객체여야 합니다");
            }
            JsonNode key = m.get("key");
            if (key == null || !key.isString() || !METRIC_KEY.matcher(key.asString()).matches()) {
                throw new OutputContractException("metrics[" + i + "].key는 영문자로 시작하는 64자 이하(영문·숫자·_)여야 합니다");
            }
            JsonNode value = m.get("value");
            double number;
            if (value != null && value.isNumber()) {
                number = value.asDouble();
            } else if (value != null && value.isBoolean()) {
                number = value.asBoolean() ? 1 : 0;
            } else {
                throw new OutputContractException("metrics[" + i + "].value는 숫자여야 합니다");
            }
            if (Double.isNaN(number) || Double.isInfinite(number)) {
                throw new OutputContractException("metrics[" + i + "].value는 유한한 숫자여야 합니다");
            }
            JsonNode unit = m.get("unit");
            String unitText = unit != null && unit.isString() ? unit.asString() : null;
            byKey.put(key.asString(), new Metric(key.asString(), number, unitText));
        }
        return List.copyOf(byKey.values());
    }

    private static Instant instant(JsonNode node, String field) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return Instant.ofEpochMilli(node.asLong());
        }
        if (!node.isString()) {
            throw new OutputContractException(field + "는 ISO-8601 문자열이어야 합니다");
        }
        try {
            return Instant.parse(node.asString());
        } catch (DateTimeParseException e) {
            try {
                return java.time.OffsetDateTime.parse(node.asString()).toInstant();
            } catch (DateTimeParseException e2) {
                throw new OutputContractException(field + "는 ISO-8601 형식이어야 합니다: " + node.asString());
            }
        }
    }

    private static CanonicalTelemetry.Link link(JsonNode node) {
        if (node == null || !node.isObject()) {
            return null;
        }
        List<CanonicalTelemetry.GatewayReception> gateways = null;
        JsonNode gw = node.get("gateways");
        if (gw != null && gw.isArray()) {
            gateways = new ArrayList<>();
            for (JsonNode g : gw) {
                if (g.hasNonNull("eui")) {
                    gateways.add(new CanonicalTelemetry.GatewayReception(g.get("eui").asString(),
                            number(g.get("rssi")), number(g.get("snr"))));
                }
            }
        }
        JsonNode fc = node.get("frameCounter");
        return new CanonicalTelemetry.Link(number(node.get("rssi")), number(node.get("snr")),
                fc != null && fc.isNumber() ? fc.asLong() : null, gateways);
    }

    private static Map<String, String> tags(JsonNode meta) {
        if (meta == null || !meta.isObject() || !meta.has("tags") || !meta.get("tags").isObject()) {
            return Map.of();
        }
        Map<String, String> tags = new LinkedHashMap<>();
        meta.get("tags").properties().forEach(e -> {
            if (e.getValue().isValueNode() && !e.getValue().isNull()) {
                tags.put(e.getKey(), e.getValue().asString());
            }
        });
        return tags;
    }

    private static Double number(JsonNode node) {
        return node != null && node.isNumber() ? node.asDouble() : null;
    }

    /** 측정값 하나(키, 값, 단위) */
    public record Metric(String key, double value, String unit) {
    }

    /** 반환값 계약 위반(SCRIPT_OUTPUT_INVALID) */
    public static final class OutputContractException extends RuntimeException {
        public OutputContractException(String message) {
            super(message, null, false, false);
        }
    }
}
