package net.java21.data2flow.pipeline.decoder.service;

import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * generic-json 매핑 설정 검증(ING-02.03 TC-ING-040): 잘못된 JSON 경로, 토픽 인덱스 범위, 측정 키 중복, 필수 항목 누락.
 * DSC 소스 저장 검증과 같은 규칙이다.
 */
public final class GenericJsonMappingValidator {

    private static final Pattern TOPIC_INDEX = Pattern.compile("topic\\[(\\d{1,2})]");
    private static final int MAX_TOPIC_LEVELS = 32;

    private GenericJsonMappingValidator() {
    }

    public static List<String> validate(JsonNode config) {
        List<String> errors = new ArrayList<>();
        if (config == null || !config.isObject()) {
            errors.add("매핑 설정이 없습니다");
            return errors;
        }
        JsonNode from = config.get("deviceIdFrom");
        if (from == null || !from.isString()) {
            errors.add("deviceIdFrom이 필요합니다");
        } else {
            var m = TOPIC_INDEX.matcher(from.asString());
            if (m.matches()) {
                if (Integer.parseInt(m.group(1)) >= MAX_TOPIC_LEVELS) {
                    errors.add("토픽 인덱스는 0~" + (MAX_TOPIC_LEVELS - 1) + "입니다: " + from.asString());
                }
            } else if (!JsonPaths.isValid(from.asString())) {
                errors.add("deviceIdFrom 경로가 잘못되었습니다: " + from.asString());
            }
        }
        String timePath = GenericJsonDecoder.timePath(config);
        if (timePath != null && !JsonPaths.isValid(timePath)) {
            errors.add("timePath 경로가 잘못되었습니다: " + timePath);
        }
        if (config.hasNonNull("timeFormat") && !GenericJsonDecoder.TIME_FORMATS.contains(config.get("timeFormat").asString())) {
            errors.add("timeFormat은 AUTO·EPOCH_S·EPOCH_MS·ISO8601 중 하나입니다: " + config.get("timeFormat").asString());
        }
        Set<String> keys = new HashSet<>();
        JsonNode metrics = config.get("metrics");
        boolean any = false;
        if (metrics != null && metrics.isArray()) {
            for (JsonNode m : metrics) {
                any = true;
                String path = m.path("path").asString(null);
                String key = m.path("key").asString(null);
                checkEntry(errors, keys, path, key);
            }
        } else if (metrics != null && metrics.isObject()) {
            for (Map.Entry<String, JsonNode> e : metrics.properties()) {
                any = true;
                checkEntry(errors, keys, e.getKey(), e.getValue().asString(null));
            }
        }
        JsonNode items = config.get("items");
        if (items != null && items.isObject()) {
            any = true;
            for (String field : List.of("path", "keyFrom", "valueFrom", "unitFrom")) {
                if (items.hasNonNull(field) && !JsonPaths.isValid(items.get(field).asString())) {
                    errors.add("items." + field + " 경로가 잘못되었습니다: " + items.get(field).asString());
                }
            }
            if (!items.hasNonNull("path")) {
                errors.add("items.path가 필요합니다");
            }
        }
        if (!any) {
            errors.add("metrics 또는 items가 필요합니다");
        }
        return errors;
    }

    private static void checkEntry(List<String> errors, Set<String> keys, String path, String key) {
        if (path == null || !JsonPaths.isValid(path)) {
            errors.add("측정 항목 경로가 잘못되었습니다: " + path);
        }
        if (key == null || key.isBlank()) {
            errors.add("측정 키가 비어 있습니다: " + path);
        } else if (!keys.add(key)) {
            errors.add("측정 키가 중복되었습니다: " + key);
        }
    }
}
