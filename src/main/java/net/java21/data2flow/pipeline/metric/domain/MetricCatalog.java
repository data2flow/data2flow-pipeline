package net.java21.data2flow.pipeline.metric.domain;

import java.util.Map;

/**
 * 조직의 측정 항목·별칭 전체(API-DEV-123). 별칭은 한 단계만(별칭의 별칭 금지, BR-DEV-17).
 *
 * @param version 카탈로그 버전(sinceVersion 비교)
 * @param metrics 표준 키 → 정의
 * @param aliases 별칭 → 표준 키
 */
public record MetricCatalog(long version, Map<String, MetricDefinition> metrics, Map<String, String> aliases) {

    public static final MetricCatalog EMPTY = new MetricCatalog(-1, Map.of(), Map.of());

    public MetricCatalog {
        metrics = Map.copyOf(metrics);
        aliases = Map.copyOf(aliases);
    }

    /** 원본 키 → 표준 키(별칭이면 대상, 아니면 그대로) */
    public String canonicalKey(String rawKey) {
        String target = aliases.get(rawKey);
        return target == null ? rawKey : target;
    }

    public MetricDefinition definition(String key) {
        return metrics.get(key);
    }
}
