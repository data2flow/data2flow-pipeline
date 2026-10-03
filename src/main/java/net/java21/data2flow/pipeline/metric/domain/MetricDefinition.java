package net.java21.data2flow.pipeline.metric.domain;

import java.util.Map;

/**
 * 측정 항목 정의(API-DEV-123 {@code metrics[]}, DEV domain-model §2.8).
 *
 * @param id        ID
 * @param key       표준 키
 * @param unit      단위
 * @param valueType NUMBER, BOOLEAN, ENUM
 * @param validMin  유효 범위 하한(품질 1 판정). 없으면 null
 * @param validMax  유효 범위 상한
 * @param status    VERIFIED, UNVERIFIED, IGNORED
 * @param enumMap   글자 값 → 숫자(ING-02.06, 대소문자 무시). 없으면 빈 맵
 * @param stateType 상태형(door 등, 집계 state_on_sec·state_changes)
 */
public record MetricDefinition(long id, String key, String unit, String valueType, Double validMin, Double validMax,
                               String status, Map<String, Double> enumMap, boolean stateType) {

    public MetricDefinition {
        enumMap = enumMap == null ? Map.of() : Map.copyOf(enumMap);
    }

    public boolean unverified() {
        return "UNVERIFIED".equalsIgnoreCase(status);
    }

    public boolean ignored() {
        return "IGNORED".equalsIgnoreCase(status);
    }

    public boolean outOfRange(double value) {
        return (validMin != null && value < validMin) || (validMax != null && value > validMax);
    }
}
