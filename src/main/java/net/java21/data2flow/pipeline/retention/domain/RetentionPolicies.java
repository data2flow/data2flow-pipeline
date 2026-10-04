package net.java21.data2flow.pipeline.retention.domain;

import java.util.List;
import java.util.Map;

/**
 * 조직의 보관 정책(TSD-02.01·05.01·05.03, OPS-04, NFR-04.03). 정본은 core {@code retention_policies}이고 우선순위는
 * METRIC > MODEL > ORG다. 정책이 없는 종류는 시스템 기본값(TSD domain-model §2.7)을 쓴다.
 *
 * @param organizationId 조직
 * @param version        정책 판(API-TSD-53 통지)
 * @param items          정책 항목
 */
public record RetentionPolicies(long organizationId, long version, List<Item> items) {

    public RetentionPolicies {
        items = items == null ? List.of() : List.copyOf(items);
    }

    /**
     * @param scope               ORG, MODEL, METRIC
     * @param scopeRef            모델 ID·코드 또는 측정 키(ORG면 null)
     * @param dataClass           RAW_MESSAGE, TELEMETRY, LINK, AGG_1M, AGG_1H, AGG_1D …
     * @param retainDays          보관 일수(0 = 무기한, AGG_1D만)
     * @param compressAfterDays   정렬 재작성(TELEMETRY만)
     * @param archiveBeforeDelete 삭제 전 Parquet 콜드 보관
     * @param storeMode           ALL, ON_CHANGE(METRIC 범위만, TSD-05.03)
     */
    public record Item(String scope, String scopeRef, String dataClass, int retainDays, Integer compressAfterDays,
                       boolean archiveBeforeDelete, String storeMode) {
    }

    /** ORG 범위 보관 일수. 없으면 기본값 */
    public int orgDays(String dataClass, int defaultDays) {
        return items.stream().filter(i -> "ORG".equalsIgnoreCase(i.scope()) && dataClass.equalsIgnoreCase(i.dataClass()))
                .map(Item::retainDays).findFirst().orElse(defaultDays);
    }

    public boolean archive(String dataClass) {
        return items.stream().anyMatch(i -> "ORG".equalsIgnoreCase(i.scope()) && dataClass.equalsIgnoreCase(i.dataClass())
                && i.archiveBeforeDelete());
    }

    /** METRIC·MODEL 범위 재정의(범위 → 일수) */
    public List<Item> overrides(String dataClass) {
        return items.stream().filter(i -> !"ORG".equalsIgnoreCase(i.scope()) && dataClass.equalsIgnoreCase(i.dataClass())
                && i.scopeRef() != null).toList();
    }

    /** 측정 항목의 저장 방식(TSD-05.03). 없으면 ALL */
    public String storeMode(String metricKey) {
        return items.stream().filter(i -> "METRIC".equalsIgnoreCase(i.scope()) && metricKey.equals(i.scopeRef())
                && i.storeMode() != null).map(Item::storeMode).findFirst().orElse("ALL");
    }

    /** 데이터 종류 → 기본 보관 일수(NFR-04.03, TSD domain-model §2.7) */
    public static Map<String, Integer> defaults(int raw, int telemetry, int link, int agg1m, int agg1h, int agg1d) {
        return Map.of("RAW_MESSAGE", raw, "TELEMETRY", telemetry, "LINK", link, "AGG_1M", agg1m, "AGG_1H", agg1h,
                "AGG_1D", agg1d);
    }
}
