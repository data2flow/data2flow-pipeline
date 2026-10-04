package net.java21.data2flow.pipeline.retention.service;

import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.service.CoreDirectory;
import net.java21.data2flow.pipeline.retention.domain.RetentionPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 조직별 보관 정책 캐시(TSD-02.01·05.01·05.03). core 정책(API-TSD-60)을 5분마다·정책 변경 통지(API-TSD-53) 때 다시 읽는다. core가
 * 응답하지 않으면 마지막으로 읽은 값을 쓰고, 한 번도 못 읽었으면 시스템 기본값(NFR-04.03)을 쓴다.
 */
public class RetentionPolicyCache {

    private static final Logger log = LoggerFactory.getLogger(RetentionPolicyCache.class);

    private final CoreDirectory core;
    private final PipelineProperties.Retention defaults;
    private final Clock clock;
    private final Map<Long, RetentionPolicies> byOrg = new ConcurrentHashMap<>();
    private volatile Instant loadedAt;

    public RetentionPolicyCache(CoreDirectory core, PipelineProperties.Retention defaults, Clock clock) {
        this.core = core;
        this.defaults = defaults;
        this.clock = clock;
    }

    /** core에서 다시 읽는다. 읽었으면 true */
    public boolean refresh() {
        try {
            Optional<List<RetentionPolicies>> fresh = core.retentionPolicies();
            loadedAt = clock.instant();
            if (fresh.isEmpty()) {
                return false;
            }
            byOrg.clear();
            fresh.get().forEach(p -> byOrg.put(p.organizationId(), p));
            return true;
        } catch (RuntimeException e) {
            log.warn("보관 정책을 읽지 못했습니다(마지막 값 유지): {}", e.getMessage());
            return false;
        }
    }

    /** 주기 갱신이 필요하면 읽는다 */
    public void refreshIfStale() {
        Instant at = loadedAt;
        if (at == null || at.plus(defaults.policyRefresh()).isBefore(clock.instant())) {
            refresh();
        }
    }

    public RetentionPolicies of(long organizationId) {
        RetentionPolicies p = byOrg.get(organizationId);
        return p != null ? p : new RetentionPolicies(organizationId, 0, List.of());
    }

    /** 데이터 종류의 시스템 기본 보관 일수 */
    public int defaultDays(String dataClass) {
        return switch (dataClass) {
            case "RAW_MESSAGE" -> defaults.rawMessageDays();
            case "TELEMETRY" -> defaults.telemetryDays();
            case "LINK" -> defaults.linkDays();
            case "AGG_1M" -> defaults.agg1mDays();
            case "AGG_1H" -> defaults.agg1hDays();
            case "AGG_1D" -> defaults.agg1dDays();
            default -> 0;
        };
    }

    /** 조직의 보관 일수(ORG 범위, 없으면 기본). 0은 무기한 */
    public int days(long organizationId, String dataClass) {
        return of(organizationId).orgDays(dataClass, defaultDays(dataClass));
    }

    /** 모든 조직과 기본값 중 가장 긴 보관 일수(공용 파티션을 통째로 지워도 되는 기준). 무기한이 있으면 0 */
    public int maxDays(String dataClass) {
        int max = defaultDays(dataClass);
        if (max == 0) {
            return 0;
        }
        for (RetentionPolicies p : byOrg.values()) {
            int d = p.orgDays(dataClass, defaultDays(dataClass));
            if (d == 0) {
                return 0;
            }
            max = Math.max(max, d);
        }
        return max;
    }

    /** 측정 항목 저장 방식(TSD-05.03): ALL 또는 ON_CHANGE */
    public String storeMode(long organizationId, String metricKey) {
        return of(organizationId).storeMode(metricKey);
    }

    public java.util.Collection<RetentionPolicies> known() {
        return byOrg.values();
    }
}
