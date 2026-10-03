package net.java21.data2flow.pipeline.device.domain;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;

import java.time.Duration;

/**
 * 처리용 기기 정보(API-DEV-120 {@code GET /internal/core/sources/{source-id}/devices/{external-id}}, API-DEV-130 목록 항목).
 *
 * @param deviceId                 기기 ID
 * @param organizationId           조직 ID
 * @param sourceId                 데이터 소스 ID
 * @param externalId               외부 ID(소문자 정규화)
 * @param name                     이름
 * @param status                   PENDING·ACTIVE·INACTIVE(DELETED는 미등록과 같게 본다)
 * @param modelId                  기기 모델 ID. 없으면 null
 * @param modelCode                기기 모델 코드(예: MILESIGHT-EM300-TH). 없으면 null
 * @param spaceId                  공간 ID. 없으면 null
 * @param virtual                  가상 기기
 * @param ignored                  무시 목록에 있음(저장하지 않음)
 * @param expectedIntervalSec      기기 보고 주기. 없으면 null(모델 → 시스템 기본값, BR-DEV-08)
 * @param offlineMultiplier        기기 오프라인 배수. 없으면 null
 * @param modelIntervalSec         모델 기본 주기. 없으면 null
 * @param modelOfflineMultiplier   모델 기본 배수. 없으면 null
 * @param timezone                 사이트 시간대(1d 집계, BR-TSD-05). 없으면 null
 */
public record DeviceInfo(long deviceId, long organizationId, long sourceId, String externalId, String name, String status,
                         Long modelId, String modelCode, Long spaceId, boolean virtual, boolean ignored,
                         Integer expectedIntervalSec, Double offlineMultiplier, Integer modelIntervalSec,
                         Double modelOfflineMultiplier, String timezone) {

    public boolean deleted() {
        return "DELETED".equalsIgnoreCase(status);
    }

    public CanonicalTelemetry.DeviceStatus telemetryStatus() {
        if ("ACTIVE".equalsIgnoreCase(status)) {
            return CanonicalTelemetry.DeviceStatus.ACTIVE;
        }
        if ("INACTIVE".equalsIgnoreCase(status)) {
            return CanonicalTelemetry.DeviceStatus.INACTIVE;
        }
        return CanonicalTelemetry.DeviceStatus.PENDING;
    }

    /** 예상 보고 주기: 기기 값 → 모델 값 → 시스템 기본값(BR-DEV-08, DEV-02.08) */
    public int effectiveIntervalSec(int systemDefault) {
        if (expectedIntervalSec != null && expectedIntervalSec > 0) {
            return expectedIntervalSec;
        }
        if (modelIntervalSec != null && modelIntervalSec > 0) {
            return modelIntervalSec;
        }
        return systemDefault;
    }

    /** 오프라인 배수: 기기 값 → 모델 값 → 시스템 기본값(BR-DEV-08) */
    public double effectiveMultiplier(double systemDefault) {
        if (offlineMultiplier != null && offlineMultiplier > 0) {
            return offlineMultiplier;
        }
        if (modelOfflineMultiplier != null && modelOfflineMultiplier > 0) {
            return modelOfflineMultiplier;
        }
        return systemDefault;
    }

    /** 오프라인 판정 기준 시간 = 주기 × 배수 */
    public Duration offlineAfter(int systemInterval, double systemMultiplier) {
        return Duration.ofMillis(Math.round(effectiveIntervalSec(systemInterval) * effectiveMultiplier(systemMultiplier) * 1000));
    }
}
