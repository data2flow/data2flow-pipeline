package net.java21.data2flow.pipeline.telemetry.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

/** API-ING-25 계산값 저장(analytics 가상 센서, ENE-02.02). ID는 문자열·숫자 모두 받는다 */
public final class DerivedPointDtos {

    /** 한 번에 받는 점 수 상한 */
    public static final int MAX_POINTS = 5000;

    private DerivedPointDtos() {
    }

    /**
     * @param orgId  조직 ID
     * @param source 출처(DERIVED만). 없으면 DERIVED
     * @param points 점 목록(1~{@value #MAX_POINTS})
     */
    public record Request(@NotNull @Positive Long orgId, String source,
                          @NotNull @Size(min = 1, max = MAX_POINTS) List<@Valid @NotNull Point> points) {
    }

    /**
     * @param deviceId  공간 가상 기기 ID(core-api가 가상 센서를 켤 때 만든다)
     * @param metricKey 출력 측정 키(예: occupancy_est)
     * @param ts        측정 시각
     * @param value     값
     * @param estimated 추정값 표시(기본 true)
     * @param meta      계산 방법·신뢰 구간 등(저장하지 않고 표준 메시지 meta.derived로 넘긴다)
     */
    public record Point(Long deviceId, String metricKey, Instant ts, Double value, Boolean estimated, JsonNode meta) {
    }

    public record Rejected(int index, String reason) {
    }

    /** @param accepted 저장한 점 수(이미 있던 같은 시각 값은 덮어쓴다) */
    public record Response(int accepted, List<Rejected> rejected) {
    }
}
