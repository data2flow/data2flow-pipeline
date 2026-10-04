package net.java21.data2flow.pipeline.quality.domain;

import net.java21.data2flow.pipeline.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ING-06.04 기기 시계 오차 감지(BR-ING-18) */
class ClockSkewDetectorTest {

    private final ClockSkewDetector detector = new ClockSkewDetector(Duration.ofMinutes(5), Duration.ofHours(1),
            Duration.ofMinutes(30), Duration.ofMinutes(2));
    private final MutableClock clock = MutableClock.atUtc("2026-10-03T00:00:00Z");

    private List<ClockSkewDetector.Observation> feed(long device, int count, Duration skew) {
        List<ClockSkewDetector.Observation> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Instant received = clock.instant();
            out.add(detector.observe(device, received.plus(skew), received));
            clock.advance(Duration.ofMinutes(1));
        }
        return out;
    }

    @Test
    @DisplayName("[ING-06.04][AT-ING-09.3] TC-ING-076 1시간 평균 오차 +7분이 30분 이어지면 이벤트 1건(평균 +420초), 같은 상태로 계속되면 다시 내지 않는다")
    void suspectedOnceAfterSustain() {
        List<ClockSkewDetector.Observation> obs = feed(1, 60, Duration.ofMinutes(7));

        List<ClockSkewDetector.Observation> events = obs.stream().filter(o -> o.event() != null).toList();
        assertThat(events).hasSize(1);
        assertThat(events.getFirst().event().avgSkewSec()).isEqualTo(420.0);
        assertThat(events.getFirst().event().since()).isEqualTo(Instant.parse("2026-10-03T00:00:00Z"));
        assertThat(obs.indexOf(events.getFirst())).as("30분 지속 뒤(31번째 표본)").isEqualTo(30);
        assertThat(obs.getLast().suspected()).isTrue();
        assertThat(obs.getLast().avgSkewSec()).isEqualTo(420);
    }

    @Test
    @DisplayName("[ING-06.04] TC-ING-076 +4분 59초는 의심하지 않는다, 음수 오차(기기 시계가 늦음)도 크기로 본다")
    void belowThresholdAndNegative() {
        assertThat(feed(2, 60, Duration.ofMinutes(4).plusSeconds(59))).noneMatch(o -> o.event() != null || o.suspected());
        assertThat(feed(3, 40, Duration.ofMinutes(-10))).anyMatch(o -> o.event() != null);
    }

    @Test
    @DisplayName("[ING-06.04] TC-ING-076 평균 오차가 2분 이하로 돌아오면 해제되고, 다시 넘으면 다시 30분을 센다")
    void clearsWhenBack() {
        feed(4, 40, Duration.ofMinutes(7));
        List<ClockSkewDetector.Observation> back = feed(4, 70, Duration.ZERO);

        assertThat(back).anyMatch(ClockSkewDetector.Observation::cleared);
        assertThat(back.getLast().suspected()).isFalse();
        assertThat(back.getLast().since()).isNull();
        assertThat(feed(4, 120, Duration.ofMinutes(8))).filteredOn(o -> o.event() != null).hasSize(1);
        assertThat(detector.observe(4, null, clock.instant())).as("측정 시각이 없으면 반영 안 함").isNull();
        detector.reset();
    }

    @Test
    @DisplayName("[ING-06.04] 평균이 기준 아래로 잠시 내려가면(해제 기준 위) 지속 시간을 다시 센다")
    void sustainResetsBelowThreshold() {
        feed(5, 20, Duration.ofMinutes(7));
        // 1시간 창 평균을 기준 아래로: 0 오차 표본을 많이 넣는다
        List<ClockSkewDetector.Observation> mixed = feed(5, 40, Duration.ZERO);
        assertThat(mixed.getLast().since()).isNull();
        assertThat(mixed).noneMatch(o -> o.event() != null);
    }
}
