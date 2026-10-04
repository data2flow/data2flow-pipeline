package net.java21.data2flow.pipeline.quality.domain;

import net.java21.data2flow.contracts.message.Quality;
import net.java21.data2flow.pipeline.ingest.domain.QualityAssigner;
import net.java21.data2flow.pipeline.metric.domain.MetricDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** ING-04.01 품질 3 "의심(값 멈춤·급변)"(TC-ING-054) */
class SuspectDetectorTest {

    private static final MetricDefinition TEMP = new MetricDefinition(1, "temperature", "℃", "NUMBER", -20.0, 60.0,
            "VERIFIED", Map.of(), false);
    private static final MetricDefinition DOOR = new MetricDefinition(2, "door", null, "ENUM", null, null, "VERIFIED",
            Map.of("open", 1.0), true);
    private static final Instant T0 = Instant.parse("2026-10-03T00:00:00Z");

    private final SuspectDetector detector = new SuspectDetector(12, 0.1875);

    @Test
    @DisplayName("[ING-04.01][AT-ING-04.1] TC-ING-054 같은 값 12회 → 12번째부터 의심, 값이 바뀌면 다시 센다")
    void stuck() {
        for (int i = 1; i <= 11; i++) {
            assertThat(detector.observe(1, "temperature", TEMP, 22.5, T0.plus(Duration.ofMinutes(i)), false)).as("%d회", i)
                    .isFalse();
        }
        assertThat(detector.observe(1, "temperature", TEMP, 22.5, T0.plus(Duration.ofMinutes(12)), false)).isTrue();
        assertThat(detector.observe(1, "temperature", TEMP, 22.5, T0.plus(Duration.ofMinutes(13)), false)).isTrue();
        assertThat(detector.observe(1, "temperature", TEMP, 22.6, T0.plus(Duration.ofMinutes(14)), false)).isFalse();
    }

    @Test
    @DisplayName("[ING-04.01] TC-ING-054 급변: -20~60℃에서 1분에 +15.1 → 의심, +15 → 아님, 30초 간격도 1분으로 본다")
    void jump() {
        detector.observe(2, "temperature", TEMP, 20, T0, false);
        assertThat(detector.observe(2, "temperature", TEMP, 35, T0.plus(Duration.ofMinutes(1)), false)).isFalse();
        assertThat(detector.observe(2, "temperature", TEMP, 50.1, T0.plus(Duration.ofMinutes(2)), false)).isTrue();
        assertThat(detector.observe(2, "temperature", TEMP, 50.1 - 14, T0.plus(Duration.ofMinutes(2)).plusSeconds(30), false))
                .isFalse();
        assertThat(detector.observe(2, "temperature", TEMP, 36.0 + 40, T0.plus(Duration.ofMinutes(10)), false))
                .as("7.5분에 39.9 → 5.3/분").isFalse();
    }

    @Test
    @DisplayName("[ING-04.01] 상태형·범위 없는 항목·배터리는 멈춤을 보지 않고, 늦은 값·같은 시각 재전송은 판정하지 않는다")
    void exemptions() {
        for (int i = 0; i < 20; i++) {
            assertThat(detector.observe(3, "door", DOOR, 1, T0.plus(Duration.ofMinutes(i)), false)).isFalse();
            assertThat(detector.observe(3, "battery", TEMP, 100, T0.plus(Duration.ofMinutes(i)), false)).isFalse();
        }
        assertThat(detector.observe(3, "temperature", TEMP, 99, T0, true)).isFalse();
        detector.observe(4, "temperature", TEMP, 20, T0, false);
        assertThat(detector.observe(4, "temperature", TEMP, 59, T0, false)).as("같은 시각").isFalse();
        detector.reset();
    }

    @ParameterizedTest(name = "[ING-04.01] TC-ING-054 우선순위 4 > 1 > 3 > 2: corrected={0} oor={1} suspect={2} unverified={3} → {4}")
    @CsvSource({"false,false,true,false,3", "false,true,true,false,1", "true,false,true,true,4", "false,false,true,true,3",
            "false,false,false,true,2"})
    void priority(boolean corrected, boolean oor, boolean suspect, boolean unverified, int expected) {
        assertThat(QualityAssigner.assign(corrected, oor, suspect, unverified, false)).isEqualTo(expected);
        assertThat(QualityAssigner.assign(corrected, oor, suspect, unverified, true)).isEqualTo(Quality.FORECAST);
    }
}
