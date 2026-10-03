package net.java21.data2flow.pipeline.ingest.domain;

import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ING 업무 규칙 단위 테스트(순수 JUnit) */
class IngestRulesTest {

    private static final Instant RECEIVED = Instant.parse("2026-07-01T00:00:00Z");

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = RawMessageStatus.class, names = "RECEIVED", mode = EnumSource.Mode.EXCLUDE)
    @DisplayName("[ING-01.02][AT-ING-01.1] TC-ING-011 RECEIVED에서 모든 결과로 전이, 종결 상태에서 OK로 직접 전이는 없음")
    void statusTransitions(RawMessageStatus status) {
        assertThat(RawMessageStatus.RECEIVED.canTransitionTo(status)).isTrue();
        assertThat(status.canTransitionTo(RawMessageStatus.OK)).isFalse();
        assertThat(RawMessageStatus.RECEIVED.canTransitionTo(RawMessageStatus.RECEIVED)).isFalse();
    }

    @Test
    @DisplayName("[ING-01.02][AT-ING-01.4] TC-ING-011 재처리는 실패 상태에서만, DLQ 단계는 DECODE·SCRIPT·STORE·PUBLISH")
    void reprocessing() {
        assertThat(RawMessageStatus.DECODE_ERROR.canReprocessTo(RawMessageStatus.OK)).isTrue();
        assertThat(RawMessageStatus.INVALID.reprocessable()).isFalse();
        assertThat(RawMessageStatus.OK.canReprocessTo(RawMessageStatus.OK)).isFalse();
        assertThat(RawMessageStatus.DECODE_ERROR.dlqStage()).isEqualTo(DlqStage.DECODE);
        assertThat(RawMessageStatus.SCRIPT_ERROR.dlqStage()).isEqualTo(DlqStage.SCRIPT);
        assertThat(RawMessageStatus.STORE_ERROR.dlqStage()).isEqualTo(DlqStage.STORE);
        assertThat(RawMessageStatus.PUBLISH_ERROR.dlqStage()).isEqualTo(DlqStage.PUBLISH);
        assertThat(RawMessageStatus.INVALID.dlqStage()).isNull();
        assertThat(RawMessageStatus.DUPLICATE.dlqStage()).isNull();
    }

    @ParameterizedTest(name = "측정 = 수신 {0}초 → 보정 {1}")
    @CsvSource({"300,false", "301,true", "600,true", "-604800,false", "-604801,true", "-601200,false"})
    @DisplayName("[ING-02.05][AT-ING-02.4] TC-ING-043 미래 +5분 초과·과거 7일 초과면 수신 시각 + 품질 4, 경계는 그대로")
    void measuredAtBoundaries(long offsetSeconds, boolean corrected) {
        MeasuredAtNormalizer normalizer = new MeasuredAtNormalizer(Duration.ofMinutes(5), Duration.ofDays(7));

        MeasuredAtNormalizer.Result r = normalizer.normalize(RECEIVED.plusSeconds(offsetSeconds), RECEIVED, false);

        assertThat(r.corrected()).isEqualTo(corrected);
        assertThat(r.measuredAt()).isEqualTo(corrected ? RECEIVED : RECEIVED.plusSeconds(offsetSeconds));
    }

    @Test
    @DisplayName("[ING-02.05][AT-ING-02.4] TC-ING-043 측정 시각이 없으면 수신 시각 + 품질 4(MISSING)")
    void missingMeasuredAt() {
        MeasuredAtNormalizer.Result r = new MeasuredAtNormalizer(Duration.ofMinutes(5), Duration.ofDays(7))
                .normalize(null, RECEIVED, false);

        assertThat(r.measuredAt()).isEqualTo(RECEIVED);
        assertThat(r.corrected()).isTrue();
        assertThat(r.reason()).isEqualTo("MISSING");
    }

    @Test
    @DisplayName("[ING-02.05][AT-ING-02.10] TC-ING-098 가상(simRunId)·엣지 배치·가져오기는 +3시간·-6일이어도 그대로")
    void keepOriginal() {
        MeasuredAtNormalizer normalizer = new MeasuredAtNormalizer(Duration.ofMinutes(5), Duration.ofDays(7));

        assertThat(normalizer.normalize(RECEIVED.plus(Duration.ofHours(3)), RECEIVED, true).corrected()).isFalse();
        assertThat(normalizer.normalize(RECEIVED.minus(Duration.ofDays(6)), RECEIVED, true).measuredAt())
                .isEqualTo(RECEIVED.minus(Duration.ofDays(6)));
        assertThat(normalizer.normalize(RECEIVED.plus(Duration.ofHours(3)), RECEIVED, false).corrected()).isTrue();
    }

    @ParameterizedTest(name = "{0}초 과거 → late {1}")
    @CsvSource({"3599,false", "3600,true", "7200,true", "0,false"})
    @DisplayName("[ING-06.03][AT-ING-04.4] TC-ING-074 측정 시각이 수신 시각보다 1시간 이상 과거면 late")
    void late(long secondsBefore, boolean late) {
        assertThat(new LateArrivalClassifier(Duration.ofHours(1)).isLate(RECEIVED.minusSeconds(secondsBefore), RECEIVED))
                .isEqualTo(late);
    }

    @ParameterizedTest(name = "시각보정={0} 범위밖={1} 의심={2} 미검증={3} 예보={4} → {5}")
    @CsvSource({
            "false,false,false,false,false,0",
            "false,true,false,false,false,1",
            "false,false,false,true,false,2",
            "false,false,true,false,false,3",
            "true,false,false,false,false,4",
            "false,false,false,false,true,5",
            "true,true,true,true,false,4",
            "false,true,true,true,false,1",
            "false,false,true,true,false,3"})
    @DisplayName("[ING-04.01][AT-ING-04.1] TC-ING-054 품질 코드 0~5와 우선순위 4 > 1 > 3 > 2")
    void quality(boolean corrected, boolean outOfRange, boolean suspect, boolean unverified, boolean forecast, int expected) {
        assertThat(QualityAssigner.assign(corrected, outOfRange, suspect, unverified, forecast)).isEqualTo(expected);
    }

    @Test
    @DisplayName("[ING-07.01][AT-ING-10.1] TC-ING-080 측정 항목 100개 통과, 101개 ING_LIMIT_METRICS_EXCEEDED(101/100)")
    void metricLimit() {
        MessageLimitValidator validator = new MessageLimitValidator(100, 64, 1024);
        List<DecodedValue> hundred = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            hundred.add(DecodedValue.of("k" + i, i));
        }
        assertThat(validator.check(hundred)).isEmpty();
        hundred.add(DecodedValue.of("k100", 1));
        assertThat(validator.check(hundred)).hasValueSatisfying(v -> {
            assertThat(v.code()).isEqualTo("ING_LIMIT_METRICS_EXCEEDED");
            assertThat(v.message()).isEqualTo("101/100");
        });
    }

    @Test
    @DisplayName("[ING-07.01][AT-ING-10.2] TC-ING-080 키 64자 통과·65자 ING_LIMIT_KEY_TOO_LONG(앞 20자), 문자열 1,024바이트 통과·1,025바이트(한글) 거부")
    void keyAndStringLimits() {
        MessageLimitValidator validator = new MessageLimitValidator(100, 64, 1024);
        assertThat(validator.check(List.of(DecodedValue.of("a".repeat(64), 1)))).isEmpty();
        assertThat(validator.check(List.of(DecodedValue.of("a".repeat(65), 1)))).hasValueSatisfying(v -> {
            assertThat(v.code()).isEqualTo("ING_LIMIT_KEY_TOO_LONG");
            assertThat(v.key()).hasSize(20);
        });
        assertThat(validator.check(List.of(new DecodedValue("s", "x".repeat(1024), null)))).isEmpty();
        String korean = "가".repeat(341) + "xx"; // 3 × 341 + 2 = 1,025바이트
        assertThat(validator.check(List.of(new DecodedValue("s", korean, null)))).hasValueSatisfying(v ->
                assertThat(v.code()).isEqualTo("ING_LIMIT_STRING_TOO_LONG"));
    }
}
