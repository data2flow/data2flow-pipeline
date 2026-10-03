package net.java21.data2flow.pipeline.device.service;

import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** DEV-02.08 TC-DEV-073 · BR-DEV-08: 오프라인 기준은 기기 값 > 모델 값 > 시스템 기본값(300초, 3배) */
class OfflineThresholdInheritanceTest {

    @ParameterizedTest(name = "기기 {0}/{1}, 모델 {2}/{3} → {4}초 × {5}")
    @CsvSource(nullValues = "-", value = {
            "60,2.0,120,4.0,60,2.0",
            "-,-,120,4.0,120,4.0",
            "-,-,-,-,300,3.0",
            "60,-,120,4.0,60,4.0",
            "-,1.5,600,-,600,1.5"})
    @DisplayName("[DEV-02.08] TC-DEV-073 상속 순서")
    void inheritance(Integer interval, Double multiplier, Integer modelInterval, Double modelMultiplier,
                     int expectedInterval, double expectedMultiplier) {
        DeviceInfo device = FakeCoreDirectory.device(1, 3, "x", interval, multiplier, modelInterval, modelMultiplier);

        assertThat(device.effectiveIntervalSec(300)).isEqualTo(expectedInterval);
        assertThat(device.effectiveMultiplier(3)).isEqualTo(expectedMultiplier);
        assertThat(device.offlineAfter(300, 3)).isEqualTo(Duration.ofMillis(Math.round(expectedInterval * expectedMultiplier * 1000)));
    }
}
