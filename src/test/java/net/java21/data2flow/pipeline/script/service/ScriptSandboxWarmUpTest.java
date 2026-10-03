package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SCR-02.02 TC-SCR-030 · AT-SCR-02.2: 시작 예열은 횟수를 고정하지 않고 대표 스크립트의 감시 구간 CPU 시간이 목표 아래로 이어질
 * 때까지 하며(정상 스크립트가 덜 데워진 엔진에서 50ms 한도를 넘어 시간 초과로 오판되지 않게), 최대 횟수·시간에서 멈춘다.
 */
class ScriptSandboxWarmUpTest {

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-030 예열 뒤 대표 스크립트 CPU가 목표 안이면 최소 횟수+연속 횟수에서 끝난다")
    void stopsWhenStable() {
        ScriptSandbox.WarmUpResult result = ScriptSandboxHarness.sandbox().warmUp(
                new ScriptSandbox.WarmUpPolicy(2, 50, 2, Duration.ofSeconds(10), Duration.ofSeconds(60)));

        assertThat(result.reachedTarget()).isTrue();
        assertThat(result.rounds()).isEqualTo(2);
        assertThat(result.lastCpuMs()).isPositive();
    }

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-030 목표에 못 미치면 최대 횟수에서 멈추고 미도달로 알린다(한도는 그대로)")
    void stopsAtMaxRounds() {
        ScriptSandbox sandbox = ScriptSandboxHarness.sandbox();
        ScriptSandbox.WarmUpResult result = sandbox.warmUp(
                new ScriptSandbox.WarmUpPolicy(1, 2, 3, Duration.ZERO, Duration.ofSeconds(60)));

        assertThat(result.reachedTarget()).isFalse();
        assertThat(result.rounds()).isEqualTo(2);
        assertThat(sandbox.limits().cpuTime()).isEqualTo(Duration.ofMillis(50));
    }

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-030 예열 시간 상한을 넘으면 최소 횟수 전이라도 멈춘다(시작 지연 상한)")
    void stopsAtMaxTime() {
        ScriptSandbox.WarmUpResult result = ScriptSandboxHarness.sandbox().warmUp(
                new ScriptSandbox.WarmUpPolicy(100, 100, 3, Duration.ZERO, Duration.ZERO));

        assertThat(result.reachedTarget()).isFalse();
        assertThat(result.rounds()).isEqualTo(1);
    }
}
