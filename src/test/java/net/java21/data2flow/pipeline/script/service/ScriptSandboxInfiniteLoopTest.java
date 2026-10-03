package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness.Attack;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-02.02 TC-SCR-027 · AT-SCR-02.2: 무한 루프·재귀·정규식 폭주·거대 배열(loop-*.js 5종) */
class ScriptSandboxInfiniteLoopTest {

    /** 50ms + 감시 여유. 해석 실행(JIT 없음)과 CI 부하를 감안해 벽시계 상한(200ms) + 여유로 판정 */
    private static final double MAX_MS = 400;

    static Stream<Attack> loopAttacks() {
        return ScriptSandboxHarness.attacks("loop-");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("loopAttacks")
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-027 시간 안에 SCRIPT_TIMEOUT 또는 SCRIPT_RUNTIME_ERROR, 다음 실행은 정상")
    void stopsInTimeAndNextRunIsHealthy(Attack attack) {
        ScriptOutcome outcome = ScriptSandboxHarness.transform(attack.code());

        assertThat(attack.expected()).contains(attack.resultOf(outcome));
        assertThat(outcome.durationMs()).isLessThan(MAX_MS);

        ScriptOutcome next = ScriptSandboxHarness.transform("function transform(msg, ctx) { return msg; }");
        assertThat(next.ok()).isTrue();
    }
}
