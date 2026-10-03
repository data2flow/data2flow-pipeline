package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.ScriptErrorCode;
import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness.Attack;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-02.02 TC-SCR-029 · AT-SCR-02.2: 출력 64KB 경계, 거대·순환·깊은 출력, 거대 로그(output-*.js) */
class ScriptSandboxHugeOutputTest {

    static Stream<Attack> outputAttacks() {
        return ScriptSandboxHarness.attacks("output-");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("outputAttacks")
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-029 거대·순환·깊은 출력은 차단되고 호스트 직렬화 중 StackOverflow가 없다")
    void hugeOutputIsBlocked(Attack attack) {
        ScriptOutcome outcome = ScriptSandboxHarness.transform(attack.code());

        assertThat(attack.expected()).contains(attack.resultOf(outcome));
        if (outcome.ok()) {
            int total = outcome.logs().stream().mapToInt(String::length).sum();
            assertThat(total).as("로그는 1KB × 최대 건수로 잘린다").isLessThanOrEqualTo(110 * 1100);
        }
    }

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-029 반환값 65,536바이트는 성공, 65,537바이트는 SCRIPT_OUTPUT_INVALID")
    void outputBoundary() {
        // {"s":"…"} = 8바이트 + 문자열 길이
        ScriptOutcome exact = ScriptSandboxHarness.transform(
                "function transform(msg, ctx) { return {s: 'x'.repeat(65536 - 8)}; }");
        ScriptOutcome over = ScriptSandboxHarness.transform(
                "function transform(msg, ctx) { return {s: 'x'.repeat(65536 - 7)}; }");

        assertThat(exact.ok()).as(String.valueOf(exact.failure())).isTrue();
        assertThat(exact.outputBytes()).isEqualTo(65_536);
        assertThat(over.failure().code()).isEqualTo(ScriptErrorCode.SCRIPT_OUTPUT_INVALID);
    }
}
