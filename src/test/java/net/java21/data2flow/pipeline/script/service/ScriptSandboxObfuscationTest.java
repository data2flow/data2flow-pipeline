package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness.Attack;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-02.01 TC-SCR-023 · AT-SCR-04.4: 정적 검사를 피하는 문자열 조합·생성자 체인도 런타임에서 막힌다(bypass-*.js 8종) */
class ScriptSandboxObfuscationTest {

    static Stream<Attack> bypassAttacks() {
        return ScriptSandboxHarness.attacks("bypass-");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("bypassAttacks")
    @DisplayName("[SCR-02.01][AT-SCR-04.4] TC-SCR-023 우회 시도는 SCRIPT_FORBIDDEN_API 또는 ReferenceError, 호스트 객체 획득 실패")
    void bypassIsBlocked(Attack attack) {
        ScriptOutcome outcome = ScriptSandboxHarness.transform(attack.code());

        assertThat(outcome.ok()).as("호스트·전역 객체를 돌려받지 못한다").isFalse();
        assertThat(attack.expected()).contains(attack.resultOf(outcome));
    }

    @Test
    @DisplayName("[SCR-02.01][AT-SCR-04.4] TC-SCR-023 우회 코퍼스는 8종")
    void corpusSize() {
        assertThat(bypassAttacks()).hasSize(8);
    }
}
