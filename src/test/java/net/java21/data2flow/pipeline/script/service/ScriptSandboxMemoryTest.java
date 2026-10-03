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

/**
 * SCR-02.02 TC-SCR-028 · AT-SCR-02.2: 메모리 공격(memory-*.js). 커뮤니티판에는 메모리 상한이 없으므로(ADR-008) 시간·문장 수·
 * 문자열 길이·타입 배열 길이 한도로 간접 차단하고, 호스트는 OOM 없이 계속 동작한다.
 */
class ScriptSandboxMemoryTest {

    static Stream<Attack> memoryAttacks() {
        return ScriptSandboxHarness.attacks("memory-");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("memoryAttacks")
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-028 메모리 공격은 한도로 멈추고 호스트 OOM이 없다")
    void memoryAttackIsStopped(Attack attack) {
        ScriptOutcome outcome = ScriptSandboxHarness.transform(attack.code());

        assertThat(attack.expected()).contains(attack.resultOf(outcome));
        assertThat(ScriptSandboxHarness.transform("function transform(msg, ctx) { return msg; }").ok()).isTrue();
    }

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-028 정상 범위의 배열(1MB 타입 배열, 1만 개 배열)은 성공한다")
    void moderateArraySucceeds() {
        ScriptOutcome outcome = ScriptSandboxHarness.transform("""
                function transform(msg, ctx) {
                  const a = new Uint8Array(1000000);
                  for (let i = 0; i < 1000; i++) a[i] = i & 0xff;
                  const b = new Array(10000).fill(1.5);
                  msg.meta = {n: a.length, m: b.length};
                  return msg;
                }
                """);

        assertThat(outcome.ok()).as(String.valueOf(outcome.failure())).isTrue();
        assertThat(outcome.output().get("meta").get("n").asInt()).isEqualTo(1_000_000);
    }
}
