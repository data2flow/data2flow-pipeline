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

/** SCR-02.01 TC-SCR-024 · AT-SCR-04.4: 프로토타입 오염은 그 실행 안에만 머물고 다음 실행(새 Context)에 보이지 않는다 */
class ScriptSandboxPrototypePollutionTest {

    private static final String PROBE = """
            function transform(msg, ctx) {
              const a = [];
              a.push(1);
              msg.meta = {
                polluted: ({}).polluted === undefined,
                push: a.length === 1,
                json: JSON.parse('{"x":1}').x === 1,
                proto: ({}).x === undefined,
                toJSON: typeof ({}).toJSON === 'undefined',
                from: Array.from([1, 2]).length === 2
              };
              return msg;
            }
            """;

    static Stream<Attack> pollutionAttacks() {
        return ScriptSandboxHarness.attacks("pollution-");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pollutionAttacks")
    @DisplayName("[SCR-02.01][AT-SCR-04.4] TC-SCR-024 오염 실행 뒤 다음 실행은 깨끗하고, 출력 직렬화는 호스트가 해서 toJSON 영향이 없다")
    void pollutionDoesNotLeak(Attack attack) {
        ScriptOutcome polluted = ScriptSandboxHarness.transform(attack.code());
        assertThat(attack.expected()).contains(attack.resultOf(polluted));
        if (polluted.ok()) {
            assertThat(polluted.output().get("metrics").get(0).get("key").asString()).isEqualTo("temperature");
        }

        ScriptOutcome next = ScriptSandboxHarness.transform(PROBE);

        assertThat(next.ok()).as(String.valueOf(next.failure())).isTrue();
        next.output().get("meta").properties().forEach(e ->
                assertThat(e.getValue().asBoolean()).as(e.getKey()).isTrue());
    }

    @Test
    @DisplayName("[SCR-02.01][AT-SCR-04.4] TC-SCR-024 오염 코퍼스는 6종 이상")
    void corpusSize() {
        assertThat(pollutionAttacks()).hasSizeGreaterThanOrEqualTo(6);
    }
}
