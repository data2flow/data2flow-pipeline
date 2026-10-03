package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-02.01 TC-SCR-025 · AT-SCR-04.4: 실행마다 새 Context(공유 Engine) */
class ScriptContextIsolationTest {

    @Test
    @DisplayName("[SCR-02.01][AT-SCR-04.4] TC-SCR-025 전역 변수 counter++가 실행 사이에 누적되지 않는다")
    void globalsDoNotAccumulate() {
        String code = """
                var counter = globalThis.counter || 0;
                function transform(msg, ctx) { counter++; globalThis.counter = counter; msg.meta = {counter: counter}; return msg; }
                """;
        for (int i = 0; i < 5; i++) {
            ScriptOutcome outcome = ScriptSandboxHarness.transform(code);
            assertThat(outcome.output().get("meta").get("counter").asInt()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("[SCR-02.01][AT-SCR-04.4] TC-SCR-025 조직 A 스크립트는 조직 B의 ctx.config를 볼 수 없다(입력으로 받은 것만 보인다)")
    void configIsPerExecution() {
        String code = "function transform(msg, ctx) { msg.meta = {secret: ctx.config.offset, keys: Object.keys(ctx.config)}; return msg; }";
        ScriptOutcome orgB = ScriptSandboxHarness.transform(code, ScriptSandboxHarness.NORMAL_INPUT,
                "{\"config\":{\"offset\":42}}");
        ScriptOutcome orgA = ScriptSandboxHarness.transform(code, ScriptSandboxHarness.NORMAL_INPUT,
                "{\"config\":{}}");

        assertThat(orgB.ok()).as(String.valueOf(orgB.failure())).isTrue();
        assertThat(orgB.output().get("meta").get("secret").asInt()).isEqualTo(42);
        assertThat(orgA.output().get("meta").has("secret")).isFalse();
        assertThat(orgA.output().get("meta").get("keys")).isEmpty();
    }
}
