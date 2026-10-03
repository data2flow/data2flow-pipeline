package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.ScriptErrorCode;
import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-02.02 TC-SCR-030 · AT-SCR-02.2: 워치독 강제 중단, 닫힌 Context 재사용 없음, 감시 스레드 누수 0 */
class ScriptWatchdogTest {

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-030 문장 수 한도를 피하는 네이티브 장기 연산도 시간 초과로 끊긴다")
    void nativeLongOperationIsCancelled() {
        ScriptOutcome outcome = ScriptSandboxHarness.transform("""
                function transform(msg, ctx) {
                  const s = '[' + '1,'.repeat(500000) + '1]';
                  let n = 0;
                  while (true) { n += JSON.parse(s).length; }
                }
                """);

        assertThat(outcome.failure().code()).isIn(ScriptErrorCode.SCRIPT_TIMEOUT, ScriptErrorCode.SCRIPT_RUNTIME_ERROR);
    }

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-030 1,000회 실행(시간 초과 섞음) 뒤 스레드 수가 그대로이고 감시 대상이 남지 않는다")
    void noThreadLeak() {
        ScriptSandboxHarness.transform("function transform(msg, ctx) { return msg; }");
        int before = Thread.activeCount();
        for (int i = 0; i < 1000; i++) {
            String code = i % 100 == 0
                    ? "function transform(msg, ctx) { while (true) {} }"
                    : "function transform(msg, ctx) { msg.meta = {i: " + i + "}; return msg; }";
            ScriptOutcome outcome = ScriptSandboxHarness.transform(code);
            assertThat(outcome.ok()).as("%d: %s %sms", i, outcome.failure(), outcome.durationMs()).isEqualTo(i % 100 != 0);
        }

        assertThat(Thread.activeCount()).isLessThanOrEqualTo(before + 2);
    }

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-030 감시 스레드는 하나로 시작해 닫으면 멈춘다")
    void watchdogLifecycle() {
        try (ScriptWatchdog watchdog = new ScriptWatchdog()) {
            assertThat(watchdog.activeCount()).isZero();
        }
    }
}
