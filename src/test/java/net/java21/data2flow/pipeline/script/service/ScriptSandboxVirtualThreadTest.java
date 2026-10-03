package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.ScriptErrorCode;
import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SCR-02.02 TC-SCR-030 · AT-SCR-02.2: 가상 스레드(테스트 실행 API 요청 스레드)에서 불러도 CPU 시간으로 한도를 잰다.
 * 가상 스레드에서 직접 돌리면 워치독이 벽시계 50ms로 재서 부하·GC 정지만으로 정상 스크립트가 시간 초과가 된다.
 */
class ScriptSandboxVirtualThreadTest {

    private static ScriptOutcome onVirtualThread(String code) throws InterruptedException {
        AtomicReference<ScriptOutcome> result = new AtomicReference<>();
        Thread.ofVirtual().start(() -> result.set(ScriptSandboxHarness.transform(code))).join();
        return result.get();
    }

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-030 가상 스레드에서 부른 무한 루프도 CPU 시간 기준으로 끊긴다")
    void infiniteLoopIsMeasuredByCpuTime() throws InterruptedException {
        ScriptOutcome outcome = onVirtualThread("function transform(msg, ctx) { let n = 0; while (true) { n = (n + 1) | 0; } }");

        assertThat(outcome.failure().code()).isEqualTo(ScriptErrorCode.SCRIPT_TIMEOUT);
        assertThat(outcome.failure().message()).doesNotContain("cpu 측정 불가");
    }

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-030 가상 스레드에서 부른 정상 스크립트는 결과가 같다")
    void normalScriptWorks() throws InterruptedException {
        ScriptOutcome outcome = onVirtualThread("function transform(msg, ctx) { msg.meta = {v: 1}; return msg; }");

        assertThat(outcome.ok()).as("%s", outcome.failure()).isTrue();
        assertThat(outcome.output().get("meta").get("v").asInt()).isEqualTo(1);
    }
}
