package net.java21.data2flow.pipeline.script.service;

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import net.java21.data2flow.pipeline.script.domain.ScriptErrorCode;
import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness.Attack;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-02.01 TC-SCR-022 · AT-SCR-04.4 · NFR-03.04: 호스트 접근 공격 코퍼스(host-*.js 13종)가 모두 SCRIPT_FORBIDDEN_API로 막힌다 */
class ScriptSandboxHostAccessTest {

    static Stream<Attack> hostAttacks() {
        return ScriptSandboxHarness.attacks("host-");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("hostAttacks")
    @DisplayName("[SCR-02.01][AT-SCR-04.4] TC-SCR-022 호스트·입출력·네트워크·타이머·모듈 접근은 SCRIPT_FORBIDDEN_API")
    void hostAccessIsForbidden(Attack attack) {
        ScriptOutcome outcome = ScriptSandboxHarness.transform(attack.code());

        assertThat(attack.expected()).contains(attack.resultOf(outcome));
        assertThat(outcome.failure().code()).isEqualTo(ScriptErrorCode.SCRIPT_FORBIDDEN_API);
    }

    @Test
    @DisplayName("[SCR-02.01][AT-SCR-04.4] TC-SCR-022 코퍼스는 13종이고, 실행 중 JVM 파일 읽기·쓰기·소켓·프로세스 생성 이벤트가 0건")
    void noFileSocketOrProcessActivity() throws Exception {
        List<Attack> attacks = hostAttacks().toList();
        assertThat(attacks).hasSize(13);
        String thread = Thread.currentThread().getName();
        Path dump = Files.createTempFile("sandbox-jfr", ".jfr");
        try (Recording recording = new Recording()) {
            for (String event : List.of("jdk.FileRead", "jdk.FileWrite", "jdk.SocketRead", "jdk.SocketWrite",
                    "jdk.SocketConnect", "jdk.ProcessStart")) {
                recording.enable(event).withThreshold(Duration.ZERO).withStackTrace();
            }
            recording.start();
            for (Attack attack : attacks) {
                ScriptSandboxHarness.transform(attack.code());
            }
            recording.stop();
            recording.dump(dump);
        }
        // 클래스 적재(JVM이 jar에서 클래스를 읽음)는 스크립트 행동이 아니므로 뺀다
        List<RecordedEvent> events = RecordingFile.readAllEvents(dump).stream()
                .filter(e -> e.getThread() != null && thread.equals(e.getThread().getJavaName()))
                .filter(e -> !isClassLoading(e))
                .toList();
        Files.deleteIfExists(dump);

        assertThat(events).as("공격 실행 스레드의 파일·소켓·프로세스 이벤트").isEmpty();
    }

    private static boolean isClassLoading(RecordedEvent event) {
        String path = event.hasField("path") ? event.getString("path") : null;
        boolean jar = path != null && (path.endsWith(".jar") || path.endsWith(".class") || path.contains("/jre/lib/")
                || path.endsWith("/lib/modules"));
        boolean loader = event.getStackTrace() != null && event.getStackTrace().getFrames().stream()
                .anyMatch(f -> f.getMethod().getType().getName().contains("ClassLoader")
                        || f.getMethod().getType().getName().startsWith("jdk.internal.loader"));
        return jar || loader;
    }
}
