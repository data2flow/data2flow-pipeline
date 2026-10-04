package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.FailurePolicy;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.repository.ScriptOpsRepository;
import net.java21.data2flow.pipeline.support.MutableClock;
import net.java21.data2flow.script.sandbox.ScriptErrorCode;
import net.java21.data2flow.script.sandbox.ScriptFailure;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** SCR-03.05 운영 지표, SCR-05.01 오류 스냅샷, SCR-05.02 로그 수집, SCR-05.03 성능 경고 */
class ScriptOpsTest {

    private final MutableClock clock = MutableClock.atUtc("2026-10-03T00:00:00Z");
    private final ScriptOpsRepository repository = mock(ScriptOpsRepository.class);
    private final ScriptOps ops = new ScriptOps(repository, clock);

    private static RuntimeBundle.Script script(Instant captureUntil) {
        return new RuntimeBundle.Script(42, ScriptKind.TRANSFORM, 420, 3, "function transform(m){return m;}", null,
                FailurePolicy.FAIL_OPEN, true, List.of(), List.of(), 1, captureUntil);
    }

    private static ScriptOutcome ok(double ms, List<String> logs) {
        return new ScriptOutcome(null, null, logs, ms, 10);
    }

    @SuppressWarnings("unchecked")
    private List<ScriptOpsRepository.StatRow> flushStats() {
        ArgumentCaptor<List<ScriptOpsRepository.StatRow>> rows = ArgumentCaptor.forClass(List.class);
        ops.flush();
        verify(repository, org.mockito.Mockito.atLeastOnce()).upsertStats(rows.capture());
        return rows.getAllValues().stream().flatMap(List::stream).toList();
    }

    @Test
    @DisplayName("[SCR-03.05][AT-SCR-10.3] TC-SCR-059 스크립트·버전·분별 처리·오류·시간 초과 수, 평균·p95, 가장 큰 입력")
    void minuteStats() {
        Instant now = clock.instant();
        for (int i = 1; i <= 100; i++) {
            ops.record(1, script(null), ok(i, List.of()), 100 + i, "{}", 7L, null, now);
        }
        ops.record(1, script(null), new ScriptOutcome(null, ScriptFailure.of(ScriptErrorCode.SCRIPT_TIMEOUT, "cpu"), List.of(),
                150, 0), 50_000, "{\"x\":1}", 7L, 99L, now.plusSeconds(10));
        ops.record(1, script(null), ok(5, List.of()), 10, "{}", 7L, null, now.plusSeconds(70));

        List<ScriptOpsRepository.StatRow> rows = flushStats();
        ScriptOpsRepository.StatRow first = rows.stream().filter(r -> r.minute().equals(now)).findFirst().orElseThrow();
        assertThat(first.processed()).isEqualTo(101);
        assertThat(first.errors()).isEqualTo(1);
        assertThat(first.timeouts()).isEqualTo(1);
        assertThat(first.avgMs()).isCloseTo((5050 + 150) / 101.0, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(first.p95Ms()).isEqualTo(96.0);
        assertThat(first.maxMs()).isEqualTo(150);
        assertThat(first.maxInputBytes()).isEqualTo(50_000);
        assertThat(first.versionNo()).isEqualTo(3);
        assertThat(rows).filteredOn(r -> r.minute().equals(now.plusSeconds(60))).singleElement()
                .satisfies(r -> assertThat(r.processed()).isEqualTo(1));
        assertThat(ScriptOps.p95(new double[0], 0)).isZero();
    }

    @Test
    @DisplayName("[SCR-05.01][AT-SCR-10.1] TC-SCR-080 오류마다 메시지·줄·열·입력 스냅샷(64KB 상한)·버전, 스크립트별 최근 100건만 유지 요청")
    @SuppressWarnings("unchecked")
    void errorSnapshots() {
        String big = "{\"p\":\"" + "x".repeat(70_000) + "\"}";
        ops.record(1, script(null), new ScriptOutcome(null, new ScriptFailure(ScriptErrorCode.SCRIPT_RUNTIME_ERROR, "boom", 3, 7),
                List.of(), 1, 0), 20, "{\"a\":1}", 7L, 5L, clock.instant());
        ops.record(1, script(null), new ScriptOutcome(null, ScriptFailure.of(ScriptErrorCode.SCRIPT_RUNTIME_ERROR, "x".repeat(800)),
                List.of(), 1, 0), big.length(), big, 7L, 6L, clock.instant());

        ops.flush();

        ArgumentCaptor<List<ScriptOpsRepository.ErrorRow>> errors = ArgumentCaptor.forClass(List.class);
        verify(repository).insertErrors(errors.capture(), org.mockito.ArgumentMatchers.eq(100));
        assertThat(errors.getValue()).hasSize(2);
        ScriptOpsRepository.ErrorRow e = errors.getValue().getFirst();
        assertThat(e.message()).isEqualTo("boom");
        assertThat(e.line()).isEqualTo(3);
        assertThat(e.col()).isEqualTo(7);
        assertThat(e.inputSnapshot()).isEqualTo("{\"a\":1}");
        assertThat(e.versionNo()).isEqualTo(3);
        assertThat(errors.getValue().get(1).inputSnapshot()).as("64KB 넘는 입력은 스냅샷 없음").isNull();
        assertThat(errors.getValue().get(1).message()).hasSize(500);
    }

    @Test
    @DisplayName("[SCR-05.02][AT-SCR-10.2] TC-SCR-082 수집 OFF면 0건, ON이면 스크립트당 초당 10건(11번째부터 버리고 수를 셈), 30분 뒤 자동 OFF")
    @SuppressWarnings("unchecked")
    void logCapture() {
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            lines.add("line " + i);
        }
        ops.record(1, script(null), ok(1, lines), 1, "{}", 7L, null, clock.instant());
        Instant until = clock.instant().plus(Duration.ofMinutes(30));
        ops.record(1, script(until), ok(1, lines), 1, "{}", 7L, null, clock.instant());
        ops.record(1, script(until), ok(1, List.of("next second")), 1, "{}", 7L, null, clock.instant().plusSeconds(1));
        clock.advance(Duration.ofMinutes(30).plusSeconds(1));
        ops.record(1, script(until), ok(1, List.of("after")), 1, "{}", 7L, null, clock.instant());

        ArgumentCaptor<List<ScriptOpsRepository.LogRow>> logs = ArgumentCaptor.forClass(List.class);
        List<ScriptOpsRepository.StatRow> stats = flushStats();
        verify(repository).insertLogs(logs.capture());
        assertThat(logs.getValue()).extracting(ScriptOpsRepository.LogRow::message)
                .containsExactly("line 0", "line 1", "line 2", "line 3", "line 4", "line 5", "line 6", "line 7", "line 8",
                        "line 9", "next second");
        assertThat(stats.stream().mapToInt(ScriptOpsRepository.StatRow::logsDropped).sum()).isEqualTo(5);
        assertThat(script(until).capturingLogs(clock.instant())).isFalse();
    }

    @Test
    @DisplayName("[SCR-03.05] DB 쓰기 실패는 경고만 남기고 처리 경로에 영향 없음")
    void flushFailure() {
        doThrow(new RuntimeException("db down")).when(repository).upsertStats(anyList());
        ops.record(1, script(null), ok(1, List.of()), 1, "{}", 7L, null, clock.instant());
        ops.flush();
        verify(repository, org.mockito.Mockito.never()).insertErrors(anyList(), anyInt());
    }

    @Test
    @DisplayName("[SCR-05.03][AT-SCR-10.3] TC-SCR-084 p95 25ms → SLOW 경고(원인 후보: 큰 입력 32KB 초과, 반복문), 19.9ms → 없음, 오류율 10% 이상 → ERROR_RATE")
    void advisor() {
        Instant t = clock.instant();
        var slow = new ScriptOpsRepository.StatRow(42, 420, t, 1, 3, 100, 2, 0, 12, 25, 40, 40_000, 0);
        assertThat(ScriptPerformanceAdvisor.warnings(List.of(slow), "function transform(m){ for (const x of m.metrics) {} }"))
                .singleElement().satisfies(w -> {
                    assertThat(w.type()).isEqualTo("SLOW");
                    assertThat(w.value()).isEqualTo(25.0);
                    assertThat(w.hints()).containsExactly("LARGE_INPUT", "LARGE_LOOP");
                });
        var fine = new ScriptOpsRepository.StatRow(42, 420, t, 1, 3, 100, 0, 0, 5, 19.9, 30, 100, 0);
        assertThat(ScriptPerformanceAdvisor.warnings(List.of(fine), "x")).isEmpty();
        var cpu = new ScriptOpsRepository.StatRow(42, 421, t.plusSeconds(60), 1, 4, 10, 1, 0, 5, 30, 30, 100, 0);
        assertThat(ScriptPerformanceAdvisor.warnings(List.of(slow, cpu), null)).as("마지막 버전 기준")
                .extracting(ScriptPerformanceAdvisor.Warning::type).containsExactly("SLOW", "ERROR_RATE");
        assertThat(ScriptPerformanceAdvisor.warnings(List.of(cpu), null).getFirst().hints()).containsExactly("CPU");
        assertThat(ScriptPerformanceAdvisor.warnings(List.of(), null)).isEmpty();
    }
}
