package net.java21.data2flow.pipeline.script.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.service.ScriptPerformanceAdvisor;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** 스크립트 운영 내부 API(API-SCR-35 테스트 케이스 일괄 실행, API-SCR-36 운영 지표) 요청·응답 */
public final class ScriptOpsDtos {

    private ScriptOpsDtos() {
    }

    /** API-SCR-36 응답 {@code {points[], warnings[]}} */
    public record StatsResponse(List<Point> points, List<ScriptPerformanceAdvisor.Warning> warnings) {
    }

    /** 지표 한 점 {@code {t, versionNo, processed, errors, timeouts, avgMs, p95Ms, maxInputBytes}} */
    public record Point(Instant t, int versionNo, int processed, int errors, int timeouts, double avgMs, double p95Ms,
                        int maxInputBytes, int logsDropped) {
    }

    /** API-SCR-35 요청 */
    public record TestCasesRunRequest(@NotNull Long organizationId, @NotNull ScriptKind kind, @NotNull String code,
                                      Long scriptId, List<String> moduleRefs,
                                      @NotNull @Size(min = 1, max = 50) @Valid List<TestCase> cases) {
    }

    /** 케이스 {@code {id, name, input, context, expected, compareMode, compareFields, tolerance}} */
    public record TestCase(String id, String name, @NotNull JsonNode input, JsonNode context, JsonNode expected,
                           String compareMode, List<String> compareFields, Double tolerance) {
    }

    /** API-SCR-35 응답 {@code {passed, failed, results[]}} */
    public record TestCasesRunResponse(int passed, int failed, List<CaseResult> results) {
    }

    public record CaseResult(String caseId, String name, boolean passed, List<Map<String, Object>> diff, double durationMs,
                             ScriptTestRunResponse.Error error) {
    }
}
