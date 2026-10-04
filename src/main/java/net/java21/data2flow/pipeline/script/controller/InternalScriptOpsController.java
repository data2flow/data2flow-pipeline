package net.java21.data2flow.pipeline.script.controller;

import jakarta.validation.Valid;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.dto.ScriptOpsDtos;
import net.java21.data2flow.pipeline.script.dto.ScriptTestRunRequest;
import net.java21.data2flow.pipeline.script.dto.ScriptTestRunResponse;
import net.java21.data2flow.pipeline.script.repository.ScriptOpsRepository;
import net.java21.data2flow.pipeline.script.service.ScriptPerformanceAdvisor;
import net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry;
import net.java21.data2flow.pipeline.script.service.ScriptTestRunService;
import net.java21.data2flow.pipeline.script.service.TestCaseComparator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 스크립트 운영 내부 API(core-api가 호출, ADR-021): API-SCR-35 테스트 케이스 일괄 실행(SCR-03.03 배포 전 자동 확인),
 * API-SCR-36 운영 지표와 성능·오류 경고(SCR-03.05·05.03, API-SCR-12의 원천). 권한 판정은 외부 API를 받는 core-api가 한다.
 */
@RestController
public class InternalScriptOpsController {

    private final ScriptOpsRepository ops;
    private final ScriptRuntimeRegistry registry;
    private final ScriptTestRunService testRuns;

    public InternalScriptOpsController(ScriptOpsRepository ops, ScriptRuntimeRegistry registry, ScriptTestRunService testRuns) {
        this.ops = ops;
        this.registry = registry;
        this.testRuns = testRuns;
    }

    /** API-SCR-36 {@code GET /internal/pipeline/scripts/{script-id}/stats?organizationId=&from=&to=&step=1m|1h} */
    @GetMapping("/internal/pipeline/scripts/{script-id}/stats")
    public ApiResponse<ScriptOpsDtos.StatsResponse> stats(@PathVariable("script-id") long scriptId,
                                                          @RequestParam long organizationId, @RequestParam Instant from,
                                                          @RequestParam Instant to,
                                                          @RequestParam(defaultValue = "1m") String step) {
        if (!to.isAfter(from) || Duration.between(from, to).toDays() > 90 || !(step.equals("1m") || step.equals("1h"))) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
        }
        List<ScriptOpsRepository.StatRow> rows = ops.findStats(organizationId, scriptId, from, to, step.equals("1h"));
        String code = registry.plan(organizationId).bundle().scripts().stream().filter(s -> s.scriptId() == scriptId)
                .map(RuntimeBundle.Script::code).findFirst().orElse(null);
        List<ScriptOpsDtos.Point> points = rows.stream().map(r -> new ScriptOpsDtos.Point(r.minute(), r.versionNo(),
                r.processed(), r.errors(), r.timeouts(), round(r.avgMs()), round(r.p95Ms()), r.maxInputBytes(),
                r.logsDropped())).toList();
        return ApiResponse.success(new ScriptOpsDtos.StatsResponse(points, ScriptPerformanceAdvisor.warnings(rows, code)));
    }

    /** API-SCR-35 {@code POST /internal/pipeline/scripts/test-cases/run}. 저장·발행하지 않는다(BR-SCR-08) */
    @PostMapping("/internal/pipeline/scripts/test-cases/run")
    public ApiResponse<ScriptOpsDtos.TestCasesRunResponse> runCases(@Valid @RequestBody ScriptOpsDtos.TestCasesRunRequest request) {
        List<ScriptOpsDtos.CaseResult> results = new ArrayList<>();
        int passed = 0;
        for (ScriptOpsDtos.TestCase c : request.cases()) {
            ScriptTestRunResponse r = testRuns.run(new ScriptTestRunRequest(request.kind(), request.code(), c.input(),
                    c.context(), request.scriptId(), request.organizationId(), null, request.moduleRefs()));
            boolean ok;
            List<Map<String, Object>> diff = List.of();
            if (!r.ok()) {
                ok = false;
            } else {
                diff = TestCaseComparator.compare(c.compareMode(), c.expected() == null ? tools.jackson.databind.node.NullNode.getInstance() : c.expected(),
                        r.output() == null ? tools.jackson.databind.node.NullNode.getInstance() : r.output(), c.compareFields(), c.tolerance())
                        .stream().map(TestCaseComparator.Difference::toMap).toList();
                ok = diff.isEmpty();
            }
            passed += ok ? 1 : 0;
            results.add(new ScriptOpsDtos.CaseResult(c.id(), c.name(), ok, diff, r.durationMs(), r.error()));
        }
        return ApiResponse.success(new ScriptOpsDtos.TestCasesRunResponse(passed, results.size() - passed, results));
    }

    private static double round(double v) {
        return Math.round(v * 100) / 100.0;
    }
}
