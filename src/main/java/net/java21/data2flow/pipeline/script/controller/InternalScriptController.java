package net.java21.data2flow.pipeline.script.controller;

import jakarta.validation.Valid;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.pipeline.script.dto.ScriptCheckRequest;
import net.java21.data2flow.pipeline.script.dto.ScriptCheckResponse;
import net.java21.data2flow.pipeline.script.dto.ScriptTestRunRequest;
import net.java21.data2flow.pipeline.script.dto.ScriptTestRunResponse;
import net.java21.data2flow.pipeline.script.service.ScriptStaticChecker;
import net.java21.data2flow.pipeline.script.service.ScriptTestRunService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** 스크립트 내부 API(core-api가 호출, ADR-021 토큰 없음): API-SCR-30 정적 검사, API-SCR-31 테스트 실행 */
@RestController
public class InternalScriptController {

    private final ScriptStaticChecker checker;
    private final ScriptTestRunService testRuns;

    public InternalScriptController(ScriptStaticChecker checker, ScriptTestRunService testRuns) {
        this.checker = checker;
        this.testRuns = testRuns;
    }

    /** API-SCR-30 {@code POST /internal/pipeline/scripts/check} */
    @PostMapping("/internal/pipeline/scripts/check")
    public ApiResponse<ScriptCheckResponse> check(@Valid @RequestBody ScriptCheckRequest request) {
        var problems = checker.check(request.kind(), request.code());
        return ApiResponse.success(new ScriptCheckResponse(!ScriptStaticChecker.hasErrors(problems), problems));
    }

    /** API-SCR-31 {@code POST /internal/pipeline/scripts/test-run}. 저장·발행하지 않는다(BR-SCR-08) */
    @PostMapping("/internal/pipeline/scripts/test-run")
    public ApiResponse<ScriptTestRunResponse> testRun(@Valid @RequestBody ScriptTestRunRequest request) {
        return ApiResponse.success(testRuns.run(request));
    }
}
