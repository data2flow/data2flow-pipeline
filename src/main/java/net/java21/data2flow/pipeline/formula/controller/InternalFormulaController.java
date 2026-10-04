package net.java21.data2flow.pipeline.formula.controller;

import jakarta.validation.Valid;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.pipeline.formula.dto.FormulaDtos;
import net.java21.data2flow.pipeline.formula.service.FormulaPreviewService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 수식 항목 내부 API(core-api가 호출, ADR-021): API-SCR-37 검사·컴파일(API-SCR-20 저장 전 {@code SCRIPT_FORMULA_INVALID} 판정과
 * {@code compiled_js}), API-SCR-38 미리 보기(API-SCR-21 처리).
 */
@RestController
public class InternalFormulaController {

    private final FormulaPreviewService formulas;

    public InternalFormulaController(FormulaPreviewService formulas) {
        this.formulas = formulas;
    }

    @PostMapping("/internal/pipeline/formula-metrics/compile")
    public ApiResponse<FormulaDtos.CompileResponse> compile(@Valid @RequestBody FormulaDtos.CompileRequest request) {
        return ApiResponse.success(formulas.compile(request));
    }

    @PostMapping("/internal/pipeline/formula-metrics/preview")
    public ApiResponse<FormulaDtos.PreviewResponse> preview(@Valid @RequestBody FormulaDtos.PreviewRequest request) {
        return ApiResponse.success(formulas.preview(request));
    }
}
