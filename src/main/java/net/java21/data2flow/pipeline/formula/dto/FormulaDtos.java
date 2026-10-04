package net.java21.data2flow.pipeline.formula.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** 수식 항목 내부 API(API-SCR-37 컴파일·검사, API-SCR-38 미리 보기) 요청·응답 */
public final class FormulaDtos {

    private FormulaDtos() {
    }

    /** API-SCR-37 {@code {organizationId?, expression, knownKeys?[]}}. knownKeys가 없으면 조직 측정 항목·별칭으로 오타를 본다 */
    public record CompileRequest(Long organizationId, @NotBlank String expression, List<String> knownKeys) {
    }

    /** API-SCR-37 응답. 오류면 ok=false와 {@code error{code: SCRIPT_FORMULA_INVALID, line, col, message}} */
    public record CompileResponse(boolean ok, String compiledJs, List<String> inputs, Map<String, String> windows,
                                  Error error) {
    }

    public record Error(String code, int line, int col, String message) {
    }

    /** API-SCR-38 {@code {organizationId, expression, deviceId, hours: 1~24}} */
    public record PreviewRequest(@NotNull Long organizationId, @NotBlank String expression, @NotNull Long deviceId,
                                 @NotNull @Min(1) @Max(24) Integer hours) {
    }

    /** API-SCR-38 응답 {@code {series:[{t, value}], inputs:{key:[{t, value}]}}}(API-SCR-21과 같음) */
    public record PreviewResponse(List<Point> series, Map<String, List<Point>> inputs) {
    }

    public record Point(Instant t, double value) {
    }
}
