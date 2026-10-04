package net.java21.data2flow.pipeline.script.dto;

import jakarta.validation.constraints.NotNull;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import tools.jackson.databind.JsonNode;

/**
 * API-SCR-31 테스트 실행 요청(API-SCR-08과 같음).
 *
 * @param kind           DECODE / TRANSFORM
 * @param code           저장 전 코드도 가능
 * @param input          DECODE {@code {topic, payload(base64 또는 JSON), receivedAt, source:{code, config}}} /
 *                       TRANSFORM CanonicalTelemetry
 * @param context        {@code {device:{id, attributes}, last:{key: value}, config:{}}}
 * @param scriptId       설정값을 가져올 스크립트(선택)
 * @param organizationId 조직
 * @param rawMessageId   input 대신 원본 메시지(선택, DECODE)
 * @param moduleRefs     가져다 쓰는 모듈 {@code name@version}(선택, SCR-04.01). 모듈은 조직의 현재 번들에서 찾는다
 */
public record ScriptTestRunRequest(@NotNull ScriptKind kind, @NotNull String code, JsonNode input, JsonNode context,
                                   Long scriptId, Long organizationId, Long rawMessageId, java.util.List<String> moduleRefs) {

    public ScriptTestRunRequest(ScriptKind kind, String code, JsonNode input, JsonNode context, Long scriptId,
                                Long organizationId, Long rawMessageId) {
        this(kind, code, input, context, scriptId, organizationId, rawMessageId, null);
    }

    public java.util.List<String> moduleRefsOrEmpty() {
        return moduleRefs == null ? java.util.List.of() : moduleRefs;
    }
}
