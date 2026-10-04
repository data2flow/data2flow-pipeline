package net.java21.data2flow.pipeline.script.dto;

import jakarta.validation.constraints.NotNull;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;

import java.util.List;

/**
 * API-SCR-30 정적 검사 요청 {@code {kind, code, moduleRefs?, organizationId?}}. 공유 모듈을 쓰는 코드는 조직이 있어야 모듈을 찾는다
 * (SCR-04.01).
 */
public record ScriptCheckRequest(@NotNull ScriptKind kind, @NotNull String code, List<String> moduleRefs, Long organizationId) {

    public ScriptCheckRequest(ScriptKind kind, String code, List<String> moduleRefs) {
        this(kind, code, moduleRefs, null);
    }

    public List<String> moduleRefsOrEmpty() {
        return moduleRefs == null ? List.of() : moduleRefs;
    }
}
