package net.java21.data2flow.pipeline.script.dto;

import jakarta.validation.constraints.NotNull;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;

import java.util.List;

/** API-SCR-30 정적 검사 요청 {@code {kind, code, moduleRefs?}} */
public record ScriptCheckRequest(@NotNull ScriptKind kind, @NotNull String code, List<String> moduleRefs) {
}
