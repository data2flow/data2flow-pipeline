package net.java21.data2flow.pipeline.script.dto;

import net.java21.data2flow.pipeline.script.domain.ScriptProblem;

import java.util.List;

/** API-SCR-30 응답 {@code {ok, problems[{line, col, severity, code, message}]}} */
public record ScriptCheckResponse(boolean ok, List<ScriptProblem> problems) {
}
