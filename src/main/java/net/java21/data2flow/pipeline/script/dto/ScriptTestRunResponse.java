package net.java21.data2flow.pipeline.script.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.JsonNode;

import java.util.List;

/** API-SCR-31 응답 {@code {ok, output, diff, logs[], durationMs, outputBytes, error?}} */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ScriptTestRunResponse(boolean ok, JsonNode output, Diff diff, List<Log> logs, double durationMs,
                                    int outputBytes, Error error) {

    public record Diff(List<String> added, List<String> removed, List<Change> changed) {
    }

    public record Change(String key, Double from, Double to) {
    }

    public record Log(String at, String message) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Error(String code, String message, Integer line, Integer col, String stack) {
    }
}
