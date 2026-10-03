package net.java21.data2flow.pipeline.script.domain;

import tools.jackson.databind.JsonNode;

import java.util.List;

/**
 * 스크립트 한 번 실행의 결과. 성공이면 {@code output}(JSON, null 반환이면 NullNode), 실패면 {@code failure}.
 *
 * @param output      반환값(호스트가 Jackson으로 옮긴 것). 실패면 null
 * @param failure     실패 정보. 성공이면 null
 * @param logs        console.log 출력(각 1KB 이하)
 * @param durationMs  실행 시간(ms, 소수 셋째 자리)
 * @param outputBytes 반환값 JSON 크기
 */
public record ScriptOutcome(JsonNode output, ScriptFailure failure, List<String> logs, double durationMs, int outputBytes) {

    public ScriptOutcome {
        logs = logs == null ? List.of() : List.copyOf(logs);
    }

    public boolean ok() {
        return failure == null;
    }

    /** 성공했고 스크립트가 null을 돌려줬다(TRANSFORM: 버리기) */
    public boolean returnedNull() {
        return ok() && (output == null || output.isNull());
    }
}
