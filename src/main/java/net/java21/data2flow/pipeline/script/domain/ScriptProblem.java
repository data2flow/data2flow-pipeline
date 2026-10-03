package net.java21.data2flow.pipeline.script.domain;

/**
 * 정적 검사 결과 한 건(API-SCR-07·30 {@code problems[]}).
 *
 * @param line     줄(1부터)
 * @param col      열(1부터)
 * @param severity ERROR(배포 불가) 또는 WARNING
 * @param code     문제 코드(예: SCRIPT_FORBIDDEN_API, SYNTAX_ERROR, ENTRY_MISSING, RETURN_MISSING)
 * @param message  사용자 문구
 */
public record ScriptProblem(int line, int col, Severity severity, String code, String message) {

    public enum Severity {
        ERROR, WARNING
    }
}
