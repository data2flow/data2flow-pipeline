package net.java21.data2flow.pipeline.script.domain;

/**
 * 실행 실패 정보(SCR-05.01 오류 스냅샷의 메시지·줄·열).
 *
 * @param code    오류 코드
 * @param message 메시지(500자 이하)
 * @param line    사용자 코드 줄. 모르면 null
 * @param col     사용자 코드 열. 모르면 null
 */
public record ScriptFailure(ScriptErrorCode code, String message, Integer line, Integer col) {

    public static ScriptFailure of(ScriptErrorCode code, String message) {
        return new ScriptFailure(code, truncate(message), null, null);
    }

    public static String truncate(String message) {
        if (message == null) {
            return "";
        }
        return message.length() <= 500 ? message : message.substring(0, 500);
    }
}
