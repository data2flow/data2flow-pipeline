package net.java21.data2flow.pipeline.script.domain;

/** 스크립트 실패 정책(SCR-02.03, BR-SCR-04). 기본값은 FAIL_OPEN */
public enum FailurePolicy {
    /** 실패한 스크립트의 입력을 그대로 다음 단계로 넘긴다 */
    FAIL_OPEN,
    /** 메시지를 저장하지 않고 DLQ로 보낸다(처리 상태 SCRIPT_ERROR) */
    FAIL_CLOSED;

    /** 모르는 값·null은 기본값 FAIL_OPEN */
    public static FailurePolicy parse(String value) {
        if (value == null) {
            return FAIL_OPEN;
        }
        return "FAIL_CLOSED".equalsIgnoreCase(value.trim()) ? FAIL_CLOSED : FAIL_OPEN;
    }
}
