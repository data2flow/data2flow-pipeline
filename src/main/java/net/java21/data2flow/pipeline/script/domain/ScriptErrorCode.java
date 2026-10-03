package net.java21.data2flow.pipeline.script.domain;

/** 스크립트 실행 결과 오류 코드(SCR domain-model "오류 코드", HTTP 상태가 아니라 실행 결과) */
public enum ScriptErrorCode {
    /** 실행 시간(CPU 50ms) 또는 문장 수 한도 초과(BR-SCR-02) */
    SCRIPT_TIMEOUT,
    /** 예외, 문법 오류, 메모리 부족 */
    SCRIPT_RUNTIME_ERROR,
    /** 호스트·입출력·네트워크·타이머·모듈 로드·동적 코드 생성 시도(BR-SCR-01) */
    SCRIPT_FORBIDDEN_API,
    /** 반환값 계약 위반 또는 64KB 초과(BR-SCR-06) */
    SCRIPT_OUTPUT_INVALID
}
