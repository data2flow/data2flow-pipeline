package net.java21.data2flow.pipeline.script.domain;

/** 스크립트 종류(SCR-01). DECODE는 데이터 소스, TRANSFORM은 기기 모델·기기에 연결한다 */
public enum ScriptKind {
    DECODE("decode"),
    TRANSFORM("transform");

    private final String functionName;

    ScriptKind(String functionName) {
        this.functionName = functionName;
    }

    /** 스크립트가 정의해야 하는 진입 함수 이름(SCR-api §3) */
    public String functionName() {
        return functionName;
    }
}
