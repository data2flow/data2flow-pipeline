package net.java21.data2flow.pipeline.common;

import net.java21.data2flow.contracts.error.ErrorCode;

/** pipeline 내부 API 오류 코드(ING domain-model "오류 코드"). 문구는 messages*.properties(4개 언어, ADR-037) */
public enum PipelineErrorCode implements ErrorCode {
    ING_DLQ_BATCH_TOO_LARGE(400),
    ING_DLQ_ITEM_LOCKED(409),
    ING_RAW_MESSAGE_NOT_FOUND(404);

    private final int httpStatus;

    PipelineErrorCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    @Override
    public String code() {
        return name();
    }

    @Override
    public int httpStatus() {
        return httpStatus;
    }
}
