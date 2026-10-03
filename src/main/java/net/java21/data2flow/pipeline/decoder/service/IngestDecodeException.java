package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.decoder.DecodeException;

/**
 * 디코딩 실패와 처리 결과 오류 코드(ING domain-model 오류 코드): {@code ING_DECODE_FAILED}, {@code ING_EXTERNAL_ID_MISSING},
 * {@code ING_VALUE_NOT_NUMERIC}. 처리 상태는 모두 {@code DECODE_ERROR}.
 */
public class IngestDecodeException extends DecodeException {

    public static final String EXTERNAL_ID_MISSING = "ING_EXTERNAL_ID_MISSING";
    public static final String VALUE_NOT_NUMERIC = "ING_VALUE_NOT_NUMERIC";

    private final String errorCode;
    private final transient Object detail;

    public IngestDecodeException(String decoderKey, String errorCode, String message) {
        this(decoderKey, errorCode, message, null, null);
    }

    public IngestDecodeException(String decoderKey, String errorCode, String message, Object detail, Throwable cause) {
        super(decoderKey, message, cause);
        this.errorCode = errorCode;
        this.detail = detail;
    }

    public static IngestDecodeException failed(String decoderKey, String message) {
        return new IngestDecodeException(decoderKey, RESULT_CODE, message);
    }

    public String errorCode() {
        return errorCode;
    }

    /** 추가 정보(스크립트 오류 줄·열 등). 없으면 null */
    public Object detail() {
        return detail;
    }
}
