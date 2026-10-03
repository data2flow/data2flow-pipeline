package net.java21.data2flow.pipeline.ingest.domain;

import net.java21.data2flow.contracts.message.decoder.DecodedValue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * 메시지 한도(ING-07.01, BR-ING-10, TC-ING-080): 측정 항목 100개, 키 64자, 문자열 값 1KB(UTF-8 바이트). 넘으면 잘라 내지 않고
 * 메시지 전체를 INVALID로 표시하고 사유를 남긴다.
 */
public final class MessageLimitValidator {

    public static final String METRICS_EXCEEDED = "ING_LIMIT_METRICS_EXCEEDED";
    public static final String KEY_TOO_LONG = "ING_LIMIT_KEY_TOO_LONG";
    public static final String STRING_TOO_LONG = "ING_LIMIT_STRING_TOO_LONG";
    public static final String PAYLOAD_EXCEEDED = "ING_LIMIT_PAYLOAD_EXCEEDED";

    private final int maxMetrics;
    private final int maxKeyLength;
    private final int maxStringBytes;

    public MessageLimitValidator(int maxMetrics, int maxKeyLength, int maxStringBytes) {
        this.maxMetrics = maxMetrics;
        this.maxKeyLength = maxKeyLength;
        this.maxStringBytes = maxStringBytes;
    }

    public Optional<Violation> check(List<DecodedValue> values) {
        if (values.size() > maxMetrics) {
            return Optional.of(new Violation(METRICS_EXCEEDED, values.size() + "/" + maxMetrics, null));
        }
        for (DecodedValue v : values) {
            if (v.key().length() > maxKeyLength) {
                String head = v.key().substring(0, Math.min(20, v.key().length()));
                return Optional.of(new Violation(KEY_TOO_LONG, "측정 항목 이름이 너무 깁니다: " + head + "…", head));
            }
            if (v.value() instanceof String s && s.getBytes(StandardCharsets.UTF_8).length > maxStringBytes) {
                return Optional.of(new Violation(STRING_TOO_LONG, "문자열 값이 1KB를 넘습니다: " + v.key(), v.key()));
            }
        }
        return Optional.empty();
    }

    /**
     * @param code    오류 코드
     * @param message 사유(원본 기록 error_detail)
     * @param key     위반 위치(키 앞부분 등)
     */
    public record Violation(String code, String message, String key) {
    }
}
