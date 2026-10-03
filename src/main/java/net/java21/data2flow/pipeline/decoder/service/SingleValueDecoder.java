package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.decoder.DecodeException;
import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import net.java21.data2flow.contracts.message.decoder.DecoderKeys;
import net.java21.data2flow.contracts.message.decoder.PayloadDecoder;
import net.java21.data2flow.pipeline.common.PayloadEncoding;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/**
 * 기본 디코더 {@code single-value}(ING-02.04): 토픽 마지막 칸이 측정 키, 그 앞 칸이 externalId, payload는 숫자 하나.
 * 예: {@code sensors/esp-02/temperature} + {@code 22.8} → esp-02, temperature 22.8. 숫자가 아니면 {@code ING_VALUE_NOT_NUMERIC}.
 * payload에 측정 시각·카운터가 없으므로 중복 판정은 수신 시각 버킷 키를 쓴다(BR-ING-07).
 */
public class SingleValueDecoder implements PayloadDecoder {

    @Override
    public String key() {
        return DecoderKeys.SINGLE_VALUE;
    }

    @Override
    public String version() {
        return "1";
    }

    @Override
    public DecodedUplink decode(RawEnvelope raw, JsonNode config) throws DecodeException {
        String topic = raw.topic() == null ? "" : raw.topic();
        String[] parts = java.util.Arrays.stream(topic.split("/")).filter(s -> !s.isBlank()).toArray(String[]::new);
        if (parts.length < 2) {
            throw new IngestDecodeException(key(), IngestDecodeException.EXTERNAL_ID_MISSING,
                    "토픽에서 기기 ID를 찾을 수 없습니다: " + topic);
        }
        String text = PayloadEncoding.utf8(raw.payload());
        String trimmed = text == null ? "" : text.strip();
        double value;
        try {
            if (trimmed.isEmpty() || !trimmed.matches("[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+)?")) {
                throw new NumberFormatException(trimmed);
            }
            value = Double.parseDouble(trimmed);
        } catch (NumberFormatException e) {
            throw new IngestDecodeException(key(), IngestDecodeException.VALUE_NOT_NUMERIC,
                    "payload가 숫자가 아닙니다: " + (trimmed.length() > 40 ? trimmed.substring(0, 40) + "…" : trimmed));
        }
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IngestDecodeException(key(), IngestDecodeException.VALUE_NOT_NUMERIC, "payload가 유한한 숫자가 아닙니다");
        }
        return new DecodedUplink(parts[parts.length - 2], null,
                List.of(DecodedValue.of(parts[parts.length - 1], value)), null, Map.of());
    }
}
