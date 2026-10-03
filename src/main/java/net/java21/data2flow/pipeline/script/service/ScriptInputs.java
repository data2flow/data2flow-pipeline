package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.common.PayloadEncoding;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.Base64;

/** DECODE 입력(SCR-api §3.1 {@code DecodeInput})을 만든다. 실제 처리와 테스트 실행이 같은 모양을 쓴다 */
public final class ScriptInputs {

    private ScriptInputs() {
    }

    /**
     * @param topic        MQTT 토픽 또는 Webhook 경로
     * @param payload      원본 바이트
     * @param receivedAt   수신 시각
     * @param sourceId     데이터 소스 ID
     * @param sourceType   소스 유형(RawEnvelope.sourceType)
     * @param sourceConfig 소스 디코더 설정. 없으면 null
     */
    public static ObjectNode decodeInput(JsonMapper mapper, String topic, byte[] payload, Instant receivedAt,
                                         long sourceId, String sourceType, JsonNode sourceConfig) {
        ObjectNode input = mapper.createObjectNode();
        input.put("topic", topic);
        PayloadEncoding encoding = PayloadEncoding.detect(payload);
        String base64 = Base64.getEncoder().encodeToString(payload);
        if (encoding == PayloadEncoding.JSON) {
            try {
                input.set("payload", mapper.readTree(payload));
            } catch (RuntimeException e) {
                encoding = PayloadEncoding.TEXT;
                input.put("payload", base64);
            }
        } else {
            input.put("payload", base64);
        }
        input.put("payloadBase64", base64);
        input.put("payloadEncoding", encoding.name());
        if (encoding == PayloadEncoding.TEXT) {
            input.put("payloadText", PayloadEncoding.utf8(payload));
        }
        input.put("receivedAt", receivedAt.toString());
        ObjectNode source = input.putObject("source");
        source.put("id", sourceId);
        source.put("code", "source-" + sourceId);
        source.put("type", sourceType);
        source.set("config", sourceConfig == null ? mapper.createObjectNode() : sourceConfig);
        return input;
    }
}
