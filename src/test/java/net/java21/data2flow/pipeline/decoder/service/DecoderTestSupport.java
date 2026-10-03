package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** 디코더 단위 테스트 도구: 원본 봉투 만들기, 디코더 출력 → 비교용 JSON */
final class DecoderTestSupport {

    static final JsonMapper MAPPER = MessageCodec.newMapper();
    static final Instant RECEIVED = Instant.parse("2026-10-03T02:40:09.501Z");

    private DecoderTestSupport() {
    }

    static RawEnvelope raw(String topic, byte[] payload) {
        return RawEnvelope.of(1, 3, "MQTT_SUBSCRIBE", topic, payload, RECEIVED, "ingress-0",
                DedupKeys.detect(3, topic, payload));
    }

    static RawEnvelope raw(String topic, String payload) {
        return raw(topic, payload.getBytes(StandardCharsets.UTF_8));
    }

    static ObjectNode toJson(DecodedUplink uplink) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("externalId", uplink.externalId());
        n.put("measuredAt", uplink.measuredAt() == null ? null : uplink.measuredAt().toString());
        ArrayNode values = n.putArray("values");
        for (DecodedValue v : uplink.values()) {
            ObjectNode o = values.addObject();
            o.put("key", v.key());
            o.set("value", MAPPER.valueToTree(v.value()));
            o.put("unit", v.unit());
        }
        if (uplink.link() == null) {
            n.putNull("link");
        } else {
            n.set("link", MAPPER.valueToTree(uplink.link()));
        }
        n.set("tags", MAPPER.valueToTree(uplink.tags()));
        return n;
    }
}
