package net.java21.data2flow.pipeline.ingest;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.IngressStatus;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRepository;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRow;
import net.java21.data2flow.pipeline.ingest.service.IngestProcessor;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-09.07·09.08 AT-DSC-16.1·16.4·16.5 BR-DSC-28(pipeline 받는 쪽, ADR-056): ingress가 바꾼 payload(JSON)·원본(originalPayload)·
 * 토픽 템플릿 값(topicAttributes)·미처리 판정(ingressStatus)을 받아 저장·처리한다.
 */
class IngressPayloadFormatIT extends IntegrationTestSupport {

    private static final long SOURCE = 23;
    private static final long SINGLE = 24;
    private static final String TOPIC = "site/a/room/301/em-1/temperature";
    private static final byte[] ORIGINAL = {0x0A, 0x04, 'e', 'm', '-', '1', 0x11, 0, 0, 0, 0, 0, (byte) 0x80, 0x36, 0x40};
    private static final Map<String, String> ATTRS = Map.of("site", "a", "room", "301", "deviceId", "em-1",
            "metric", "temperature", IngressStatus.ATTR_EXTERNAL_ID, "em-1", IngressStatus.ATTR_SPACE_HINT, "a/301");

    @Autowired
    private IngestProcessor processor;
    @Autowired
    private RawMessageRepository raws;

    @BeforeEach
    void sources() {
        CORE.source(SOURCE, 1, "generic-json", "AUTO_REGISTER", null);
        // 기기 ID 경로가 payload에 없으면 토픽 템플릿 값(externalId)을 쓴다
        CORE.sourceDecoderConfig(SOURCE, CODEC.mapper().readTree(
                "{\"deviceIdFrom\":\"$.nope\",\"metrics\":[{\"path\":\"$.temperature\",\"key\":\"temperature\"},"
                        + "{\"path\":\"$.humidity\",\"key\":\"humidity\"}]}"));
        CORE.source(SINGLE, 1, "single-value", "AUTO_REGISTER", null);
        CORE.metric("temperature", "°C", -40.0, 125.0);
        CORE.metric("humidity", "%", 0.0, 100.0);
    }

    private RawEnvelope converted(long source, String topic, String json) {
        return envelope(source, SourceTypes.CONNECTOR, topic, ORIGINAL, clock.instant())
                .withTopicAttributes(ATTRS)
                .withConvertedPayload("PROTOBUF", json.getBytes(StandardCharsets.UTF_8), "sha256:" + UUID.randomUUID());
    }

    private Map<String, Object> row(UUID messageId) {
        return jdbc.sql("""
                        SELECT external_id, payload_format, original_payload, topic_attributes::text AS attrs, error_code, payload
                          FROM data2flow_pipeline.raw_messages WHERE message_id = :id""")
                .param("id", messageId).query().singleRow();
    }

    @Test
    @DisplayName("[DSC-09.07][AT-DSC-16.1][DSC-09.08][AT-DSC-16.4] 바꾼 JSON을 디코딩하고 원본·형식·템플릿 값을 보관, 기기 ID는 템플릿, 공간 힌트는 태그")
    void convertedPayloadIsDecodedAndOriginalKept() {
        RawEnvelope e = converted(SOURCE, TOPIC, "{\"deviceId\":\"em-1\",\"temperature\":22.5,\"humidity\":41}");
        publish(e);
        await().atMost(TestStreams.timeout()).until(() -> !telemetry.telemetry().isEmpty());

        assertThat(rawStatus(e.messageId())).isEqualTo("OK");
        Map<String, Object> row = row(e.messageId());
        assertThat(row.get("external_id")).isEqualTo("em-1");
        assertThat(row.get("payload_format")).isEqualTo("PROTOBUF");
        assertThat((byte[]) row.get("original_payload")).isEqualTo(ORIGINAL);
        assertThat(CODEC.mapper().readTree((String) row.get("attrs")).get("spaceHint").asString()).isEqualTo("a/301");
        CanonicalTelemetry t = telemetry.telemetry().getFirst();
        assertThat(t.metric("temperature").value()).isEqualTo(22.5);
        assertThat(t.metric("humidity").value()).isEqualTo(41.0);
        assertThat(t.meta().tags()).containsEntry("spaceHint", "a/301").containsEntry("site", "a")
                .doesNotContainKey("externalId");

        // 재처리도 보관한 템플릿 값으로 같은 기기에
        long rawId = jdbc.sql("SELECT id FROM data2flow_pipeline.raw_messages WHERE message_id = :id")
                .param("id", e.messageId()).query(Long.class).single();
        RawMessageRow stored = raws.findById(1, rawId).orElseThrow();
        assertThat(stored.topicAttributes()).contains("em-1");
        IngestProcessor.Outcome again = processor.reprocess(stored);
        assertThat(again.status()).isIn(RawMessageStatus.OK, RawMessageStatus.DUPLICATE);
        assertThat(row(e.messageId()).get("external_id")).isEqualTo("em-1");
    }

    @Test
    @DisplayName("[DSC-09.08][AT-DSC-16.4] single-value: 템플릿의 externalId·metric으로 기기·측정 키를 정한다(값 하나)")
    void singleValueUsesTemplate() {
        RawEnvelope e = envelope(SINGLE, SourceTypes.MQTT_SUBSCRIBE, TOPIC, "21.5".getBytes(StandardCharsets.UTF_8),
                clock.instant()).withTopicAttributes(Map.of(IngressStatus.ATTR_EXTERNAL_ID, "em-9", IngressStatus.ATTR_METRIC,
                "humidity"));
        publish(e);
        await().atMost(TestStreams.timeout()).until(() -> !telemetry.telemetry().isEmpty());
        assertThat(row(e.messageId()).get("external_id")).isEqualTo("em-9");
        assertThat(telemetry.telemetry().getFirst().metric("humidity").value()).isEqualTo(21.5);
    }

    @Test
    @DisplayName("[DSC-09.08][AT-DSC-16.5] BR-DSC-28 UNMATCHED_TOPIC은 디코딩 없이 INVALID·UNMATCHED_TOPIC으로 원본만, 자동 등록·표준 메시지 0")
    void unmatchedTopicKeepsRawOnly() {
        RawEnvelope e = envelope(SOURCE, SourceTypes.CONNECTOR, "site/a/em-1",
                "{\"temperature\":22.5}".getBytes(StandardCharsets.UTF_8), clock.instant())
                .withIngressStatus(IngressStatus.UNMATCHED_TOPIC, "토픽이 템플릿에 맞지 않습니다");
        publish(e);
        awaitRaw(e.messageId());
        awaitIdle();
        assertThat(rawStatus(e.messageId())).isEqualTo("INVALID");
        assertThat(row(e.messageId()).get("error_code")).isEqualTo("UNMATCHED_TOPIC");
        assertThat(new String((byte[]) row(e.messageId()).get("payload"), StandardCharsets.UTF_8)).isEqualTo("{\"temperature\":22.5}");
        assertThat(CORE.autoRegisterRequests()).isEmpty();
        assertThat(telemetry.telemetry()).isEmpty();
    }

    @Test
    @DisplayName("[DSC-09.07] UC-DSC-16 1a ingress DECODE_ERROR는 DECODE_ERROR(실패 보관함 DECODE)로 원본만, 기기 ID는 템플릿 값")
    void ingressDecodeError() {
        RawEnvelope e = envelope(SOURCE, SourceTypes.CONNECTOR, TOPIC, new byte[]{(byte) 0xFF, 1}, clock.instant())
                .withTopicAttributes(ATTRS).withIngressStatus(IngressStatus.DECODE_ERROR, "PROTOBUF: 읽을 수 없습니다");
        publish(e);
        awaitRaw(e.messageId());
        assertThat(rawStatus(e.messageId())).isEqualTo("DECODE_ERROR");
        assertThat(row(e.messageId()).get("external_id")).isEqualTo("em-1");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.dlq_items")).isEqualTo(1);
        assertThat(telemetry.telemetry()).isEmpty();
    }
}
