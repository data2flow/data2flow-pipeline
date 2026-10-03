package net.java21.data2flow.pipeline.ingest;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageSchemas;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.test.message.MessageFixtures;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * ING-01.01 TC-ING-003 · ING-02.01 TC-ING-033 · ING-05.01 · TSD-01.01·01.02: data2flow.raw 1건 → raw_messages 1행(OK) + telemetry +
 * link_qualities + device_state + data2flow.telemetry 1건(스키마 통과). 256KB 초과는 앞 4KB만 보관하고 INVALID.
 */
class RawMessageStoreIT extends IntegrationTestSupport {

    @Test
    @DisplayName("[ING-01.01][AT-ING-01.1] TC-ING-003 ChirpStack 업링크가 원본·측정값·통신 품질·기기 상태로 저장되고 표준 메시지가 발행된다")
    void storesAndPublishes() {
        CORE.device(17, 1, 3, "24e124743d012436", "ACTIVE", 6L, 31L, 60);
        RawEnvelope envelope = MessageFixtures.rawEnvelope("chirpstack-ws302-uplink");

        publish(envelope);

        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 1);
        assertThat(rawStatus(envelope.messageId())).isEqualTo("OK");
        CanonicalTelemetry t = telemetry.telemetry().getFirst();
        MessageSchemas.assertValid(t);
        assertThat(t.deviceId()).isEqualTo(17);
        assertThat(t.externalId()).isEqualTo("24e124743d012436");
        assertThat(t.measuredAt()).isEqualTo(Instant.parse("2026-10-03T02:40:09.140Z"));
        assertThat(t.metric("LAeq").value()).isEqualTo(30.5);
        assertThat(t.metric("LAeq").unit()).isEqualTo("dB");
        assertThat(t.metric("battery").value()).isEqualTo(55);
        assertThat(t.link().frameCounter()).isEqualTo(23518);
        assertThat(t.link().rssi()).isEqualTo(-33);
        assertThat(t.link().gateways()).hasSize(2);
        assertThat(t.meta().tags()).containsEntry("location", "실습실");
        assertThat(t.meta().decoder().key()).isEqualTo("chirpstack-v4");
        assertThat(t.routingKey()).isEqualTo("17");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE device_id = 17")).isEqualTo(4);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.link_qualities WHERE device_id = 17")).isEqualTo(2);
        assertThat(jdbc.sql("SELECT payload FROM data2flow_pipeline.raw_messages").query(byte[].class).single())
                .isEqualTo(envelope.payload());
        assertThat(jdbc.sql("SELECT source_type FROM data2flow_pipeline.raw_messages").query(String.class).single())
                .isEqualTo("MQTT_SUBSCRIBE");
        assertThat(jdbc.sql("SELECT (latest->'LAeq'->>'v')::float FROM data2flow_pipeline.device_state WHERE device_id = 17")
                .query(Double.class).single()).isEqualTo(30.5);
        assertThat(jdbc.sql("SELECT connectivity FROM data2flow_pipeline.device_state WHERE device_id = 17")
                .query(String.class).single()).isEqualTo("ONLINE");
        assertThat(t.rawMessageId()).isEqualTo(jdbc.sql("SELECT id FROM data2flow_pipeline.raw_messages")
                .query(Long.class).single());
        assertThat(events("device.connectivity.changed")).hasSize(1);
    }

    @Test
    @DisplayName("[ING-01.01][AT-ING-01.1] TC-ING-003 payload 256KB 초과는 앞 4KB만 보관하고 INVALID + ING_LIMIT_PAYLOAD_EXCEEDED")
    void oversizedPayload() {
        byte[] payload = new byte[262_145];
        Arrays.fill(payload, (byte) 'x');
        RawEnvelope envelope = envelope(3, "MQTT_SUBSCRIBE", "application/a/device/24e124743d012436/event/up", payload,
                clock.instant());

        publish(envelope);
        awaitRaw(envelope.messageId());

        assertThat(rawStatus(envelope.messageId())).isEqualTo("INVALID");
        assertThat(jdbc.sql("SELECT error_code FROM data2flow_pipeline.raw_messages").query(String.class).single())
                .isEqualTo("ING_LIMIT_PAYLOAD_EXCEEDED");
        assertThat(jdbc.sql("SELECT octet_length(payload) FROM data2flow_pipeline.raw_messages").query(Integer.class).single())
                .isEqualTo(4096);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry")).isZero();
        assertThat(telemetry.telemetry()).isEmpty();
        assertThat(List.of()).isEmpty();
    }
}
