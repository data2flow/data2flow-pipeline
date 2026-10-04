package net.java21.data2flow.pipeline.ingest;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.pipeline.ingest.domain.LoRaWanUplinkSignal;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * LoRaWAN 업링크 신호(EVT-ACT-07 {@code device.state.reported}, ACT-07.02 AT-ACT-07.5, ADR-049 남은 것 ②): 승인된 실제 기기의 ChirpStack
 * 업링크를 처리하면 action이 Class A 대기 다운링크를 보내도록 빈 상태 보고를 낸다. 실제 PostgreSQL 18·RabbitMQ 3.13 Stream + core 대역.
 */
class LoRaWanUplinkSignalIT extends IntegrationTestSupport {

    private static final String TOPIC = "application/6a1c5b0e/device/%s/event/up";

    private static byte[] uplink(String devEui, String nsTime, int fCnt) {
        return """
                {"deduplicationId":"%s","time":"%s","deviceInfo":{"devEui":"%s","deviceName":"valve"},
                 "fCnt":%d,"object":{"temperature":21.5},"rxInfo":[{"gatewayId":"24e124fffef79304","rssi":-70,"snr":9.0,"nsTime":"%s"}]}
                """.formatted(UUID.randomUUID(), nsTime, devEui, fCnt, nsTime).getBytes(StandardCharsets.UTF_8);
    }

    private RawEnvelope chirp(String devEui, int fCnt) {
        return envelope(3, "MQTT_SUBSCRIBE", TOPIC.formatted(devEui), uplink(devEui, "2026-10-03T00:00:00.100Z", fCnt), clock.instant());
    }

    @Test
    @DisplayName("[ACT-07.02][AT-ACT-07.5][TC-ACT-126] 승인된 LoRaWAN 기기 업링크(fCnt 42) → device.state.reported 1건(빈 capabilities, version 42, 실제 기기, 표준 메시지에서 정한 messageId)")
    void activeLoRaWanDeviceEmitsUplinkSignal() {
        CORE.device(501, 1, 3, "70b3d57ed0000501", "ACTIVE", 1L, 31L, 600);
        RawEnvelope envelope = chirp("70b3d57ed0000501", 42);

        publish(envelope);

        await().atMost(TestStreams.timeout()).until(() -> !events("device.state.reported").isEmpty());
        CanonicalTelemetry t = telemetry.telemetry().getFirst();
        JsonNode e = events("device.state.reported").getFirst();
        assertThat(e.get("organizationId").asLong()).isEqualTo(1);
        assertThat(e.get("messageId").asString()).isEqualTo(LoRaWanUplinkSignal.messageId(t).toString());
        JsonNode p = e.get("payload");
        assertThat(p.get("deviceId").asLong()).isEqualTo(501);
        assertThat(p.get("version").asLong()).isEqualTo(42);
        assertThat(p.get("capabilities").isEmpty()).isTrue();
        assertThat(p.get("virtual").asBoolean()).isFalse();
        assertThat(p.get("reportedAt").asString()).isEqualTo(envelope.receivedAt().toString());
    }

    @Test
    @DisplayName("[ACT-07.02][AT-ACT-07.5][TC-ACT-126] 같은 업링크가 두 ingress로 와도(DUPLICATE) 신호는 1건")
    void duplicateUplinkEmitsOnce() {
        CORE.device(502, 1, 3, "70b3d57ed0000502", "ACTIVE", 1L, 31L, 600);
        byte[] bytes = uplink("70b3d57ed0000502", "2026-10-03T00:00:01Z", 7);
        RawEnvelope a = envelope(3, "MQTT_SUBSCRIBE", TOPIC.formatted("70b3d57ed0000502"), bytes, clock.instant());
        RawEnvelope b = envelope(3, "MQTT_SUBSCRIBE", a.topic(), bytes, clock.instant().plusMillis(30));

        publish(a);
        publish(b);
        awaitRaw(a.messageId());
        awaitRaw(b.messageId());
        awaitIdle();

        assertThat(events("device.state.reported")).hasSize(1);
    }

    @Test
    @DisplayName("[ACT-07.02][AT-ACT-07.5][TC-ACT-126] 같은 스트림 메시지를 다시 읽으면(커밋 뒤 오프셋 저장 전 장애) 신호도 같은 messageId로 다시 낸다(최소 1회, action은 멱등)")
    void replayRepublishesSameSignal() {
        CORE.device(506, 1, 3, "70b3d57ed0000506", "ACTIVE", 1L, 31L, 600);
        RawEnvelope envelope = chirp("70b3d57ed0000506", 9);

        publish(envelope);
        await().atMost(TestStreams.timeout()).until(() -> events("device.state.reported").size() == 1);
        publish(envelope);
        await().atMost(TestStreams.timeout()).until(() -> events("device.state.reported").size() == 2);

        assertThat(events("device.state.reported")).extracting(e -> e.get("messageId").asString()).containsOnly(
                events("device.state.reported").getFirst().get("messageId").asString());
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.raw_messages")).isEqualTo(1);
    }

    @Test
    @DisplayName("[ACT-07.02][TC-ACT-126] 승인 대기(PENDING)·가상 기기·LoRaWAN이 아닌 소스(generic-json)는 신호를 내지 않는다(텔레메트리는 그대로 발행)")
    void otherUplinksEmitNothing() {
        // 처음 보는 기기 → PENDING 자동 등록(제어 불가)
        RawEnvelope pending = chirp("70b3d57ed0000503", 1);
        // 가상 기기(시뮬레이터가 직접 상태를 보고)
        CORE.device(504, 1, 3, "70b3d57ed0000504", "ACTIVE", 1L, 31L, 600);
        RawEnvelope base = chirp("70b3d57ed0000504", 2);
        RawEnvelope virtual = new RawEnvelope(base.v(), base.messageId(), base.organizationId(), base.sourceId(), base.sourceType(),
                base.topic(), base.payload(), base.receivedAt(), base.ingressInstance(), base.dedupKey(), true, null);
        // MQTT JSON 소스의 승인된 기기
        CORE.source(55, 1, "generic-json", "AUTO_REGISTER", null);
        CORE.sourceDecoderConfig(55, CODEC.mapper().readTree("""
                {"deviceIdFrom":"topic[1]","metrics":[{"path":"$.t","key":"temperature"}]}"""));
        CORE.device(505, 1, 55, "room-505", "ACTIVE", null, null, 60);
        RawEnvelope json = envelope(55, "MQTT_SUBSCRIBE", "devices/room-505/up", "{\"t\":20.5}".getBytes(StandardCharsets.UTF_8),
                clock.instant());

        publish(pending);
        publish(virtual);
        publish(json);
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() >= 3);
        awaitIdle();

        assertThat(events("device.state.reported")).isEmpty();
    }
}
