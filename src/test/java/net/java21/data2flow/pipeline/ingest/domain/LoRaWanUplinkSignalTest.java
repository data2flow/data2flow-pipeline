package net.java21.data2flow.pipeline.ingest.domain;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** EVT-ACT-07 LoRaWAN 업링크 신호 판정 규칙(ACT-07.02, ADR-049 남은 것 ②) */
class LoRaWanUplinkSignalTest {

    private static final Instant RECEIVED = Instant.parse("2026-10-03T00:00:01Z");
    private static final String UP = "application/6a1c5b0e/device/70b3d57ed0000001/event/up";

    private static CanonicalTelemetry telemetry(CanonicalTelemetry.DeviceStatus status, boolean virtual, String decoder, Long fCnt) {
        return CanonicalTelemetry.builder()
                .messageId(UUID.fromString("5d0c7c1a-9a51-4c43-bf0e-0d6f2d6c4a12")).organizationId(1).sourceId(3)
                .externalId("70b3d57ed0000001").deviceId(31).deviceStatus(status)
                .measuredAt(RECEIVED.minusMillis(400)).receivedAt(RECEIVED).virtual(virtual)
                .link(fCnt == null ? null : new CanonicalTelemetry.Link(-70.0, 9.0, fCnt, null))
                .meta(new CanonicalTelemetry.Meta(null, decoder == null ? null : new CanonicalTelemetry.DecoderRef(decoder, "1"), null))
                .rawMessageId(9).build();
    }

    @Test
    @DisplayName("[ACT-07.02][TC-ACT-128] 승인된 실제 기기의 ChirpStack 업링크 → 빈 capabilities, version = fCnt, reportedAt = 수신 시각, virtual=false")
    void signalFromChirpStackUplink() {
        DeviceStateReported s = LoRaWanUplinkSignal.from(telemetry(CanonicalTelemetry.DeviceStatus.ACTIVE, false, "chirpstack-v4", 42L), UP)
                .orElseThrow();

        assertThat(s.deviceId()).isEqualTo(31);
        assertThat(s.version()).isEqualTo(42);
        assertThat(s.capabilities()).isEmpty();
        assertThat(s.reportedAt()).isEqualTo(RECEIVED);
        assertThat(s.virtual()).isFalse();
    }

    @Test
    @DisplayName("[ACT-07.02][TC-ACT-128] fCnt가 없으면 version = 수신 시각 밀리초, messageId는 표준 메시지에서 정해진다(다시 내도 같음)")
    void versionFallbackAndStableMessageId() {
        CanonicalTelemetry t = telemetry(CanonicalTelemetry.DeviceStatus.ACTIVE, false, "chirpstack-v4", null);

        assertThat(LoRaWanUplinkSignal.from(t, null).orElseThrow().version()).isEqualTo(RECEIVED.toEpochMilli());
        assertThat(LoRaWanUplinkSignal.messageId(t)).isEqualTo(LoRaWanUplinkSignal.messageId(t)).isNotEqualTo(t.messageId());
    }

    @ParameterizedTest(name = "[{index}] {0}·virtual={1}·{2}·{3} → {4}")
    @DisplayName("[ACT-07.02][TC-ACT-128] 대상 판정: ACTIVE 실제 기기의 LoRaWAN 업링크만(ChirpStack 토픽이면 event/up만, 아니면 디코더 chirpstack-v4)")
    @CsvSource({
            "ACTIVE,   false, chirpstack-v4, application/1/device/a/event/up,     true",
            "ACTIVE,   false, script,        application/1/device/a/event/up,     true",
            "ACTIVE,   false, chirpstack-v4, application/1/device/a/event/status, false",
            "ACTIVE,   false, chirpstack-v4, application/1/device/a/event/join,   false",
            "ACTIVE,   false, chirpstack-v4, /webhook/chirpstack,                 true",
            "ACTIVE,   false, generic-json,  devices/a/up,                        false",
            "PENDING,  false, chirpstack-v4, application/1/device/a/event/up,     false",
            "INACTIVE, false, chirpstack-v4, application/1/device/a/event/up,     false",
            "ACTIVE,   true,  chirpstack-v4, application/1/device/a/event/up,     false",
    })
    void eligibility(CanonicalTelemetry.DeviceStatus status, boolean virtual, String decoder, String topic, boolean expected) {
        assertThat(LoRaWanUplinkSignal.from(telemetry(status, virtual, decoder, 1L), topic).isPresent()).isEqualTo(expected);
    }

    @Test
    @DisplayName("[ACT-07.02][TC-ACT-128] 표준 메시지가 없으면 신호 없음")
    void nullTelemetry() {
        assertThat(LoRaWanUplinkSignal.from(null, UP)).isEmpty();
    }
}
