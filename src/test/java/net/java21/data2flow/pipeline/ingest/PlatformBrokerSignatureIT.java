package net.java21.data2flow.pipeline.ingest;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SignatureStatus;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRepository;
import net.java21.data2flow.pipeline.ingest.service.IngestProcessor;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DSC-03.03·03.05 TC-DSC-322(pipeline 부분): 플랫폼 브로커 기기 메시지의 서명 결과(RawEnvelope.signatureStatus, ingress가 채움)에 따라
 * 승인 대기 기기는 quality 2로 격리하고, 승인된 기기는 서명이 맞는 메시지만 받는다. 서명 불일치는 원본만 INVALID로 남긴다.
 */
class PlatformBrokerSignatureIT extends IntegrationTestSupport {

    private static final long SOURCE = 19;

    @Autowired
    private MeterRegistry meters;
    @Autowired
    private IngestProcessor processor;
    @Autowired
    private RawMessageRepository raws;

    @BeforeEach
    void platformSource() {
        CORE.source(SOURCE, 1, "generic-json", "AUTO_REGISTER", null);
        CORE.sourceDecoderConfig(SOURCE, CODEC.mapper().readTree(
                "{\"deviceIdFrom\":\"topic[1]\",\"timeFrom\":\"$.ts\",\"metrics\":[{\"path\":\"$.co2\",\"key\":\"co2\"}]}"));
        CORE.metric("co2", "ppm", 0.0, 10000.0);
    }

    private RawEnvelope message(String deviceKey, int co2, String signatureStatus) {
        byte[] body = ("{\"ts\":\"" + clock.instant() + "\",\"co2\":" + co2 + "}").getBytes(StandardCharsets.UTF_8);
        RawEnvelope e = envelope(SOURCE, SourceTypes.PLATFORM_BROKER, "devices/" + deviceKey + "/telemetry", body,
                clock.instant());
        return signatureStatus == null ? e : e.withSignature(signatureStatus, body);
    }

    private double rejectedCount() {
        Counter c = meters.find("data2flow.ingest.signature.rejected").tag("source_id", Long.toString(SOURCE)).counter();
        return c == null ? 0 : c.count();
    }

    private String errorCode(RawEnvelope e) {
        return jdbc.sql("SELECT error_code FROM data2flow_pipeline.raw_messages WHERE message_id = :id")
                .param("id", e.messageId()).query(String.class).single();
    }

    private CanonicalTelemetry awaitTelemetry(int count) {
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() >= count);
        return telemetry.telemetry().get(count - 1);
    }

    @Test
    @DisplayName("[DSC-03.05][AT-DSC-07.4] TC-DSC-322 미등록 deviceKey 첫 메시지(UNSIGNED) → PENDING 자동 등록, quality 2 격리 저장")
    void pendingIsQuarantined() {
        RawEnvelope first = message("esp32-co2-02", 812, SignatureStatus.UNSIGNED);

        publish(first);
        CanonicalTelemetry t = awaitTelemetry(1);

        assertThat(rawStatus(first.messageId())).isEqualTo("OK");
        assertThat(t.deviceStatus()).isEqualTo(CanonicalTelemetry.DeviceStatus.PENDING);
        assertThat(t.metric("co2").quality()).isEqualTo(2);
        assertThat(CORE.findDevice(SOURCE, "esp32-co2-02").get("status").asString()).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("[DSC-03.05] TC-DSC-322 승인 대기 기기는 서명이 맞아도(VERIFIED) quality 2 격리를 유지한다")
    void pendingVerifiedStillQuarantined() {
        CORE.device(191, 1, SOURCE, "esp32-pending", "PENDING", null, null, 60);
        publish(message("esp32-pending", 700, SignatureStatus.VERIFIED));

        assertThat(awaitTelemetry(1).metric("co2").quality()).isEqualTo(2);
    }

    @Test
    @DisplayName("[DSC-03.05][AT-DSC-07.5] TC-DSC-322 승인된 기기: 서명이 맞으면 quality 0, 서명 없음·서명 결과 없음은 DEVICE_SIGNATURE_INVALID 거부·지표 증가")
    void approvedRequiresVerified() {
        CORE.device(192, 1, SOURCE, "esp32-co2-03", "ACTIVE", null, null, 60);
        double before = rejectedCount();
        RawEnvelope verified = message("esp32-co2-03", 820, SignatureStatus.VERIFIED);
        RawEnvelope unsigned = message("esp32-co2-03", 821, SignatureStatus.UNSIGNED);
        RawEnvelope missing = message("esp32-co2-03", 822, null);

        publish(verified);
        publish(unsigned);
        publish(missing);
        awaitRaw(verified.messageId());
        awaitRaw(unsigned.messageId());
        awaitRaw(missing.messageId());

        assertThat(rawStatus(verified.messageId())).isEqualTo("OK");
        assertThat(awaitTelemetry(1).metric("co2").quality()).isZero();
        for (RawEnvelope rejected : new RawEnvelope[]{unsigned, missing}) {
            assertThat(rawStatus(rejected.messageId())).isEqualTo("INVALID");
            assertThat(errorCode(rejected)).isEqualTo(SignatureStatus.ERROR_CODE);
        }
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry")).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.dlq_items")).isZero();
        assertThat(rejectedCount() - before).isEqualTo(2.0);
        assertThat(telemetry.telemetry()).hasSize(1);
    }

    @Test
    @DisplayName("[DSC-03.03][AT-DSC-07.1] TC-DSC-114 ingress가 INVALID로 판정한 메시지 → 원본만 INVALID(외부 ID는 토픽에서), 자동 등록 0, 재처리해도 그대로")
    void invalidIsRejectedAndNotReprocessable() {
        double before = rejectedCount();
        RawEnvelope forged = message("esp32-co2-04", 999, SignatureStatus.INVALID);

        publish(forged);
        awaitRaw(forged.messageId());

        assertThat(rawStatus(forged.messageId())).isEqualTo("INVALID");
        assertThat(errorCode(forged)).isEqualTo(SignatureStatus.ERROR_CODE);
        assertThat(jdbc.sql("SELECT external_id FROM data2flow_pipeline.raw_messages WHERE message_id = :id")
                .param("id", forged.messageId()).query(String.class).single()).isEqualTo("esp32-co2-04");
        assertThat(CORE.autoRegisterRequests()).isEmpty();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry")).isZero();
        assertThat(rejectedCount() - before).isEqualTo(1.0);

        long rawId = jdbc.sql("SELECT id FROM data2flow_pipeline.raw_messages WHERE message_id = :id")
                .param("id", forged.messageId()).query(Long.class).single();
        IngestProcessor.Outcome outcome = processor.reprocess(raws.findById(1, rawId).orElseThrow());

        assertThat(outcome.status()).isEqualTo(RawMessageStatus.INVALID);
        assertThat(outcome.errorCode()).isEqualTo(SignatureStatus.ERROR_CODE);
        assertThat(CORE.autoRegisterRequests()).isEmpty();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry")).isZero();
    }

    @Test
    @DisplayName("[ING-01.04][DSC-03.05] 재처리: 원본에 보관한 서명 판정이 VERIFIED면 quality 0을 유지하고(ADR-042), 판정이 없는(M5 이전) 원본은 거부하지 않고 quality 2(미검증)")
    void reprocessKeepsUnverified() {
        CORE.device(193, 1, SOURCE, "esp32-co2-05", "ACTIVE", null, null, 60);
        RawEnvelope verified = message("esp32-co2-05", 640, SignatureStatus.VERIFIED);
        publish(verified);
        awaitTelemetry(1);
        long rawId = jdbc.sql("SELECT id FROM data2flow_pipeline.raw_messages WHERE message_id = :id")
                .param("id", verified.messageId()).query(Long.class).single();

        assertThat(jdbc.sql("SELECT signature_status FROM data2flow_pipeline.raw_messages WHERE id = :id").param("id", rawId)
                .query(String.class).single()).isEqualTo("VERIFIED");

        IngestProcessor.Outcome outcome = processor.reprocess(raws.findById(1, rawId).orElseThrow());

        assertThat(outcome.status()).isEqualTo(RawMessageStatus.OK);
        assertThat(jdbc.sql("SELECT max(quality) FROM data2flow_pipeline.telemetry WHERE device_id = 193")
                .query(Integer.class).single()).isZero();

        jdbc.sql("UPDATE data2flow_pipeline.raw_messages SET signature_status = NULL WHERE id = :id").param("id", rawId).update();
        IngestProcessor.Outcome legacy = processor.reprocess(raws.findById(1, rawId).orElseThrow());

        assertThat(legacy.status()).isNotEqualTo(RawMessageStatus.INVALID);
        assertThat(jdbc.sql("SELECT max(quality) FROM data2flow_pipeline.telemetry WHERE device_id = 193")
                .query(Integer.class).single()).isEqualTo(2);
    }

    @Test
    @DisplayName("[DSC-03.05] 플랫폼 브로커가 아닌 소스는 서명 결과와 무관하게 처리한다")
    void otherSourcesUnaffected() {
        CORE.device(194, 1, SOURCE, "plain-01", "ACTIVE", null, null, 60);
        RawEnvelope plain = envelope(SOURCE, SourceTypes.MQTT_SUBSCRIBE, "devices/plain-01/telemetry",
                ("{\"ts\":\"" + clock.instant() + "\",\"co2\":500}").getBytes(StandardCharsets.UTF_8), clock.instant());

        publish(plain);

        assertThat(awaitTelemetry(1).metric("co2").quality()).isZero();
        assertThat(rawStatus(plain.messageId())).isEqualTo("OK");
    }
}
