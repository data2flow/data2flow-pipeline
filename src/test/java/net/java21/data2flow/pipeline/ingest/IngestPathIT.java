package net.java21.data2flow.pipeline.ingest;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.pipeline.ingest.service.GatewayToucher;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 수집 경로 통합 시험(실제 PostgreSQL 18·RabbitMQ 3.13 Stream + core 대역): 처리 결과 상태·자동 등록·중복·품질·상태·공백 */
class IngestPathIT extends IntegrationTestSupport {

    private static final String TOPIC = "application/6a1c5b0e/device/%s/event/up";

    @Autowired
    private GatewayToucher gateways;

    private static String uplink(String devEui, String dedupId, String nsTime, int fCnt, String object) {
        return """
                {"deduplicationId":"%s","time":"%s","deviceInfo":{"devEui":"%s","deviceName":"dev-%s","tags":{"location":"실습실"}},
                 "fCnt":%d,"object":%s,"rxInfo":[{"gatewayId":"24e124fffef79304","rssi":-70,"snr":9.0,"nsTime":"%s"}]}
                """.formatted(dedupId, nsTime, devEui, devEui, fCnt, object, nsTime);
    }

    private RawEnvelope chirp(String devEui, String nsTime, int fCnt, String object) {
        String payload = uplink(devEui, UUID.randomUUID().toString(), nsTime, fCnt, object);
        return envelope(3, "MQTT_SUBSCRIBE", TOPIC.formatted(devEui), payload.getBytes(StandardCharsets.UTF_8),
                clock.instant());
    }

    private CanonicalTelemetry awaitTelemetry(int count) {
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() >= count);
        return telemetry.telemetry().get(count - 1);
    }

    @Test
    @DisplayName("[ING-01.02][AT-ING-01.4] TC-ING-014 깨진 JSON → DECODE_ERROR, 원문 그대로, dlq_items(DECODE) 1건, 발행 0")
    void decodeErrorGoesToDlq() {
        RawEnvelope envelope = envelope(3, TOPIC.formatted("24e124000000aaaa"), "{\"broken");

        publish(envelope);
        awaitRaw(envelope.messageId());

        assertThat(rawStatus(envelope.messageId())).isEqualTo("DECODE_ERROR");
        assertThat(jdbc.sql("SELECT error_code FROM data2flow_pipeline.raw_messages").query(String.class).single())
                .isEqualTo("ING_DECODE_FAILED");
        assertThat(jdbc.sql("SELECT payload FROM data2flow_pipeline.raw_messages").query(byte[].class).single())
                .isEqualTo(envelope.payload());
        assertThat(jdbc.sql("SELECT stage FROM data2flow_pipeline.dlq_items").query(String.class).list())
                .containsExactly("DECODE");
        assertThat(telemetry.telemetry()).isEmpty();
    }

    @Test
    @DisplayName("[ING-03.02][AT-ING-03.1] TC-ING-050 미등록 devEui → PENDING 자동 등록(core API-DEV-121), 측정값 저장·발행(PENDING)")
    void autoRegistersPending() {
        RawEnvelope envelope = chirp("24e1240000000001", "2026-10-03T00:00:00.100Z", 1, "{\"temperature\":21.5}");

        publish(envelope);
        CanonicalTelemetry t = awaitTelemetry(1);

        assertThat(t.deviceStatus()).isEqualTo(CanonicalTelemetry.DeviceStatus.PENDING);
        assertThat(CORE.findDevice(3, "24e1240000000001").get("status").asString()).isEqualTo("PENDING");
        JsonNode request = CORE.autoRegisterRequests().getFirst();
        assertThat(request.get("name").asString()).isEqualTo("dev-24e1240000000001");
        assertThat(request.get("sourceMeta").get("tags").get("location").asString()).isEqualTo("실습실");
        assertThat(request.get("metrics").get(0).asString()).isEqualTo("temperature");
        assertThat(rawStatus(envelope.messageId())).isEqualTo("OK");
    }

    @Test
    @DisplayName("[ING-03.02][AT-ING-03.2] TC-ING-050 거부 정책 소스는 UNKNOWN_DEVICE_REJECTED, 저장 0, 등록 요청 없음")
    void rejectPolicy() {
        CORE.source(7, 1, "chirpstack-v4", "REJECT", null);
        String payload = uplink("24e1240000000002", UUID.randomUUID().toString(), "2026-10-03T00:00:00Z", 1, "{\"t\":1}");
        RawEnvelope envelope = envelope(7, "MQTT_SUBSCRIBE", TOPIC.formatted("24e1240000000002"),
                payload.getBytes(StandardCharsets.UTF_8), clock.instant());

        publish(envelope);
        awaitRaw(envelope.messageId());

        assertThat(rawStatus(envelope.messageId())).isEqualTo("UNKNOWN_DEVICE_REJECTED");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry")).isZero();
        assertThat(CORE.autoRegisterRequests()).isEmpty();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.dlq_items")).isZero();
    }

    @Test
    @DisplayName("[ING-07.02][AT-ING-03.3] TC-ING-084 한 시간에 미등록 기기 5대(한도 3) → PENDING 3대, 거부 2건(ING_AUTO_REGISTER_QUOTA), 알람 1건")
    void autoRegisterQuota() {
        CORE.sourceLimit(3, 3);
        List<RawEnvelope> sent = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            RawEnvelope e = chirp("24e12400000001%02d".formatted(i), "2026-10-03T00:00:0%d.000Z".formatted(i), 1, "{\"t\":1}");
            sent.add(e);
            publish(e);
        }
        sent.forEach(e -> awaitRaw(e.messageId()));

        assertThat(count("SELECT count(*) FROM data2flow_pipeline.raw_messages WHERE status = 'OK'")).isEqualTo(3);
        assertThat(count("""
                SELECT count(*) FROM data2flow_pipeline.raw_messages
                 WHERE status = 'UNKNOWN_DEVICE_REJECTED' AND error_code = 'ING_AUTO_REGISTER_QUOTA'""")).isEqualTo(2);
        assertThat(CORE.autoRegisteredCount(3)).isEqualTo(3);
        await().atMost(TestStreams.timeout()).untilAsserted(() ->
                assertThat(events("ingest.alert.raised")).singleElement()
                        .satisfies(e -> assertThat(e.get("payload").get("code").asString()).isEqualTo("ING_AUTO_REGISTER_QUOTA")));
    }

    @Test
    @DisplayName("[ING-04.04][AT-ING-01.2] TC-ING-060 이중 ingress: 같은 업링크 2건(messageId 다름) → OK 1 + DUPLICATE 1, telemetry 중복 0, 발행 1")
    void dualIngressDuplicate() {
        CORE.device(21, 1, 3, "24e124136d151606", "ACTIVE", 1L, 31L, 60);
        String payload = uplink("24e124136d151606", "11111111-2222-3333-4444-555555555555", "2026-10-03T00:00:01Z", 7,
                "{\"temperature\":22.3}");
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        RawEnvelope a = envelope(3, "MQTT_SUBSCRIBE", TOPIC.formatted("24e124136d151606"), bytes, clock.instant());
        RawEnvelope b = new RawEnvelope(1, UUID.randomUUID(), 1, 3, "MQTT_SUBSCRIBE", a.topic(), bytes,
                clock.instant().plusMillis(30), "data2flow-ingress-1", a.dedupKey(), false, null);

        publish(a);
        publish(b);
        awaitRaw(a.messageId());
        awaitRaw(b.messageId());

        assertThat(List.of(rawStatus(a.messageId()), rawStatus(b.messageId()))).containsExactly("OK", "DUPLICATE");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry")).isEqualTo(1);
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).until(() -> telemetry.telemetry().size() == 1);
    }

    @Test
    @DisplayName("[ING-04.04][AT-ING-04.7] TC-ING-099 single-value(보고 주기 60초): 0·60·120초는 3행, 10초 뒤 같은 payload 재전송은 DUPLICATE")
    void valueOnlyDedup() {
        CORE.source(5, 1, "single-value", "AUTO_REGISTER", null);
        CORE.device(51, 1, 5, "esp-02", "ACTIVE", null, null, 60);
        List<RawEnvelope> sent = new ArrayList<>();
        for (int s : new int[]{0, 10, 60, 120}) {
            byte[] p = "22.8".getBytes(StandardCharsets.UTF_8);
            RawEnvelope e = new RawEnvelope(1, UUID.randomUUID(), 1, 5, "WEBHOOK", "sensors/esp-02/temperature", p,
                    clock.instant().plusSeconds(s), "data2flow-ingress-0", DedupKeys.detect(5, "sensors/esp-02/temperature", p),
                    false, null);
            sent.add(e);
            publish(e);
        }
        sent.forEach(e -> awaitRaw(e.messageId()));

        assertThat(sent.stream().map(e -> rawStatus(e.messageId())).toList())
                .containsExactly("OK", "DUPLICATE", "OK", "OK");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE device_id = 51")).isEqualTo(3);
        assertThat(jdbc.sql("SELECT dedup_key FROM data2flow_pipeline.raw_messages WHERE status = 'OK' LIMIT 1")
                .query(String.class).single()).startsWith("sha256b:");
        assertThat(jdbc.sql("SELECT DISTINCT quality FROM data2flow_pipeline.telemetry").query(Integer.class).list())
                .as("측정 시각이 없으면 수신 시각 + 품질 4").containsExactly(4);
    }

    @Test
    @DisplayName("[ING-05.01][AT-ING-01.1] TC-ING-063 같은 스트림 메시지를 다시 읽으면(커밋 뒤 오프셋 저장 전 장애) 기록은 그대로, 표준 메시지만 같은 messageId로 다시 발행")
    void replayRepublishes() {
        CORE.device(22, 1, 3, "24e124785c389818", "ACTIVE", 2L, 31L, 60);
        RawEnvelope envelope = chirp("24e124785c389818", "2026-10-03T00:00:02Z", 9, "{\"temperature\":22.8}");

        publish(envelope);
        CanonicalTelemetry first = awaitTelemetry(1);
        publish(envelope);
        CanonicalTelemetry second = awaitTelemetry(2);

        assertThat(second.messageId()).isEqualTo(first.messageId());
        assertThat(second.rawMessageId()).isEqualTo(first.rawMessageId());
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.raw_messages")).isEqualTo(1);
        assertThat(telemetry.received()).extracting(TestStreams.Received::partition).containsOnly(
                telemetry.received().getFirst().partition());
    }

    @Test
    @DisplayName("[ING-04.02][AT-ING-04.2] TC-ING-056 처음 보는 측정 키 pm4_0 → UNVERIFIED 등록 요청 1건, 품질 2로 저장")
    void unverifiedMetric() {
        CORE.device(23, 1, 3, "24e124126d152590", "ACTIVE", 3L, 31L, 60);
        RawEnvelope envelope = chirp("24e124126d152590", "2026-10-03T00:00:03Z", 1, "{\"pm4_0\":12.5,\"co2\":800}");

        publish(envelope);
        CanonicalTelemetry t = awaitTelemetry(1);

        assertThat(t.metric("pm4_0").quality()).isEqualTo(2);
        assertThat(t.metric("co2").quality()).isZero();
        assertThat(CORE.unverifiedRequests()).hasSize(1);
        assertThat(CORE.unverifiedRequests().getFirst().get("keys").get(0).get("key").asString()).isEqualTo("pm4_0");
    }

    @Test
    @DisplayName("[ING-04.01][AT-ING-04.1] TC-ING-055 유효 범위(-20~60) 밖 85는 버리지 않고 품질 1로 저장")
    void outOfRangeStored() {
        CORE.device(24, 1, 3, "24e124725d081175", "ACTIVE", 4L, 32L, 60);
        RawEnvelope envelope = chirp("24e124725d081175", "2026-10-03T00:00:04Z", 1, "{\"temperature\":85,\"humidity\":40}");

        publish(envelope);
        awaitTelemetry(1);

        assertThat(jdbc.sql("SELECT quality FROM data2flow_pipeline.telemetry WHERE metric_key = 'temperature'")
                .query(Integer.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT value FROM data2flow_pipeline.telemetry WHERE metric_key = 'temperature'")
                .query(Double.class).single()).isEqualTo(85);
    }

    @Test
    @DisplayName("[ING-05.02][AT-ING-01.1] TC-ING-066 순서가 뒤바뀐 3건: 최근값은 가장 최신 측정, 늦게 온 값은 최근값을 덮지 않음")
    void snapshotKeepsNewest() {
        CORE.device(25, 1, 3, "24e124128c067999", "ACTIVE", 5L, 31L, 60);
        publish(chirp("24e124128c067999", "2026-10-03T00:00:10Z", 2, "{\"temperature\":22.0,\"battery\":80}"));
        publish(chirp("24e124128c067999", "2026-10-03T00:00:30Z", 3, "{\"temperature\":23.0,\"battery\":79}"));
        publish(chirp("24e124128c067999", "2026-10-03T00:00:20Z", 4, "{\"temperature\":99.0,\"battery\":10}"));
        awaitTelemetry(3);

        assertThat(jdbc.sql("SELECT (latest->'temperature'->>'v')::float FROM data2flow_pipeline.device_state")
                .query(Double.class).single()).isEqualTo(23.0);
        assertThat(jdbc.sql("SELECT battery FROM data2flow_pipeline.device_state").query(Double.class).single())
                .isEqualTo(79.0);
        assertThat(jdbc.sql("SELECT last_measured_at FROM data2flow_pipeline.device_state")
                .query(java.sql.Timestamp.class).single().toInstant()).isEqualTo(Instant.parse("2026-10-03T00:00:30Z"));
        assertThat(jdbc.sql("SELECT rssi FROM data2flow_pipeline.device_state").query(Double.class).single()).isEqualTo(-70);
    }

    @Test
    @DisplayName("[ING-06.05][AT-ING-09.2] TC-ING-079 예상 주기 60초 기기가 3시간 공백 뒤 다시 들어오면 data_gaps(누락 추정 180) + ingest.gap.detected")
    void dataGap() {
        CORE.device(26, 1, 3, "24e124000000beef", "ACTIVE", null, null, 60);
        publish(chirp("24e124000000beef", "2026-10-03T00:00:00Z", 1, "{\"temperature\":20}"));
        awaitTelemetry(1);
        clock.advance(Duration.ofHours(3));
        publish(chirp("24e124000000beef", "2026-10-03T03:00:00Z", 2, "{\"temperature\":21}"));
        awaitTelemetry(2);

        assertThat(jdbc.sql("SELECT expected_count FROM data2flow_pipeline.data_gaps WHERE device_id = 26")
                .query(Integer.class).single()).isEqualTo(180);
        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(events("ingest.gap.detected")).singleElement()
                .satisfies(e -> assertThat(e.get("payload").get("estimatedMissing").asInt()).isEqualTo(180)));
    }

    @Test
    @DisplayName("[ING-03.01][AT-ING-03.4] TC-ING-047 설정 변경(DEVICE, data2flow.config)을 받으면 캐시를 지워 다음 업링크가 바뀐 공간으로 처리된다")
    void cacheInvalidation() {
        CORE.device(27, 1, 3, "24e1240000000027", "ACTIVE", 1L, 31L, 60);
        publish(chirp("24e1240000000027", "2026-10-03T00:00:00Z", 1, "{\"temperature\":20}"));
        assertThat(awaitTelemetry(1).spaceId()).isEqualTo(31);

        CORE.device(27, 1, 3, "24e1240000000027", "ACTIVE", 1L, 99L, 60);
        rabbit.convertAndSend(MessagingNames.EXCHANGE_CONFIG, "", CODEC.write(
                ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.DEVICE, 27, 2, 1, clock)));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            publish(chirp("24e1240000000027", "2026-10-03T00:00:01Z", 2, "{\"temperature\":21}"));
            assertThat(telemetry.telemetry().getLast().spaceId()).isEqualTo(99);
        });
    }

    @Test
    @DisplayName("[ING-02.06][AT-ING-02.5] TC-ING-045 WS301 문: magnet_status \"open\" → 별칭 door = 1 저장")
    void doorStatus() {
        CORE.device(28, 1, 3, "24e124538b223344", "ACTIVE", 7L, 31L, 60);
        publish(chirp("24e124538b223344", "2026-10-03T00:00:00Z", 1, "{\"magnet_status\":\"open\",\"battery\":91}"));

        CanonicalTelemetry t = awaitTelemetry(1);

        assertThat(t.metric("door").value()).isEqualTo(1);
        assertThat(t.metric("magnet_status")).isNull();
    }

    @Test
    @DisplayName("[ING-02.06][AT-ING-02.5] TC-ING-044 매핑할 수 없는 글자 값 → INVALID + ING_VALUE_NOT_NUMERIC")
    void notNumericIsInvalid() {
        CORE.device(29, 1, 3, "24e1240000000029", "ACTIVE", null, null, 60);
        RawEnvelope envelope = chirp("24e1240000000029", "2026-10-03T00:00:00Z", 1, "{\"magnet_status\":\"half\"}");

        publish(envelope);
        awaitRaw(envelope.messageId());

        assertThat(rawStatus(envelope.messageId())).isEqualTo("INVALID");
        assertThat(jdbc.sql("SELECT error_code FROM data2flow_pipeline.raw_messages").query(String.class).single())
                .isEqualTo("ING_VALUE_NOT_NUMERIC");
    }

    @Test
    @DisplayName("[ING-07.01][AT-ING-10.1] TC-ING-081 측정 항목 101개 → INVALID(telemetry 0), 정확히 100개 → 100행")
    void metricLimits() {
        CORE.source(6, 1, "generic-json", "AUTO_REGISTER", null);
        CORE.sourceDecoderConfig(6, CODEC.mapper().readTree(
                "{\"deviceIdFrom\":\"topic[1]\",\"items\":{\"path\":\"$.v[*]\",\"keyFrom\":\"$.k\",\"valueFrom\":\"$.x\"}}"));
        CORE.device(61, 1, 6, "big", "ACTIVE", null, null, 60);
        for (int i = 0; i < 101; i++) {
            CORE.metric("m" + i, null, null, null);
        }
        RawEnvelope ok = envelope(6, "devices/big/telemetry", items(100, "2026-10-03T00:00:00Z"));
        RawEnvelope tooMany = envelope(6, "devices/big/telemetry", items(101, "2026-10-03T00:00:01Z"));

        publish(ok);
        publish(tooMany);
        awaitRaw(ok.messageId());
        awaitRaw(tooMany.messageId());

        assertThat(rawStatus(ok.messageId())).isEqualTo("OK");
        assertThat(rawStatus(tooMany.messageId())).isEqualTo("INVALID");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry")).isEqualTo(100);
        assertThat(jdbc.sql("SELECT error_detail->>'message' FROM data2flow_pipeline.raw_messages WHERE status = 'INVALID'")
                .query(String.class).single()).isEqualTo("101/100");
    }

    private static String items(int n, String ts) {
        StringBuilder sb = new StringBuilder("{\"ts\":\"" + ts + "\",\"v\":[");
        for (int i = 0; i < n; i++) {
            sb.append(i == 0 ? "" : ",").append("{\"k\":\"m").append(i).append("\",\"x\":").append(i).append('}');
        }
        return sb.append("]}").toString();
    }

    @Test
    @DisplayName("[ING-02.05][AT-ING-02.4] 측정 시각이 수신 +10분이면 수신 시각으로 바꾸고 품질 4, 2시간 전 측정은 late(flags 1)·표준 메시지 late=true")
    void timeCorrectionAndLate() {
        CORE.device(30, 1, 3, "24e1240000000030", "ACTIVE", null, null, 60);
        publish(chirp("24e1240000000030", "2026-10-03T00:10:00Z", 1, "{\"temperature\":20}"));
        CanonicalTelemetry future = awaitTelemetry(1);
        publish(chirp("24e1240000000030", "2026-10-02T22:00:00Z", 2, "{\"temperature\":19}"));
        CanonicalTelemetry late = awaitTelemetry(2);

        assertThat(future.measuredAt()).isEqualTo(MutableClockInstant.T0);
        assertThat(future.metric("temperature").quality()).isEqualTo(4);
        assertThat(late.late()).isTrue();
        assertThat(jdbc.sql("SELECT flags FROM data2flow_pipeline.telemetry WHERE time = '2026-10-02T22:00:00Z'")
                .query(Integer.class).single()).isEqualTo(1);
    }

    @Test
    @DisplayName("[DEV-05.01] 업링크 rxInfo 게이트웨이를 모아 core API-DEV-125로 보낸다")
    void gatewayTouch() {
        CORE.device(31, 1, 3, "24e1240000000031", "ACTIVE", null, null, 60);
        publish(chirp("24e1240000000031", "2026-10-03T00:00:00Z", 1, "{\"temperature\":20}"));
        awaitTelemetry(1);

        gateways.flush();

        assertThat(CORE.gatewayTouches()).singleElement().satisfies(t ->
                assertThat(t.get("items").get(0).get("gatewayEui").asString()).isEqualTo("24e124fffef79304"));
    }

    /** 테스트 시계 기준 시각 */
    private static final class MutableClockInstant {
        static final Instant T0 = net.java21.data2flow.pipeline.support.MutableClock.T0;
    }
}
