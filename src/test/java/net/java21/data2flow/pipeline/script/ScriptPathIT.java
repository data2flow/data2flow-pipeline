package net.java21.data2flow.pipeline.script;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 스크립트가 운영 경로에 연결된 통합 시험(SCR-01·02·03): DECODE·TRANSFORM·실패 정책·시간 초과·파생 항목·라이브 리로드·테스트 실행 */
class ScriptPathIT extends IntegrationTestSupport {

    @org.springframework.beans.factory.annotation.Autowired
    private net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry scripts;

    /** 대역에 넣은 번들을 바로 적재(배포 이벤트 대신) */
    private void deploy() {
        scripts.reload(1);
    }

    private static final String TOPIC = "application/a/device/%s/event/up";

    private RawEnvelope chirp(String devEui, int fCnt, String object) {
        String payload = """
                {"deduplicationId":"%s","deviceInfo":{"devEui":"%s"},"fCnt":%d,"object":%s,
                 "rxInfo":[{"gatewayId":"g1","rssi":-70,"snr":9.0,"nsTime":"2026-10-03T00:00:%02dZ"}]}
                """.formatted(UUID.randomUUID(), devEui, fCnt, object, fCnt % 60);
        return envelope(3, "MQTT_SUBSCRIBE", TOPIC.formatted(devEui), payload.getBytes(StandardCharsets.UTF_8),
                clock.instant());
    }

    private CanonicalTelemetry awaitTelemetry(int count) {
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() >= count);
        return telemetry.telemetry().get(count - 1);
    }

    private JsonNode trace(UUID messageId) {
        return CODEC.mapper().readTree(jdbc.sql("SELECT processing_trace::text FROM data2flow_pipeline.raw_messages WHERE message_id = :id")
                .param("id", messageId).query(String.class).single());
    }

    @Test
    @DisplayName("[SCR-01.01][AT-SCR-05.1] TC-SCR-003 바이트 센서(base64 AXVoAQJnBgE=)를 ctx.util.bytes로 푸는 DECODE 스크립트 → 저장, trace decoder=script:{id}@v1")
    void decodeScript() {
        CORE.source(9, 1, "script", "AUTO_REGISTER", 42L);
        CORE.script(1, 42, "DECODE", 420, 1, """
                function decode(input, ctx) {
                  const b = ctx.util.bytes.fromBase64(input.payloadBase64);
                  return {externalId: input.topic.split('/')[1],
                          metrics: [{key: 'temperature', value: ctx.util.bytes.readInt16LE(b, 6) / 10},
                                    {key: 'humidity', value: b[2] / 2}]};
                }
                """, "FAIL_OPEN", "SOURCE", 9, null);
        CORE.device(91, 1, 9, "byte-01", "ACTIVE", null, null, 60);
        deploy();
        byte[] payload = Base64.getDecoder().decode("AXVoAQJnBgE=");
        RawEnvelope envelope = envelope(9, "WEBHOOK", "milesight/byte-01/up", payload, clock.instant());

        publish(envelope);
        CanonicalTelemetry t = awaitTelemetry(1);

        assertThat(t.metric("temperature").value()).isEqualTo(26.2);
        assertThat(t.metric("humidity").value()).isEqualTo(52);
        assertThat(t.meta().decoder().key()).isEqualTo("script:42@v1");
        assertThat(trace(envelope.messageId()).get("decoder").get("key").asString()).isEqualTo("script:42@v1");
    }

    @Test
    @DisplayName("[SCR-01.02][AT-SCR-04.1] TC-SCR-005 모델(+0.5) → 기기(소수 1자리 반올림): 22.04 → 22.5, trace에 두 스크립트 순서대로")
    void transformChain() {
        CORE.device(101, 1, 3, "24e1240000000101", "ACTIVE", 6L, 31L, 60);
        CORE.script(1, 1, "TRANSFORM", 10, 1, """
                function transform(msg, ctx) {
                  const t = ctx.util.metric(msg, 'temperature');
                  t.value = t.value + ctx.config.offset;
                  return msg;
                }
                """, "FAIL_OPEN", "MODEL", 6, CODEC.mapper().readTree("{\"offset\":0.5}"));
        CORE.script(1, 2, "TRANSFORM", 20, 3, """
                function transform(msg, ctx) {
                  const t = ctx.util.metric(msg, 'temperature');
                  t.value = ctx.util.round(t.value * 1.0, 1);
                  return msg;
                }
                """, "FAIL_OPEN", "DEVICE", 101, null);
        deploy();
        RawEnvelope envelope = chirp("24e1240000000101", 1, "{\"temperature\":22.04}");

        publish(envelope);
        CanonicalTelemetry t = awaitTelemetry(1);

        assertThat(t.metric("temperature").value()).isEqualTo(22.5);
        assertThat(t.meta().scripts()).extracting(CanonicalTelemetry.ScriptRef::id).containsExactly(1L, 2L);
        assertThat(t.meta().scripts()).extracting(CanonicalTelemetry.ScriptRef::version).containsExactly(1, 3);
    }

    @Test
    @DisplayName("[SCR-01.02][AT-SCR-04.1] TC-SCR-008·TC-ING-064 TRANSFORM이 null → telemetry 0행, 원본 OK·dropped=true, 발행 0")
    void transformDrop() {
        CORE.device(102, 1, 3, "24e1240000000102", "ACTIVE", 7L, 31L, 60);
        CORE.script(1, 3, "TRANSFORM", 30, 1, "function transform(msg, ctx) { return null; }", "FAIL_OPEN", "MODEL", 7, null);
        deploy();
        RawEnvelope envelope = chirp("24e1240000000102", 2, "{\"temperature\":20}");

        publish(envelope);
        awaitRaw(envelope.messageId());

        assertThat(rawStatus(envelope.messageId())).isEqualTo("OK");
        assertThat(jdbc.sql("SELECT dropped FROM data2flow_pipeline.raw_messages").query(Boolean.class).single()).isTrue();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry")).isZero();
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(3)).until(() -> telemetry.telemetry().isEmpty());
    }

    @Test
    @DisplayName("[SCR-02.03][AT-SCR-04.2] TC-SCR-034 throw new Error(3:11): fail-open은 원래 값 저장·OK·trace에 오류 줄·열, fail-closed는 SCRIPT_ERROR·DLQ(SCRIPT)")
    void failurePolicies() {
        String boom = "function transform(msg, ctx) {\n  const x = 1;\n    throw new Error('boom');\n}";
        CORE.device(103, 1, 3, "24e1240000000103", "ACTIVE", 8L, 31L, 60);
        CORE.device(104, 1, 3, "24e1240000000104", "ACTIVE", 9L, 31L, 60);
        CORE.script(1, 4, "TRANSFORM", 40, 1, boom, "FAIL_OPEN", "MODEL", 8, null);
        CORE.script(1, 5, "TRANSFORM", 50, 1, boom, "FAIL_CLOSED", "MODEL", 9, null);
        deploy();
        RawEnvelope open = chirp("24e1240000000103", 3, "{\"temperature\":20.5}");
        RawEnvelope closed = chirp("24e1240000000104", 4, "{\"temperature\":20.5}");

        publish(open);
        publish(closed);
        awaitRaw(open.messageId());
        awaitRaw(closed.messageId());

        assertThat(rawStatus(open.messageId())).isEqualTo("OK");
        assertThat(jdbc.sql("SELECT value FROM data2flow_pipeline.telemetry WHERE device_id = 103").query(Double.class).single())
                .isEqualTo(20.5);
        JsonNode stage = trace(open.messageId()).get("stages").valueStream()
                .filter(s -> "script".equals(s.get("stage").asString())).findFirst().orElseThrow();
        assertThat(stage.get("info").get("error").asString()).isEqualTo("SCRIPT_RUNTIME_ERROR");
        assertThat(stage.get("info").get("line").asInt()).isEqualTo(3);
        assertThat(stage.get("info").get("col").asInt()).as("Error를 만든 위치(new Error)").isEqualTo(11);
        assertThat(rawStatus(closed.messageId())).isEqualTo("SCRIPT_ERROR");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE device_id = 104")).isZero();
        assertThat(jdbc.sql("SELECT stage FROM data2flow_pipeline.dlq_items").query(String.class).list()).containsExactly("SCRIPT");
    }

    @Test
    @DisplayName("[SCR-02.02][AT-SCR-02.2] TC-SCR-031 운영 경로의 while(true)(fail-open) → 원래 값 저장, 원본 OK, trace SCRIPT_TIMEOUT, 다음 메시지 정상")
    void timeoutFailOpen() {
        CORE.device(105, 1, 3, "24e1240000000105", "ACTIVE", 10L, 31L, 60);
        CORE.script(1, 6, "TRANSFORM", 60, 1, "function transform(msg, ctx) { while (true) {} }", "FAIL_OPEN", "MODEL", 10,
                null);
        deploy();
        RawEnvelope first = chirp("24e1240000000105", 5, "{\"temperature\":21}");
        RawEnvelope second = chirp("24e1240000000105", 6, "{\"temperature\":22}");

        publish(first);
        publish(second);
        awaitTelemetry(2);

        assertThat(rawStatus(first.messageId())).isEqualTo("OK");
        assertThat(trace(first.messageId()).toString()).contains("SCRIPT_TIMEOUT");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE device_id = 105")).isEqualTo(2);
    }

    @Test
    @DisplayName("[SCR-01.03][AT-SCR-02.1] TC-SCR-009 이슬점 스크립트(25℃, 60%) → dew_point 16.7, derived=true, 처음 보는 키는 UNVERIFIED 등록")
    void derivedMetric() {
        CORE.device(106, 1, 3, "24e1240000000106", "ACTIVE", 11L, 31L, 60);
        CORE.script(1, 7, "TRANSFORM", 70, 1, """
                function transform(msg, ctx) {
                  const t = ctx.util.metric(msg, 'temperature').value;
                  const h = ctx.util.metric(msg, 'humidity').value;
                  ctx.util.setMetric(msg, 'dew_point', ctx.util.dewPoint(t, h), '℃');
                  return msg;
                }
                """, "FAIL_OPEN", "MODEL", 11, null);
        deploy();

        publish(chirp("24e1240000000106", 7, "{\"temperature\":25,\"humidity\":60}"));
        CanonicalTelemetry t = awaitTelemetry(1);

        assertThat(t.metric("dew_point").value()).isCloseTo(16.7, org.assertj.core.data.Offset.offset(0.05));
        assertThat(t.metric("dew_point").derived()).isTrue();
        assertThat(t.metric("temperature").derived()).isNull();
        assertThat(CORE.unverifiedRequests()).anySatisfy(r ->
                assertThat(r.get("keys").get(0).get("key").asString()).isEqualTo("dew_point"));
    }

    @Test
    @DisplayName("[SCR-03.04][AT-SCR-03.1] TC-SCR-051 v1 처리 중 v2 배포(EVT-SCR-01) → 10초 안에 새 메시지가 v2로 처리, 적용 보고(API-SCR-34)")
    void hotReload() {
        CORE.device(107, 1, 3, "24e1240000000107", "ACTIVE", 12L, 31L, 60);
        CORE.script(1, 8, "TRANSFORM", 80, 1, "function transform(msg, ctx) { ctx.util.metric(msg, 'temperature').value = 1; return msg; }",
                "FAIL_OPEN", "MODEL", 12, null);
        deploy();
        publish(chirp("24e1240000000107", 8, "{\"temperature\":20}"));
        assertThat(awaitTelemetry(1).metric("temperature").value()).isEqualTo(1);

        CORE.script(1, 8, "TRANSFORM", 81, 2, "function transform(msg, ctx) { ctx.util.metric(msg, 'temperature').value = 2; return msg; }",
                "FAIL_OPEN", "MODEL", 12, null);
        rabbit.convertAndSend(MessagingNames.EXCHANGE_CONFIG, "", CODEC.write(
                ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.SCRIPT, 8, 2, 1, clock)));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(CORE.deployAcks()).anySatisfy(a -> assertThat(a.get("versionId").asString()).isEqualTo("81")));
        publish(chirp("24e1240000000107", 9, "{\"temperature\":20}"));
        CanonicalTelemetry t = awaitTelemetry(2);
        assertThat(t.metric("temperature").value()).isEqualTo(2);
        assertThat(t.meta().scripts()).singleElement().satisfies(s -> assertThat(s.version()).isEqualTo(2));
    }

    @Test
    @DisplayName("[SCR-03.02][AT-SCR-02.4] TC-SCR-043 테스트 실행(API-SCR-31) 100회 전후 telemetry·raw_messages 행 수와 발행이 그대로, 정적 검사(API-SCR-30)")
    void testRunHasNoSideEffects() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        long before = count("SELECT count(*) FROM data2flow_pipeline.raw_messages")
                + count("SELECT count(*) FROM data2flow_pipeline.telemetry");
        for (int i = 0; i < 100; i++) {
            String code = switch (i % 3) {
                case 0 -> "function transform(msg, ctx) { return msg; }";
                case 1 -> "function transform(msg, ctx) { throw new Error('x'); }";
                default -> "function transform(msg, ctx) { while (true) {} }";
            };
            String body = CODEC.mapper().writeValueAsString(CODEC.mapper().createObjectNode()
                    .put("kind", "TRANSFORM").put("code", code).put("organizationId", 1)
                    .set("input", CODEC.mapper().readTree("{\"metrics\":[{\"key\":\"t\",\"value\":1}]}")));
            HttpResponse<String> res = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port
                            + "/internal/pipeline/scripts/test-run")).header("Content-Type", "application/json")
                    .header("X-CALLER-SERVICE", "data2flow-core-api").POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(res.statusCode()).isEqualTo(200);
            JsonNode response = CODEC.mapper().readTree(res.body()).get("response");
            assertThat(response.get("ok").asBoolean()).isEqualTo(i % 3 == 0);
        }
        HttpResponse<String> check = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port
                        + "/internal/pipeline/scripts/check")).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"kind\":\"TRANSFORM\",\"code\":\"function transform(m, c) {\\n  return require('fs');\\n}\"}"))
                .build(), HttpResponse.BodyHandlers.ofString());

        assertThat(count("SELECT count(*) FROM data2flow_pipeline.raw_messages")
                + count("SELECT count(*) FROM data2flow_pipeline.telemetry")).isEqualTo(before);
        assertThat(telemetry.telemetry()).isEmpty();
        JsonNode checked = CODEC.mapper().readTree(check.body()).get("response");
        assertThat(checked.get("ok").asBoolean()).isFalse();
        assertThat(checked.get("problems").get(0).get("message").asString()).isEqualTo("금지된 API: require (2:10)");
    }
}
