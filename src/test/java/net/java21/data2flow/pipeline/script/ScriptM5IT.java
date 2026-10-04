package net.java21.data2flow.pipeline.script;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.pipeline.script.service.ScriptOps;
import net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import net.java21.data2flow.script.sandbox.ScriptErrorCode;
import net.java21.data2flow.script.sandbox.ScriptFailure;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 스크립트 M5(SCR-01.03 파생 항목, SCR-01.06 수식, SCR-03.03 테스트 케이스, SCR-03.05·05.01·05.02·05.03 운영 기록,
 * SCR-04.01 공유 모듈, SCR-04.02 설정값)를 실제 수집 경로·내부 API로 확인한다.
 */
class ScriptM5IT extends IntegrationTestSupport {

    private static final long DEVICE = 601;
    private static final String EUI = "24e1240000000601";
    private static final long MODEL = 31;

    private final HttpClient http = HttpClient.newHttpClient();
    @Autowired
    private ScriptRuntimeRegistry registry;
    @Autowired
    private ScriptOps ops;

    @BeforeEach
    void device() {
        CORE.device(DEVICE, 1, 3, EUI, "ACTIVE", MODEL, null, 60);
        CORE.metric("dew_point", "℃", -40.0, 60.0);
    }

    private void uplink(double temperature, double humidity) {
        publish(envelope(3, "MQTT_SUBSCRIBE", "application/a/device/" + EUI + "/event/up", ("""
                {"deduplicationId":"%s","deviceInfo":{"devEui":"%s"},"fCnt":1,"object":{"temperature":%s,"humidity":%s},
                 "rxInfo":[{"gatewayId":"g","rssi":-50,"snr":5,"nsTime":"%s"}]}""".formatted(UUID.randomUUID(), EUI,
                temperature, humidity, clock.instant())).getBytes(StandardCharsets.UTF_8), clock.instant()));
        clock.advance(Duration.ofSeconds(30));
    }

    private CanonicalTelemetry awaitTelemetry(int n) {
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() >= n);
        return telemetry.telemetry().get(n - 1);
    }

    private JsonNode call(String method, String path, String body, int expected) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json").header("X-CALLER-SERVICE", "data2flow-core-api");
        b = body == null ? b.GET() : b.method(method, HttpRequest.BodyPublishers.ofString(body));
        HttpResponse<String> res = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).as(res.body()).isEqualTo(expected);
        return CODEC.mapper().readTree(res.body());
    }

    private void transformScript(long id, long versionId, int versionNo, String code, String config) {
        CORE.script(1, id, "TRANSFORM", versionId, versionNo, code, "FAIL_OPEN", "DEVICE", DEVICE,
                CODEC.mapper().readTree(config));
        registry.reload(1);
    }

    @Test
    @DisplayName("[SCR-01.03][AT-SCR-02.1] TC-SCR-009 이슬점 TRANSFORM(25℃, 60% → dew_point 16.7) 저장 시 derived=true, 표준 메시지에 포함")
    void derivedDewPoint() {
        transformScript(81, 810, 1, """
                function transform(msg, ctx) {
                  const t = ctx.util.metric(msg, 'temperature').value;
                  const h = ctx.util.metric(msg, 'humidity').value;
                  ctx.util.setMetric(msg, 'dew_point', ctx.util.dewPoint(t, h), '℃');
                  return msg;
                }""", "{}");

        uplink(25, 60);
        CanonicalTelemetry t = awaitTelemetry(1);

        assertThat(t.metric("dew_point").value()).isCloseTo(16.7, org.assertj.core.data.Offset.offset(0.05));
        assertThat(t.metric("dew_point").derived()).isTrue();
        assertThat(t.metric("temperature").derived()).isNull();
        assertThat(jdbc.sql("SELECT value FROM data2flow_pipeline.telemetry WHERE device_id = :d AND metric_key = 'dew_point'")
                .param("d", DEVICE).query(Double.class).single()).isEqualTo(16.7);
    }

    @Test
    @DisplayName("[SCR-01.06][AT-SCR-06.1] TC-SCR-018 thi 수식 배포 → 다음 메시지부터 thi 저장(derived=true), 배포 전 메시지에는 없음, 처음 보는 키는 미검증 등록")
    void formulaDeploy() {
        uplink(28, 70);
        assertThat(awaitTelemetry(1).metric("thi")).isNull();

        CORE.formula(1, 900, "thi", null, "thi(temperature, humidity)", "MODEL", MODEL);
        registry.reload(1);
        uplink(28, 70);
        CanonicalTelemetry t = awaitTelemetry(2);

        assertThat(t.metric("thi").value()).isEqualTo(78.4);
        assertThat(t.metric("thi").derived()).isTrue();
        assertThat(CORE.unverifiedRequests()).anySatisfy(r -> assertThat(r.toString()).contains("thi"));
        assertThat(jdbc.sql("SELECT processing_trace::text FROM data2flow_pipeline.raw_messages ORDER BY id DESC LIMIT 1")
                .query(String.class).single()).contains("\"stage\": \"formula\"");
    }

    @Test
    @DisplayName("[SCR-04.01][AT-SCR-08.1] TC-SCR-068 모듈 v1을 쓰는 스크립트는 모듈 v2가 배포돼도 결과가 같다(버전 고정)")
    void modulePinned() {
        CORE.module(1, "calib", 1, "export function fix(v) { return v + 1; }");
        transformScript(82, 820, 1, """
                import { fix } from 'module:calib@1';
                function transform(msg, ctx) {
                  ctx.util.metric(msg, 'temperature').value = fix(ctx.util.metric(msg, 'temperature').value);
                  return msg;
                }""", "{}");
        uplink(20, 50);
        assertThat(awaitTelemetry(1).metric("temperature").value()).isEqualTo(21.0);

        CORE.module(1, "calib", 2, "export function fix(v) { return v + 100; }");
        registry.reload(1);
        uplink(20, 50);

        assertThat(awaitTelemetry(2).metric("temperature").value()).isEqualTo(21.0);
    }

    @Test
    @DisplayName("[SCR-04.02][AT-SCR-08.2] TC-SCR-071 설정값 tempOffset 0.5 → 0.7: 새 버전 없이 반영, 처리 기록의 configRevision이 오른다")
    void configHotApply() {
        transformScript(83, 830, 1, """
                function transform(msg, ctx) {
                  const m = ctx.util.metric(msg, 'temperature');
                  m.value = ctx.util.round(m.value + ctx.config.tempOffset, 2);
                  return msg;
                }""", "{\"tempOffset\":0.5}");
        CORE.scriptNode(1, 83).put("configRevision", 1);
        registry.reload(1);
        uplink(20, 50);
        assertThat(awaitTelemetry(1).metric("temperature").value()).isEqualTo(20.5);

        CORE.scriptNode(1, 83).put("configRevision", 2).set("config", CODEC.mapper().readTree("{\"tempOffset\":0.7}"));
        registry.reload(1);
        uplink(20, 50);

        assertThat(awaitTelemetry(2).metric("temperature").value()).isEqualTo(20.7);
        assertThat(CORE.deployAcks()).extracting(a -> a.get("versionId").asString()).containsOnly("830");
        assertThat(jdbc.sql("SELECT processing_trace::text FROM data2flow_pipeline.raw_messages ORDER BY id DESC LIMIT 1")
                .query(String.class).single()).contains("\"configRevision\": 2");
    }

    @Test
    @DisplayName("[SCR-05.01][AT-SCR-10.1] TC-SCR-080 오류마다 메시지·줄·열·입력 스냅샷·버전 보관, 150건이면 최근 100건만 / API-SCR-36 지표·경고")
    void errorSnapshotsAndStats() throws Exception {
        transformScript(84, 840, 4, """
                function transform(msg, ctx) {
                  if (msg.metrics.length > 0) {
                    throw new Error('센서 값이 이상합니다');
                  }
                  return msg;
                }""", "{}");
        uplink(20, 50);
        uplink(21, 50);
        awaitTelemetry(2);
        ops.flush();

        List<java.util.Map<String, Object>> errors = jdbc.sql("""
                        SELECT version_no, error_code, message, line, input_snapshot::text AS input, device_id
                          FROM data2flow_pipeline.script_errors WHERE script_id = 84 ORDER BY id""").query().listOfRows();
        assertThat(errors).hasSize(2);
        assertThat(errors.getFirst()).containsEntry("version_no", 4).containsEntry("error_code", "SCRIPT_RUNTIME_ERROR")
                .containsEntry("line", 3).containsEntry("device_id", DEVICE);
        assertThat((String) errors.getFirst().get("message")).contains("센서 값이 이상합니다");
        assertThat((String) errors.getFirst().get("input")).contains("temperature");

        var script = registry.plan(1).bundle().scripts().stream().filter(s -> s.scriptId() == 84).findFirst().orElseThrow();
        for (int i = 0; i < 150; i++) {
            ops.record(1, script, new ScriptOutcome(null, ScriptFailure.of(ScriptErrorCode.SCRIPT_RUNTIME_ERROR, "e" + i),
                    List.of(), 25, 0), 10, "{}", DEVICE, null, clock.instant().plusMillis(i));
        }
        ops.flush();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.script_errors WHERE script_id = 84")).isEqualTo(100);
        assertThat(jdbc.sql("SELECT min(message) FROM data2flow_pipeline.script_errors WHERE script_id = 84 AND message LIKE 'e1%'")
                .query(String.class).single()).isEqualTo("e100");

        String from = URLEncoder.encode(clock.instant().minus(Duration.ofHours(1)).toString(), StandardCharsets.UTF_8);
        String to = URLEncoder.encode(clock.instant().plus(Duration.ofHours(1)).toString(), StandardCharsets.UTF_8);
        JsonNode stats = call("GET", "/internal/pipeline/scripts/84/stats?organizationId=1&from=" + from + "&to=" + to, null, 200)
                .get("response");
        assertThat(stats.get("points")).isNotEmpty();
        assertThat(stats.get("points").get(0).get("versionNo").asInt()).isEqualTo(4);
        long processed = 0;
        for (JsonNode p : stats.get("points")) {
            processed += p.get("processed").asLong();
        }
        assertThat(processed).isEqualTo(152);
        assertThat(stats.get("warnings")).extracting(w -> w.get("type").asString()).contains("SLOW", "ERROR_RATE");
        assertThat(call("GET", "/internal/pipeline/scripts/84/stats?organizationId=1&step=1h&from=" + from + "&to=" + to, null, 200)
                .get("response").get("points")).isNotEmpty();
        call("GET", "/internal/pipeline/scripts/84/stats?organizationId=1&step=5m&from=" + from + "&to=" + to, null, 400);
    }

    @Test
    @DisplayName("[SCR-05.02][AT-SCR-10.2] TC-SCR-082 로그 수집을 켠 스크립트만 console.log를 남긴다")
    void logCapture() {
        transformScript(85, 850, 1, """
                function transform(msg, ctx) {
                  console.log('t=' + ctx.util.metric(msg, 'temperature').value);
                  return msg;
                }""", "{}");
        uplink(20, 50);
        awaitTelemetry(1);
        CORE.scriptNode(1, 85).put("logCaptureUntil", clock.instant().plus(Duration.ofMinutes(30)).toString());
        registry.reload(1);
        uplink(22, 50);
        awaitTelemetry(2);
        ops.flush();

        assertThat(jdbc.sql("SELECT message FROM data2flow_pipeline.script_logs WHERE script_id = 85").query(String.class).list())
                .containsExactly("t=22");
    }

    @Test
    @DisplayName("[SCR-03.03][AT-SCR-04.1] API-SCR-35 테스트 케이스 일괄 실행: 통과·실패와 다른 곳, 저장·발행 없음")
    void testCases() throws Exception {
        JsonNode r = call("POST", "/internal/pipeline/scripts/test-cases/run", """
                {"organizationId":1,"kind":"TRANSFORM","code":"function transform(msg, ctx) { msg.metrics[0].value += ctx.config.o; return msg; }",
                 "cases":[
                   {"id":"1","name":"보정","input":{"metrics":[{"key":"t","value":1}]},"context":{"config":{"o":0.5}},
                    "expected":{"metrics":[{"key":"t","value":1.5}]}},
                   {"id":"2","name":"허용 오차","input":{"metrics":[{"key":"t","value":1}]},"context":{"config":{"o":0.52}},
                    "expected":{"metrics":[{"key":"t","value":1.5}]},"compareMode":"TOLERANCE","tolerance":0.01}
                 ]}""", 200).get("response");

        assertThat(r.get("passed").asInt()).isEqualTo(1);
        assertThat(r.get("failed").asInt()).isEqualTo(1);
        assertThat(r.get("results").get(1).get("diff").get(0).get("path").asString()).isEqualTo("$.metrics[0].value");
        assertThat(telemetry.telemetry()).isEmpty();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.script_stats_1m")).isZero();
    }

    @Test
    @DisplayName("[SCR-01.06][AT-SCR-06.3] API-SCR-37·38 수식 검사(오타 → SCRIPT_FORMULA_INVALID 위치)와 미리 보기(지난 N시간 원본에 적용)")
    void formulaCompileAndPreview() throws Exception {
        JsonNode bad = call("POST", "/internal/pipeline/formula-metrics/compile",
                "{\"organizationId\":1,\"expression\":\"temprature * 2\"}", 200).get("response");
        assertThat(bad.get("ok").asBoolean()).isFalse();
        assertThat(bad.get("error").get("code").asString()).isEqualTo("SCRIPT_FORMULA_INVALID");
        assertThat(bad.get("error").get("message").asString()).isEqualTo("알 수 없는 측정 항목: temprature");
        JsonNode ok = call("POST", "/internal/pipeline/formula-metrics/compile",
                "{\"organizationId\":1,\"expression\":\"rolling_mean(temperature, 10m) * 1.8 + 32\"}", 200).get("response");
        assertThat(ok.get("ok").asBoolean()).isTrue();
        assertThat(ok.get("inputs").get(0).asString()).isEqualTo("temperature");
        assertThat(ok.get("windows").get("temperature").asString()).isEqualTo("PT10M");

        for (int i = 0; i < 3; i++) {
            jdbc.sql("""
                            INSERT INTO data2flow_pipeline.telemetry (device_id, metric_key, time, organization_id, value, quality,
                                received_at) VALUES (:d, 'temperature', :t, 1, :v, 0, :t)""")
                    .param("d", DEVICE).param("t", Timestamp.from(clock.instant().minus(Duration.ofMinutes(30 - i))))
                    .param("v", 20 + i).update();
        }
        JsonNode preview = call("POST", "/internal/pipeline/formula-metrics/preview",
                "{\"organizationId\":1,\"expression\":\"temperature * 1.8 + 32\",\"deviceId\":601,\"hours\":1}", 200)
                .get("response");
        assertThat(preview.get("series")).extracting(p -> p.get("value").asDouble())
                .usingElementComparator((a, b) -> Math.abs(a - b) < 1e-9 ? 0 : Double.compare(a, b))
                .containsExactly(68.0, 69.8, 71.6);
        assertThat(preview.get("inputs").get("temperature")).hasSize(3);
        call("POST", "/internal/pipeline/formula-metrics/preview",
                "{\"organizationId\":1,\"expression\":\"tem *\",\"deviceId\":601,\"hours\":1}", 400);
        call("POST", "/internal/pipeline/formula-metrics/preview",
                "{\"organizationId\":1,\"expression\":\"temperature\",\"deviceId\":601,\"hours\":48}", 400);
    }

    @Test
    @DisplayName("[SCR-04.01] API-SCR-30 정적 검사: 조직의 모듈로 가져오기를 확인한다(없는 버전 → SCRIPT_MODULE_NOT_FOUND)")
    void checkWithModules() throws Exception {
        CORE.module(1, "calib", 1, "export function fix(v) { return v; }");
        registry.reload(1);
        String code = "import { fix } from 'module:calib@%d';\\nfunction transform(msg, ctx) { return msg; }";
        JsonNode ok = call("POST", "/internal/pipeline/scripts/check",
                "{\"kind\":\"TRANSFORM\",\"organizationId\":1,\"code\":\"" + code.formatted(1) + "\"}", 200).get("response");
        assertThat(ok.get("ok").asBoolean()).as(ok.toString()).isTrue();
        JsonNode missing = call("POST", "/internal/pipeline/scripts/check",
                "{\"kind\":\"TRANSFORM\",\"organizationId\":1,\"code\":\"" + code.formatted(2) + "\"}", 200).get("response");
        assertThat(missing.get("ok").asBoolean()).isFalse();
        assertThat(missing.get("problems").get(0).get("code").asString()).isEqualTo("SCRIPT_MODULE_NOT_FOUND");
    }
}
