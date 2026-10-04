package net.java21.data2flow.pipeline.ingest;

import net.java21.data2flow.pipeline.aggregate.service.AggregationService;
import net.java21.data2flow.pipeline.ingest.service.ReprocessJobService;
import net.java21.data2flow.pipeline.script.domain.FailurePolicy;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.service.BundleCodec;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * ING-01.04 기간 재처리 작업(API-ING-23, BR-ING-12·13): 같은 키 덮어쓰기와 집계 재계산, 버전 고정, 다시 발행하지 않음,
 * 끝 이벤트(EVT-ING-09), 취소, 죽은 인스턴스의 작업 넘겨받기.
 */
class ReprocessJobIT extends IntegrationTestSupport {

    private static final long DEVICE = 501;
    private static final String EUI = "24e1240000000501";
    private static final Instant FROM = Instant.parse("2026-10-02T22:00:00Z");
    private static final String OFFSET_SCRIPT = """
            function transform(msg, ctx) {
              const m = ctx.util.metric(msg, 'temperature');
              if (m) { m.value = ctx.util.round(m.value + ctx.config.offset, 2); }
              return msg;
            }""";

    private final HttpClient http = HttpClient.newHttpClient();

    @Autowired
    private ReprocessJobService jobs;
    @Autowired
    private AggregationService aggregation;

    @BeforeEach
    void device() {
        CORE.device(DEVICE, 1, 3, EUI, "ACTIVE", null, null, 60);
        deploy(1, 0.0);
    }

    private void deploy(int version, double offset) {
        CORE.script(1, 77, "TRANSFORM", 7700 + version, version, OFFSET_SCRIPT, "FAIL_OPEN", "DEVICE", DEVICE,
                CODEC.mapper().readTree("{\"offset\":" + offset + "}"));
    }

    private JsonNode post(String path, String body, int expected) throws Exception {
        HttpResponse<String> res = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json").header("X-CALLER-SERVICE", "data2flow-core-api")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).as(res.body()).isEqualTo(expected);
        return CODEC.mapper().readTree(res.body());
    }

    /** 1분 간격 n건(측정 = 수신 시각, 값 20.0, 20.1, …) */
    private void seed(int n) {
        for (int i = 0; i < n; i++) {
            Instant at = FROM.plus(Duration.ofMinutes(i));
            publish(envelope(3, "MQTT_SUBSCRIBE", "application/a/device/" + EUI + "/event/up", ("""
                    {"deduplicationId":"%s","deviceInfo":{"devEui":"%s"},"fCnt":%d,"object":{"temperature":%s},
                     "rxInfo":[{"gatewayId":"g","rssi":-50,"snr":5,"nsTime":"%s"}]}""".formatted(UUID.randomUUID(), EUI, i,
                    20 + i / 10.0, at)).getBytes(StandardCharsets.UTF_8), at));
        }
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == n);
    }

    private double value(int minute) {
        return jdbc.sql("SELECT value FROM data2flow_pipeline.telemetry WHERE device_id = :d AND metric_key = 'temperature' AND time = :t")
                .param("d", DEVICE).param("t", Timestamp.from(FROM.plus(Duration.ofMinutes(minute)))).query(Double.class).single();
    }

    private String status(long jobId) {
        return jdbc.sql("SELECT status FROM data2flow_pipeline.reprocess_jobs WHERE id = :id").param("id", jobId)
                .query(String.class).single();
    }

    private long rawIdAt(int index) {
        return jdbc.sql("SELECT id FROM data2flow_pipeline.raw_messages WHERE device_id = :d ORDER BY id OFFSET :o LIMIT 1")
                .param("d", DEVICE).param("o", index).query(Long.class).single();
    }

    @Test
    @DisplayName("[ING-01.04][AT-ING-08.1] TC-ING-026 보정 스크립트 v2(+0.5) 배포 후 재처리 → 구간 temperature가 모두 +0.5로 교체(행 수 동일), 1h 집계 재계산, 구간 밖 불변, 다시 발행 없음, ingest.reprocess.finished")
    void replacesTelemetry() throws Exception {
        seed(70);
        clock.set(FROM.plus(Duration.ofHours(2)));
        for (String level : List.of("1m", "1h", "1d")) {
            jdbc.sql("INSERT INTO data2flow_pipeline.agg_watermarks (level, processed_until) VALUES (:l, :t)")
                    .param("l", level).param("t", Timestamp.from(FROM)).update();
        }
        aggregation.aggregateMinutes();
        aggregation.aggregateHours();
        double hourBefore = jdbc.sql("SELECT avg FROM data2flow_pipeline.telemetry_1h WHERE device_id = :d AND bucket = :b")
                .param("d", DEVICE).param("b", Timestamp.from(FROM)).query(Double.class).single();
        long rowsBefore = count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE device_id = 501");
        int published = telemetry.telemetry().size();
        deploy(2, 0.5);

        JsonNode created = post("/internal/pipeline/reprocess-jobs", """
                {"organizationId":1,"requestedBy":9,"sourceId":3,"deviceIds":[501],"from":"%s","to":"%s","onlyFailed":false,
                 "memo":"보정 반영"}""".formatted(FROM, FROM.plus(Duration.ofHours(1))), 202).get("response");
        long jobId = created.get("jobId").asLong();
        assertThat(created.get("status").asString()).isEqualTo("QUEUED");
        assertThat(created.get("estimatedCount").asLong()).isEqualTo(60);
        await().atMost(Duration.ofSeconds(30)).until(() -> status(jobId).equals("COMPLETED"));

        assertThat(jdbc.sql("""
                        SELECT r.processing_trace::text FROM data2flow_pipeline.raw_messages r
                         WHERE r.device_id = 501 AND r.processing_trace::text LIKE '%SCRIPT_%'""").query(String.class).list())
                .as("스크립트 오류 없이 재처리").isEmpty();
        assertThat(value(0)).isEqualTo(20.5);
        assertThat(value(59)).isEqualTo(26.4);
        assertThat(value(65)).as("구간 밖은 그대로").isEqualTo(26.5);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE device_id = 501")).isEqualTo(rowsBefore);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE device_id = 501 AND flags & 4 = 4"))
                .isEqualTo(60);
        assertThat(jdbc.sql("SELECT processed, failed, skipped, script_versions::text FROM data2flow_pipeline.reprocess_jobs WHERE id = :id")
                .param("id", jobId).query().singleRow()).containsEntry("processed", 60L).containsEntry("failed", 0L)
                .containsEntry("script_versions", "{\"77\": 2}");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.agg_dirty_ranges WHERE reason = 'REPROCESS'")).isEqualTo(60);

        aggregation.aggregateMinutes();
        aggregation.aggregateHours();
        double hourAfter = jdbc.sql("SELECT avg FROM data2flow_pipeline.telemetry_1h WHERE device_id = :d AND bucket = :b")
                .param("d", DEVICE).param("b", Timestamp.from(FROM)).query(Double.class).single();
        assertThat(hourAfter).isCloseTo(hourBefore + 0.5, org.assertj.core.data.Offset.offset(1e-9));

        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(events("ingest.reprocess.finished"))
                .singleElement().satisfies(e -> {
                    JsonNode p = e.get("payload");
                    assertThat(p.get("jobId").asString()).isEqualTo(Long.toString(jobId));
                    assertThat(p.get("status").asString()).isEqualTo("COMPLETED");
                    assertThat(p.get("processed").asLong()).isEqualTo(60);
                    assertThat(p.get("total").asLong()).isEqualTo(60);
                    assertThat(p.get("deviceIds").get(0).asLong()).isEqualTo(DEVICE);
                    assertThat(p.get("requestedBy").asLong()).isEqualTo(9);
                }));
        assertThat(events("aggregates.recomputed")).isNotEmpty();
        assertThat(telemetry.telemetry()).as("재처리 결과는 data2flow.telemetry에 다시 내지 않는다").hasSize(published);
    }

    @Test
    @DisplayName("[ING-01.04][BR-ING-12] 죽은 인스턴스가 맡던 RUNNING 작업을 넘겨받아 마지막 원본 다음부터 잇고, 고정한 버전(v2)으로 처리한다(그 사이 v3이 배포돼도)")
    void recoversStaleJobWithPinnedBundle() {
        seed(20);
        long lastDone = rawIdAt(9);
        RuntimeBundle pinned = new RuntimeBundle(5, List.of(new RuntimeBundle.Script(77, ScriptKind.TRANSFORM, 7702, 2,
                OFFSET_SCRIPT, CODEC.mapper().readTree("{\"offset\":0.5}"), FailurePolicy.FAIL_OPEN, true,
                List.of(new RuntimeBundle.Binding("DEVICE", DEVICE, true, null)))));
        deploy(3, 1.0);
        long jobId = jdbc.sql("""
                        INSERT INTO data2flow_pipeline.reprocess_jobs (organization_id, source_id, device_ids, period_from, period_to,
                            status, total, processed, decoder_version, requested_by, last_raw_id, owner_instance, heartbeat_at,
                            started_at, pinned_bundle)
                        VALUES (1, 3, ARRAY[501]::bigint[], :from, :to, 'RUNNING', 20, 10, 'chirpstack-v4', 9, :last,
                            'pipeline-dead-0', :beat, :beat, CAST(:bundle AS jsonb)) RETURNING id""")
                .param("from", Timestamp.from(FROM)).param("to", Timestamp.from(FROM.plus(Duration.ofHours(1))))
                .param("last", lastDone).param("beat", Timestamp.from(clock.instant().minus(Duration.ofMinutes(5))))
                .param("bundle", BundleCodec.write(pinned)).query(Long.class).single();

        assertThat(jobs.recoverStale()).containsExactly(jobId);
        await().atMost(Duration.ofSeconds(30)).until(() -> status(jobId).equals("COMPLETED"));

        assertThat(value(9)).as("이미 처리한 원본은 다시 처리하지 않음").isEqualTo(20.9);
        assertThat(value(10)).as("고정한 v2(+0.5), v3(+1.0) 아님").isEqualTo(21.5);
        assertThat(value(19)).isEqualTo(22.4);
        assertThat(jdbc.sql("SELECT processed FROM data2flow_pipeline.reprocess_jobs WHERE id = :id").param("id", jobId)
                .query(Long.class).single()).isEqualTo(20);
        assertThat(jobs.recoverStale()).as("끝난 작업은 다시 맡지 않음").isEmpty();
    }

    @Test
    @DisplayName("[ING-01.04][AT-ING-08.2] TC-ING-027·TC-ING-024 같은 소스에 작업이 있으면 409, 보관 기간 밖 400, 취소 → CANCELLED·이벤트, 끝난 작업 취소 409, 살아 있는 인스턴스의 작업은 넘겨받지 않음")
    void conflictsAndCancel() throws Exception {
        long jobId = jdbc.sql("""
                        INSERT INTO data2flow_pipeline.reprocess_jobs (organization_id, source_id, period_from, period_to, status,
                            total, processed, decoder_version, requested_by, owner_instance, heartbeat_at)
                        VALUES (1, 3, :from, :to, 'PENDING', 10000, 4000, 'chirpstack-v4', 9, 'pipeline-alive-1', :beat)
                        RETURNING id""")
                .param("from", Timestamp.from(FROM)).param("to", Timestamp.from(FROM.plus(Duration.ofHours(1))))
                .param("beat", Timestamp.from(clock.instant())).query(Long.class).single();
        String body = "{\"organizationId\":1,\"requestedBy\":9,\"sourceId\":3,\"from\":\"%s\",\"to\":\"%s\",\"onlyFailed\":false}";

        assertThat(post("/internal/pipeline/reprocess-jobs", body.formatted(FROM, FROM.plus(Duration.ofHours(1))), 409)
                .get("header").get("resultCode").asString()).isEqualTo("ING_REPROCESS_ALREADY_RUNNING");
        assertThat(post("/internal/pipeline/reprocess-jobs", body.formatted(clock.instant().minus(Duration.ofDays(40)),
                clock.instant().minus(Duration.ofDays(39))), 400).get("header").get("resultCode").asString())
                .isEqualTo("ING_REPROCESS_OUT_OF_RETENTION");
        assertThat(jobs.recoverStale()).isEmpty();

        JsonNode cancelled = post("/internal/pipeline/reprocess-jobs/" + jobId + "/cancel",
                "{\"organizationId\":1,\"requestedBy\":9}", 200).get("response");
        assertThat(cancelled.get("status").asString()).isEqualTo("CANCELLING");
        assertThat(status(jobId)).isEqualTo("CANCELLED");
        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(events("ingest.reprocess.finished"))
                .singleElement().satisfies(e -> {
                    assertThat(e.get("payload").get("status").asString()).isEqualTo("CANCELLED");
                    assertThat(e.get("payload").get("processed").asLong()).as("진행률 고정").isEqualTo(4000);
                }));
        assertThat(post("/internal/pipeline/reprocess-jobs/" + jobId + "/cancel", "{\"organizationId\":1,\"requestedBy\":9}", 409)
                .get("header").get("resultCode").asString()).isEqualTo("ING_REPROCESS_NOT_CANCELLABLE");
        assertThat(post("/internal/pipeline/reprocess-jobs/999999/cancel", "{\"organizationId\":1,\"requestedBy\":9}", 404)
                .get("header").get("resultCode").asString()).isEqualTo("RESOURCE_NOT_FOUND");
    }

    @Test
    @DisplayName("[ING-01.04] 단건 재처리(API-ING-22)는 이미 발행한 원본을 두 번 내지 않는다")
    void itemReprocessDoesNotRepublishOk() throws Exception {
        seed(1);
        long rawId = rawIdAt(0);

        JsonNode r = post("/internal/pipeline/reprocess-items",
                "{\"organizationId\":1,\"requestedBy\":7,\"items\":[{\"kind\":\"RAW\",\"id\":" + rawId + "}]}", 200);

        assertThat(r.get("response").get("results").get(0).get("outcome").asString()).isIn("OK", "SKIPPED");
        assertThat(telemetry.telemetry()).hasSize(1);
    }
}
