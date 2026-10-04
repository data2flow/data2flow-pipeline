package net.java21.data2flow.pipeline.retention;

import net.java21.data2flow.pipeline.aggregate.service.AggregationService;
import net.java21.data2flow.pipeline.partition.repository.PartitionRepository;
import net.java21.data2flow.pipeline.retention.service.RetentionService;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
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
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 보관(TSD-02.01·02.03·05.01·05.03, NFR-04.03, BR-TSD-02·03·07·17): 조직·측정 항목별 보관 기간 삭제, 연장 보관, 파티션 DETACH·DROP,
 * 원본 메시지 조직별 보관, 정렬 재작성, 상태형 ON_CHANGE 저장과 켜짐 시간, 미리 보기·정책 통지 API.
 */
class RetentionIT extends IntegrationTestSupport {

    private final HttpClient http = HttpClient.newHttpClient();
    @Autowired
    private RetentionService retention;
    @Autowired
    private PartitionRepository partitions;
    @Autowired
    private AggregationService aggregation;

    private void telemetryRow(long org, long device, String key, Instant time, double value) {
        jdbc.sql("""
                        INSERT INTO data2flow_pipeline.telemetry (device_id, metric_key, time, organization_id, value, quality,
                            received_at) VALUES (:d, :k, :t, :o, :v, 0, :t)""")
                .param("d", device).param("k", key).param("t", Timestamp.from(time)).param("o", org).param("v", value).update();
        jdbc.sql("INSERT INTO data2flow_pipeline.device_state (device_id, organization_id) VALUES (:d, :o) ON CONFLICT DO NOTHING")
                .param("d", device).param("o", org).update();
    }

    private long rows(String where) {
        return count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE " + where);
    }

    private JsonNode post(String path, String body) throws Exception {
        HttpResponse<String> res = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json").header("X-CALLER-SERVICE", "data2flow-core-api")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).as(res.body()).isEqualTo(200);
        return CODEC.mapper().readTree(res.body()).get("response");
    }

    @Test
    @DisplayName("[TSD-05.01][AT-TSD-06.3] TC-TSD-126·TC-TSD-028 LAeq 90일·조직 기본 365일: 야간 작업이 LAeq만 90일 지난 행을 지우고 다른 항목·다른 조직은 유지, 미리 보기 건수, retention.purged")
    void metricRetention() throws Exception {
        Instant now = clock.instant();
        CORE.retentionPolicy(1, "ORG", null, "TELEMETRY", 365, false, null);
        CORE.retentionPolicy(1, "METRIC", "LAeq", "TELEMETRY", 90, false, null);
        telemetryRow(1, 11, "LAeq", now.minus(Duration.ofDays(100)), 55);
        telemetryRow(1, 11, "LAeq", now.minus(Duration.ofDays(80)), 56);
        telemetryRow(1, 12, "temperature", now.minus(Duration.ofDays(100)), 21);
        telemetryRow(1, 12, "temperature", now.minus(Duration.ofDays(400)), 20);
        telemetryRow(2, 21, "LAeq", now.minus(Duration.ofDays(100)), 50);

        JsonNode preview = post("/internal/pipeline/retention/preview", """
                {"organizationId":1,"items":[{"scope":"METRIC","scopeRef":"LAeq","dataClass":"TELEMETRY","retainDays":90},
                 {"scope":"ORG","dataClass":"TELEMETRY","retainDays":30}]}""");
        assertThat(preview.get("byMetric").get(0).get("rows").asLong()).isEqualTo(1);
        assertThat(preview.get("byMetric").get(1).get("rows").asLong()).isEqualTo(4);
        assertThat(preview.get("affectedRows").asLong()).isEqualTo(5);
        assertThat(Instant.parse(post("/internal/pipeline/retention/apply-policy", "{\"organizationId\":1,\"policyVersion\":2}")
                .get("appliesAt").asString())).isEqualTo(Instant.parse("2026-10-03T02:00:00Z"));

        RetentionService.Report report = retention.run();

        assertThat(rows("metric_key = 'LAeq' AND organization_id = 1")).isEqualTo(1);
        assertThat(rows("metric_key = 'temperature'")).isEqualTo(1);
        assertThat(rows("organization_id = 2")).as("다른 조직은 기본값(365일)").isEqualTo(1);
        assertThat(report.deletedRows).isEqualTo(2);
        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(events("retention.purged"))
                .allSatisfy(e -> assertThat(e.get("payload").get("dataClass").asString()).isEqualTo("TELEMETRY"))
                .extracting(e -> e.get("payload").get("rows").asLong()).containsExactlyInAnyOrder(1L, 1L));
    }

    @Test
    @DisplayName("[TSD-02.01][BR-TSD-02] TC-TSD-031·TC-TSD-034 모든 조직의 기간이 지난 월 파티션: 더 긴 보관 항목(co2 730일)은 telemetry_long으로 옮기고 DETACH 후 DROP, 등록 상태 DROPPED")
    void partitionDropWithLongRetention() {
        Instant from = Instant.parse("2025-03-01T00:00:00Z");
        Instant to = Instant.parse("2025-04-01T00:00:00Z");
        partitions.createPartition("telemetry", "telemetry_y2025m03", from, to);
        partitions.register("telemetry_y2025m03", "telemetry", from, to, clock.instant());
        CORE.retentionPolicy(1, "METRIC", "co2", "TELEMETRY", 730, false, null);
        telemetryRow(1, 13, "co2", Instant.parse("2025-03-10T00:00:00Z"), 800);
        telemetryRow(1, 13, "temperature", Instant.parse("2025-03-10T00:00:00Z"), 19);

        RetentionService.Report report = retention.run();

        assertThat(report.droppedPartitions).contains("telemetry_y2025m03");
        assertThat(partitions.exists("telemetry_y2025m03")).isFalse();
        assertThat(jdbc.sql("SELECT state FROM data2flow_pipeline.partition_registries WHERE partition_name = 'telemetry_y2025m03'")
                .query(String.class).single()).isEqualTo("DROPPED");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_long WHERE metric_key = 'co2'")).isEqualTo(1);
        assertThat(rows("device_id = 13")).isZero();
        assertThat(retention.run().droppedPartitions).as("멱등").doesNotContain("telemetry_y2025m03");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_long")).as("연장 보관 730일 안").isEqualTo(1);
    }

    @Test
    @DisplayName("[TSD-02.01][NFR-04.03] TC-NFR-042 원본 메시지: 조직 보관이 기본보다 짧으면(7일) 그 조직 행만 지우고, 1분 집계는 90일·통신 품질은 90일 기본")
    void rawAndAggregates() {
        CORE.retentionPolicy(2, "ORG", null, "RAW_MESSAGE", 7, false, null);
        Instant old = clock.instant().minus(Duration.ofDays(10));
        for (long org : new long[]{1, 2}) {
            jdbc.sql("""
                            INSERT INTO data2flow_pipeline.raw_messages (organization_id, source_id, message_id, source_type, payload,
                                payload_encoding, ingress_instance, dedup_key, stream_partition, stream_offset, status, received_at)
                            VALUES (:o, 3, :m, 'MQTT_SUBSCRIBE', '\\x7b7d', 'JSON', 'i', 'k', 0, 0, 'OK', :t)""")
                    .param("o", org).param("m", UUID.randomUUID()).param("t", Timestamp.from(old)).update();
            jdbc.sql("INSERT INTO data2flow_pipeline.device_state (device_id, organization_id) VALUES (:d, :o)")
                    .param("d", 900 + org).param("o", org).update();
        }
        jdbc.sql("""
                INSERT INTO data2flow_pipeline.telemetry_1m (device_id, metric_key, bucket, organization_id, count, count_all)
                VALUES (901, 'temperature', :old, 1, 1, 1), (901, 'temperature', :recent, 1, 1, 1)""")
                .param("old", Timestamp.from(clock.instant().minus(Duration.ofDays(120))))
                .param("recent", Timestamp.from(clock.instant().minus(Duration.ofDays(60)))).update();
        jdbc.sql("""
                INSERT INTO data2flow_pipeline.link_qualities (device_id, gateway_eui, time, organization_id, rssi)
                VALUES (901, 'g', :old, 1, -90)""").param("old", Timestamp.from(clock.instant().minus(Duration.ofDays(100))))
                .update();

        retention.run();

        assertThat(count("SELECT count(*) FROM data2flow_pipeline.raw_messages WHERE organization_id = 2")).isZero();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.raw_messages WHERE organization_id = 1")).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_1m")).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.link_qualities")).isZero();
    }

    @Test
    @DisplayName("[TSD-02.03][BR-TSD-07] TC-TSD-040 7일이 지난 원본 파티션 하나를 (device_id, time) 순서로 다시 쓰고(행 수 같음) 등록 상태 COMPRESSED, 다시 붙어 있어 쓰기 가능")
    void sortRewrite() {
        Instant base = Instant.parse("2026-09-10T00:00:00Z");
        for (int i = 0; i < 30; i++) {
            telemetryRow(1, 30 - (i % 3), "temperature", base.plus(Duration.ofMinutes(30 - i)), i);
        }
        clock.set(Instant.parse("2026-10-09T00:00:00Z"));
        RetentionService.Report report = new RetentionService.Report();
        String sorted = null;
        for (int i = 0; i < 20 && !"telemetry_y2026m09".equals(sorted); i++) {
            sorted = retention.compressOne(clock.instant(), report);
            if (sorted == null) {
                break;
            }
        }

        assertThat(sorted).isEqualTo("telemetry_y2026m09");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_y2026m09")).isEqualTo(30);
        List<Long> physical = jdbc.sql("SELECT device_id FROM data2flow_pipeline.telemetry_y2026m09").query(Long.class).list();
        assertThat(physical).isSorted();
        assertThat(jdbc.sql("SELECT state FROM data2flow_pipeline.partition_registries WHERE partition_name = 'telemetry_y2026m09'")
                .query(String.class).single()).isEqualTo("COMPRESSED");
        telemetryRow(1, 99, "temperature", base, 1);
        assertThat(rows("device_id = 99")).isEqualTo(1);
    }

    @Test
    @DisplayName("[TSD-05.03][AT-TSD-06.4] TC-TSD-132·TC-TSD-031 상태형 door(ON_CHANGE): 바뀔 때와 1시간 하트비트만 저장, 표준 메시지는 모두 발행, 시간별 켜짐 시간은 정확(3600·1800·0초)")
    void onChangeStateMetric() {
        CORE.source(44, 1, "generic-json", "AUTO_REGISTER", null);
        CORE.sourceDecoderConfig(44, CODEC.mapper().readTree(
                "{\"deviceIdFrom\":\"topic[1]\",\"timeFrom\":\"$.ts\",\"metrics\":[{\"path\":\"$.door\",\"key\":\"door\"}]}"));
        CORE.device(441, 1, 44, "door-01", "ACTIVE", null, null, 600);
        CORE.retentionPolicy(1, "METRIC", "door", "TELEMETRY", 365, false, "ON_CHANGE");
        retentionPolicies.refresh();
        Instant t0 = Instant.parse("2026-10-02T20:00:00Z");
        for (int k = 0; k < 18; k++) {
            Instant at = t0.plus(Duration.ofMinutes(10L * k));
            publish(envelope(44, "MQTT_SUBSCRIBE", "devices/door-01/up",
                    ("{\"ts\":\"" + at + "\",\"door\":" + (k < 9 ? 1 : 0) + "}").getBytes(StandardCharsets.UTF_8), at));
        }
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 18);

        List<Map<String, Object>> stored = jdbc.sql("""
                        SELECT time, value, flags FROM data2flow_pipeline.telemetry WHERE device_id = 441 ORDER BY time""")
                .query().listOfRows();
        assertThat(stored).extracting(r -> ((Timestamp) r.get("time")).toInstant()).containsExactly(t0,
                t0.plus(Duration.ofMinutes(60)), t0.plus(Duration.ofMinutes(90)), t0.plus(Duration.ofMinutes(150)));
        assertThat(stored).extracting(r -> ((Number) r.get("flags")).intValue() & 8).containsExactly(8, 0, 8, 0);

        for (String level : List.of("1m", "1h", "1d")) {
            jdbc.sql("INSERT INTO data2flow_pipeline.agg_watermarks (level, processed_until) VALUES (:l, :t)")
                    .param("l", level).param("t", Timestamp.from(t0)).update();
        }
        aggregation.aggregateMinutes();
        aggregation.aggregateHours();
        List<Map<String, Object>> hours = jdbc.sql("""
                        SELECT bucket, state_on_sec, state_changes FROM data2flow_pipeline.telemetry_1h WHERE device_id = 441
                         ORDER BY bucket LIMIT 3""").query().listOfRows();
        assertThat(hours).extracting(r -> ((Number) r.get("state_on_sec")).intValue()).containsExactly(3600, 1800, 0);
        assertThat(hours).extracting(r -> ((Number) r.get("state_changes")).intValue()).containsExactly(0, 1, 0);
    }
}
