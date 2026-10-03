package net.java21.data2flow.pipeline.ingest;

import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.pipeline.partition.service.PartitionMaintenanceService;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** ING-07.03 실패 메시지 보관함: 선택 재처리(API-ING-22, TC-ING-087), 폐기(API-ING-24), 14일 정리(TC-ING-088) */
class FailureHandlingIT extends IntegrationTestSupport {

    @Autowired
    private PartitionMaintenanceService partitions;

    private final HttpClient http = HttpClient.newHttpClient();

    private JsonNode post(String path, String body, int expectedStatus) throws Exception {
        HttpResponse<String> res = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json").header("X-CALLER-SERVICE", "data2flow-core-api")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).as(res.body()).isEqualTo(expectedStatus);
        return CODEC.mapper().readTree(res.body());
    }

    private List<RawEnvelope> failingMessages(int n) {
        CORE.source(8, 1, "generic-json", "AUTO_REGISTER", null);
        CORE.sourceDecoderConfig(8, CODEC.mapper().readTree(
                "{\"deviceIdFrom\":\"$.wrong\",\"metrics\":[{\"path\":\"$.temp\",\"key\":\"temperature\"}]}"));
        CORE.device(81, 1, 8, "esp-81", "ACTIVE", null, null, 60);
        List<RawEnvelope> sent = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            RawEnvelope e = envelope(8, "devices/esp-81/telemetry",
                    "{\"id\":\"esp-81\",\"ts\":\"2026-10-03T00:00:%02dZ\",\"temp\":%d}".formatted(i, 20 + i));
            sent.add(e);
            publish(e);
        }
        sent.forEach(e -> awaitRaw(e.messageId()));
        return sent;
    }

    @Test
    @DisplayName("[ING-07.03][AT-ING-07.1] TC-ING-087 매핑 오류로 실패 3건 → 매핑 수정 → 재처리(API-ING-22) → 3건 RESOLVED·OK, telemetry 저장, 메시지별 결과")
    void reprocessAfterFix() throws Exception {
        failingMessages(3);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.dlq_items WHERE status = 'OPEN'")).isEqualTo(3);
        List<Long> ids = jdbc.sql("SELECT id FROM data2flow_pipeline.dlq_items ORDER BY id").query(Long.class).list();

        CORE.sourceDecoderConfig(8, CODEC.mapper().readTree(
                "{\"deviceIdFrom\":\"$.id\",\"timeFrom\":\"$.ts\",\"metrics\":[{\"path\":\"$.temp\",\"key\":\"temperature\"}]}"));
        rabbit.convertAndSend("data2flow.config", "", CODEC.write(net.java21.data2flow.contracts.message.ConfigChangedMessage
                .upsert(net.java21.data2flow.contracts.message.ConfigChangedMessage.EntityType.SOURCE, 8, 2, 1, clock)));
        StringBuilder items = new StringBuilder();
        ids.forEach(id -> items.append(items.isEmpty() ? "" : ",").append("{\"kind\":\"DLQ\",\"id\":").append(id).append('}'));
        JsonNode response = await().atMost(Duration.ofSeconds(10)).until(() -> post("/internal/pipeline/reprocess-items",
                "{\"organizationId\":1,\"requestedBy\":7,\"items\":[" + items + "]}", 200).get("response"),
                r -> r.get("ok").asInt() == 3);

        assertThat(response.get("results")).allSatisfy(r -> {
            assertThat(r.get("outcome").asString()).isEqualTo("OK");
            assertThat(r.get("previousErrorCode").asString()).isEqualTo("ING_EXTERNAL_ID_MISSING");
        });
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.dlq_items WHERE status = 'RESOLVED'")).isEqualTo(3);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.raw_messages WHERE status = 'OK'")).isEqualTo(3);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE flags & 4 = 4")).as("reprocessed 비트")
                .isEqualTo(3);
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 3);
        assertThat(jdbc.sql("SELECT processing_trace->'attempts'->-1->>'status' FROM data2flow_pipeline.raw_messages LIMIT 1")
                .query(String.class).single()).isEqualTo("OK");
    }

    @Test
    @DisplayName("[ING-07.03][AT-ING-07.2] 같은 실패를 다시 처리하면 FAILED(OPEN, 시도 수 증가), 잠긴 항목·없는 항목은 SKIPPED, 5,001건은 400")
    void reprocessFailuresAndLimits() throws Exception {
        failingMessages(2);
        List<Long> ids = jdbc.sql("SELECT id FROM data2flow_pipeline.dlq_items ORDER BY id").query(Long.class).list();
        jdbc.sql("UPDATE data2flow_pipeline.dlq_items SET status = 'REPROCESSING', locked_by = 9, locked_until = now() + interval '1 hour' WHERE id = :id")
                .param("id", ids.get(1)).update();
        long rawId = jdbc.sql("SELECT id FROM data2flow_pipeline.raw_messages LIMIT 1").query(Long.class).single();

        JsonNode response = post("/internal/pipeline/reprocess-items", "{\"organizationId\":1,\"requestedBy\":7,\"items\":["
                + "{\"kind\":\"DLQ\",\"id\":" + ids.get(0) + "},{\"kind\":\"DLQ\",\"id\":" + ids.get(1) + "},"
                + "{\"kind\":\"DLQ\",\"id\":999999},{\"kind\":\"RAW\",\"id\":" + rawId + "},{\"kind\":\"RAW\",\"id\":888888}]}", 200)
                .get("response");

        assertThat(response.get("results")).extracting(r -> r.get("outcome").asString())
                .containsExactly("FAILED", "SKIPPED", "SKIPPED", "FAILED", "SKIPPED");
        assertThat(response.get("results").get(1).get("errorCode").asString()).isEqualTo("ING_DLQ_ITEM_LOCKED");
        assertThat(response.get("results").get(0).get("errorCode").asString()).isEqualTo("ING_EXTERNAL_ID_MISSING");
        assertThat(jdbc.sql("SELECT attempts FROM data2flow_pipeline.dlq_items WHERE id = :id").param("id", ids.get(0))
                .query(Integer.class).single()).isGreaterThanOrEqualTo(2);
        StringBuilder many = new StringBuilder();
        for (int i = 0; i < 5001; i++) {
            many.append(i == 0 ? "" : ",").append("{\"kind\":\"DLQ\",\"id\":").append(i + 1).append('}');
        }
        JsonNode tooMany = post("/internal/pipeline/reprocess-items", "{\"organizationId\":1,\"requestedBy\":7,\"items\":[" + many
                + "]}", 400);
        assertThat(tooMany.get("header").get("resultCode").asString()).isEqualTo("ING_DLQ_BATCH_TOO_LARGE");
    }

    @Test
    @DisplayName("[ING-07.03][AT-ING-07.2] 일괄 폐기(API-ING-24) → DISCARDED·사유, 원본은 그대로")
    void discard() throws Exception {
        failingMessages(2);
        List<Long> ids = jdbc.sql("SELECT id FROM data2flow_pipeline.dlq_items ORDER BY id").query(Long.class).list();

        JsonNode response = post("/internal/pipeline/dlq-items/discard", "{\"organizationId\":1,\"requestedBy\":7,\"dlqItemIds\":"
                + ids + ",\"reason\":\"테스트 데이터\"}", 200).get("response");
        JsonNode invalid = post("/internal/pipeline/dlq-items/discard", "{\"organizationId\":1,\"requestedBy\":7,\"dlqItemIds\":[1],"
                + "\"reason\":\"x\"}", 400);

        assertThat(response.get("discarded").asInt()).isEqualTo(2);
        assertThat(jdbc.sql("SELECT DISTINCT discard_reason FROM data2flow_pipeline.dlq_items").query(String.class).single())
                .isEqualTo("테스트 데이터");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.raw_messages")).isEqualTo(2);
        assertThat(invalid.get("header").get("resultCode").asString()).isEqualTo("INVALID_REQUEST");
    }

    @Test
    @DisplayName("[ING-07.03][AT-ING-07.3] TC-ING-088 14일 지난 DLQ 항목 삭제, 13일 23시간 항목 유지, 원본은 원본 보관 규칙을 따른다")
    void purge() {
        failingMessages(2);
        jdbc.sql("UPDATE data2flow_pipeline.dlq_items SET created_at = :t WHERE id = (SELECT min(id) FROM data2flow_pipeline.dlq_items)")
                .param("t", java.sql.Timestamp.from(clock.instant().minus(Duration.ofDays(14)).minusSeconds(1))).update();
        jdbc.sql("UPDATE data2flow_pipeline.dlq_items SET created_at = :t WHERE id = (SELECT max(id) FROM data2flow_pipeline.dlq_items)")
                .param("t", java.sql.Timestamp.from(clock.instant().minus(Duration.ofDays(13)).minus(Duration.ofHours(23)))).update();

        PartitionMaintenanceService.Result result = partitions.maintain();

        assertThat(result.dlqPurged()).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.dlq_items")).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.raw_messages")).isEqualTo(2);
    }
}
