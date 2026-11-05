package net.java21.data2flow.pipeline.telemetry;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** API-ING-25 계산값 저장(ENE-02.02): 공간 가상 기기의 telemetry(계산값 flags 16)·최신값·표준 메시지(derived) */
class DerivedPointsIT extends IntegrationTestSupport {

    private final HttpClient http = HttpClient.newHttpClient();

    private HttpResponse<String> post(String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/internal/pipeline/derived-points"))
                .header("Content-Type", "application/json").header("X-CALLER-SERVICE", "data2flow-analytics")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @DisplayName("[ENE-02.02][API-ING-25] 통과한 점만 저장하고(거부 사유는 index별), 최신값을 갱신하고, derived 표준 메시지를 낸다")
    void storesDerivedPoints() throws Exception {
        clock.set(Instant.parse("2026-10-03T01:30:00Z"));
        CORE.device(901, 1, 9, "space-31", "ACTIVE", null, 31L, 3600);
        CORE.device(902, 2, 19, "space-77", "ACTIVE", null, 77L, 3600);
        Instant t1 = Instant.parse("2026-10-03T00:55:00Z");
        Instant t2 = Instant.parse("2026-10-03T01:00:00Z");
        String body = """
                {"orgId": 1, "source": "DERIVED", "points": [
                  {"deviceId": 901, "metricKey": "occupancy_est", "ts": "%s", "value": 0, "estimated": true},
                  {"deviceId": 901, "metricKey": "occupancy_est", "ts": "%s", "value": 1, "estimated": true,
                   "meta": {"method": "rule-based", "signals": ["pir", "co2"]}},
                  {"deviceId": 999, "metricKey": "occupancy_est", "ts": "%s", "value": 1},
                  {"deviceId": 902, "metricKey": "occupancy_est", "ts": "%s", "value": 1},
                  {"deviceId": 901, "metricKey": "Bad Key", "ts": "%s", "value": 1},
                  {"deviceId": 901, "metricKey": "occupancy_est", "ts": "2026-10-03T02:00:00Z", "value": 1}
                ]}""".formatted(t1, t2, t2, t2, t2);

        HttpResponse<String> res = post(body);

        assertThat(res.statusCode()).as(res.body()).isEqualTo(200);
        JsonNode r = CODEC.mapper().readTree(res.body()).get("response");
        assertThat(r.get("accepted").asInt()).isEqualTo(2);
        assertThat(r.get("rejected").toString()).contains("{\"index\":2,\"reason\":\"UNKNOWN_DEVICE\"}",
                "{\"index\":3,\"reason\":\"UNKNOWN_DEVICE\"}", "{\"index\":4,\"reason\":\"INVALID_METRIC\"}",
                "{\"index\":5,\"reason\":\"FUTURE_TIME\"}");
        List<Map<String, Object>> rows = jdbc.sql("""
                        SELECT value, quality::int AS quality, flags::int AS flags FROM data2flow_pipeline.telemetry
                         WHERE device_id = 901 AND metric_key = 'occupancy_est' ORDER BY time""").query().listOfRows();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(1)).containsEntry("value", 1.0).containsEntry("quality", 0).containsEntry("flags", 16);
        String latest = jdbc.sql("SELECT latest::text FROM data2flow_pipeline.device_state WHERE device_id = 901").query(String.class).single();
        assertThat(CODEC.mapper().readTree(latest).get("occupancy_est").get("v").asDouble()).isEqualTo(1.0);

        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(telemetry.telemetry()).hasSize(2));
        CanonicalTelemetry last = telemetry.telemetry().stream().filter(t -> t.measuredAt().equals(t2)).findFirst().orElseThrow();
        assertThat(last.deviceId()).isEqualTo(901);
        assertThat(last.spaceId()).isEqualTo(31L);
        assertThat(last.metric("occupancy_est").derived()).isTrue();
        assertThat(last.meta().extra().get("derived").get("meta").get("method").asString()).isEqualTo("rule-based");

        // 같은 점을 다시 보내면 덮어쓴다(행 수 그대로)
        assertThat(post("""
                {"orgId": 1, "points": [{"deviceId": 901, "metricKey": "occupancy_est", "ts": "%s", "value": 0}]}""".formatted(t2))
                .statusCode()).isEqualTo(200);
        assertThat(jdbc.sql("SELECT value FROM data2flow_pipeline.telemetry WHERE device_id = 901 AND time = :t")
                .param("t", java.sql.Timestamp.from(t2)).query(Double.class).single()).isZero();
    }

    @Test
    @DisplayName("[API-ING-25] 출처가 DERIVED가 아니거나 점이 없으면 400")
    void rejectsBadRequests() throws Exception {
        assertThat(post("{\"orgId\": 1, \"source\": \"SIM\", \"points\": [{\"deviceId\": 1}]}").statusCode()).isEqualTo(400);
        assertThat(post("{\"orgId\": 1, \"points\": []}").statusCode()).isEqualTo(400);
    }
}
