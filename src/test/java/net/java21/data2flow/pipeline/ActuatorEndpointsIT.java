package net.java21.data2flow.pipeline;

import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalManagementPort;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * M0 완료 기준 + NFR-02.03(프로브): 서비스가 뜨고 관리 포트에서 프로브와 지표가 나온다. readiness에는 스트림 소비자가 들어간다
 * (reliability-and-ha.md §4.2). OPS-01.02 수집 지표 이름도 확인한다.
 */
class ActuatorEndpointsIT extends IntegrationTestSupport {

    @LocalManagementPort
    int managementPort;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    @DisplayName("[NFR-02.03] liveness·readiness 프로브가 UP(readiness는 data2flow.raw 소비자 포함)")
    void probesAreUp() {
        await().atMost(java.time.Duration.ofSeconds(60)).untilAsserted(() -> {
            for (String path : new String[]{"/actuator/health/liveness", "/actuator/health/readiness"}) {
                HttpResponse<String> res = get(path);
                assertThat(res.statusCode()).as(path).isEqualTo(200);
                assertThat(res.body()).as(path).contains("\"UP\"");
            }
        });
    }

    @Test
    @DisplayName("[OPS-01.02] Prometheus 지표에 application 태그와 처리 지연 게이지가 있다")
    void prometheusMetrics() throws Exception {
        HttpResponse<String> res = get("/actuator/prometheus");
        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(res.body()).contains("application=\"data2flow-pipeline\"")
                .contains("data2flow_ingest_lag_seconds");
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + managementPort + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
