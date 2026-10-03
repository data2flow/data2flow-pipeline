package net.java21.data2flow.pipeline.nfr;

import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestInfrastructure;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * NFR-02.02 TC-NFR-015 · ING-01.03 TC-ING-019: DB가 응답하지 않는 동안(컨테이너 일시 정지, 기본 5분) 원본 300건을 계속 받아도
 * 하나도 잃지 않는다. 처리는 일시 장애로 보고 오프셋을 넘기지 않은 채 백오프로 기다리고(스트림에 보관), DB가 돌아오면 이어서 처리한다.
 * 장애 시간은 {@code -Dnfr.db-outage=PT5M}(기본 5분, 스펙 수치. 개발 중에는 짧게 줄여 돌릴 수 있다).
 */
class DbOutageZeroLossIT extends IntegrationTestSupport {

    private static final int TOTAL = 300;

    @Test
    @DisplayName("[NFR-02.02][ING-01.03][AT-ING-01.3] TC-ING-019·TC-NFR-015 DB 5분 중단 중 300건 → 복구 후 count=300, 고유 seq=300, 원본 OK 300")
    void dbOutage() {
        Duration outage = Duration.parse(System.getProperty("nfr.db-outage", "PT5M"));
        CORE.source(5, 1, "single-value", "AUTO_REGISTER", null);
        for (int d = 0; d < 5; d++) {
            CORE.device(500 + d, 1, 5, "outage-" + d, "ACTIVE", null, null, 60);
        }
        String containerId = TestInfrastructure.POSTGRES.getContainerId();
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(containerId).exec();
        long started = System.nanoTime();
        try {
            for (int seq = 1; seq <= TOTAL; seq++) {
                publish(message(seq));
            }
            long remaining = outage.toNanos() - (System.nanoTime() - started);
            while (remaining > 0) {
                LockSupport.parkNanos(Math.min(remaining, Duration.ofSeconds(5).toNanos()));
                remaining = outage.toNanos() - (System.nanoTime() - started);
            }
        } finally {
            docker.unpauseContainerCmd(containerId).exec();
        }

        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(1)).ignoreExceptions()
                .until(() -> count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE device_id BETWEEN 500 AND 504") >= TOTAL);

        Map<String, Object> result = jdbc.sql("""
                        SELECT count(*) AS rows, count(DISTINCT value) AS seqs FROM data2flow_pipeline.telemetry
                         WHERE device_id BETWEEN 500 AND 504""").query().singleRow();
        long ok = count("SELECT count(*) FROM data2flow_pipeline.raw_messages WHERE source_id = 5 AND status = 'OK'");
        System.out.printf("[NFR-02.02] DB 중단 %ds 동안 %d건 → telemetry 행 %s, 고유 seq %s, 원본 OK %d%n", outage.toSeconds(),
                TOTAL, result.get("rows"), result.get("seqs"), ok);
        assertThat(((Number) result.get("rows")).longValue()).as("유실 0").isEqualTo(TOTAL);
        assertThat(((Number) result.get("seqs")).longValue()).as("중복 0").isEqualTo(TOTAL);
        assertThat(ok).isEqualTo(TOTAL);
    }

    private RawEnvelope message(int seq) {
        String topic = "sensors/outage-" + (seq % 5) + "/seq";
        byte[] payload = Integer.toString(seq).getBytes(StandardCharsets.UTF_8);
        return new RawEnvelope(RawEnvelope.VERSION, UUID.randomUUID(), 1, 5, "WEBHOOK", topic, payload,
                clock.instant().plusSeconds(seq), "data2flow-ingress-0", DedupKeys.detect(5, topic, payload), false, null);
    }
}
