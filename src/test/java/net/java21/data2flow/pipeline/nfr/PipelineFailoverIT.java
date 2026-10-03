package net.java21.data2flow.pipeline.nfr;

import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import net.java21.data2flow.pipeline.support.CoreApiStub;
import net.java21.data2flow.pipeline.support.MutableClock;
import net.java21.data2flow.pipeline.support.PipelineProcess;
import net.java21.data2flow.pipeline.support.TestInfrastructure;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * NFR-02.05 TC-NFR-018·NFR-02.03 TC-NFR-016·ING-01.03 TC-ING-020(TC-ING-018 포함): pipeline 인스턴스 2개(별도 JVM, Single Active
 * Consumer)가 처리하는 중 한 인스턴스를 <b>kill -9</b> 하면 다른 인스턴스가 파티션을 넘겨받아 저장된 오프셋 다음부터 이어서 처리한다.
 * 1,000건 seq 기준 유실 0·중복 0(telemetry 행 = 고유 seq = 1,000, 원본 행 = 1,000). 죽은 인스턴스를 다시 띄우면 대기 소비자로
 * 합류하고(자동 재시작 시뮬레이션), 넘겨받기부터 처리 재개까지 2분 안.
 * <p>다른 시험과 섞이지 않도록 별도 vhost {@code nfr}와 조직 77을 쓴다.
 */
class PipelineFailoverIT {

    private static final String VHOST = "nfr";
    private static final long ORG = 77;
    private static final long SOURCE = 770;
    private static final int TOTAL = 1000;
    private static final CoreApiStub CORE = TestInfrastructure.CORE;
    private static JdbcClient jdbc;
    private static TestStreams streams;
    private static PipelineProcess a;
    private static PipelineProcess b;

    @BeforeAll
    static void startInstances() {
        TestInfrastructure.createVhost(VHOST);
        jdbc = JdbcClient.create(new DriverManagerDataSource(TestInfrastructure.POSTGRES.getJdbcUrl(),
                TestInfrastructure.POSTGRES.getUsername(), TestInfrastructure.POSTGRES.getPassword()));
        migrateIfNeeded();
        CORE.source(SOURCE, ORG, "single-value", "AUTO_REGISTER", null);
        for (int d = 0; d < 10; d++) {
            CORE.device(7700 + d, ORG, SOURCE, "nfr-" + d, "ACTIVE", null, null, 60);
        }
        streams = new TestStreams(VHOST, 3);
        a = PipelineProcess.start("pipeline-nfr-a", VHOST);
        b = PipelineProcess.start("pipeline-nfr-b", VHOST);
        a.awaitReady(Duration.ofMinutes(3));
        b.awaitReady(Duration.ofMinutes(3));
    }

    /** 이 JVM의 Spring 문맥이 아직 마이그레이션하지 않았으면 같은 Flyway 파일로 만든다 */
    private static void migrateIfNeeded() {
        Boolean exists = jdbc.sql("SELECT to_regclass('data2flow_pipeline.raw_messages') IS NOT NULL").query(Boolean.class).single();
        if (!exists) {
            org.flywaydb.core.Flyway.configure()
                    .dataSource(TestInfrastructure.POSTGRES.getJdbcUrl(), TestInfrastructure.POSTGRES.getUsername(),
                            TestInfrastructure.POSTGRES.getPassword())
                    .schemas("data2flow_pipeline").defaultSchema("data2flow_pipeline").createSchemas(true).load().migrate();
        }
    }

    @AfterAll
    static void stopInstances() {
        if (a != null) {
            a.close();
        }
        if (b != null) {
            b.close();
        }
        if (streams != null) {
            streams.close();
        }
    }

    private static RawEnvelope message(int seq) {
        String topic = "sensors/nfr-" + (seq % 10) + "/seq";
        byte[] payload = Integer.toString(seq).getBytes(StandardCharsets.UTF_8);
        Instant receivedAt = MutableClock.T0.plusSeconds(seq);
        return new RawEnvelope(RawEnvelope.VERSION, UUID.randomUUID(), ORG, SOURCE, "WEBHOOK", topic, payload, receivedAt,
                "data2flow-ingress-0", DedupKeys.detect(SOURCE, topic, payload), false, null);
    }

    private static long stored() {
        return jdbc.sql("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE organization_id = :org")
                .param("org", ORG).query(Long.class).single();
    }

    @Test
    @DisplayName("[NFR-02.05][NFR-02.03][ING-01.03][AT-ING-01.5] TC-ING-020·TC-NFR-018 인스턴스 2개 중 1개 kill -9 → 다른 1개가 넘겨받아 1,000건 유실 0·중복 0")
    void killOneInstance() {
        var published = streams.collectTelemetry();
        for (int seq = 1; seq <= TOTAL / 2; seq++) {
            streams.publish(message(seq));
        }
        await().atMost(Duration.ofMinutes(2)).until(() -> stored() >= 150);

        Instant killedAt = Instant.now(java.time.Clock.systemUTC());
        a.kill();
        for (int seq = TOTAL / 2 + 1; seq <= TOTAL; seq++) {
            streams.publish(message(seq));
        }
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(1)).until(() -> stored() >= TOTAL);
        Duration recovery = Duration.between(killedAt, Instant.now(java.time.Clock.systemUTC()));

        // 자동 재시작(k8s 재시작 시뮬레이션): 다시 뜬 인스턴스는 대기 소비자로 합류하고 이미 처리한 것을 다시 저장하지 않는다
        a = PipelineProcess.start("pipeline-nfr-a2", VHOST).awaitReady(Duration.ofMinutes(3));

        Map<String, Object> result = jdbc.sql("""
                        SELECT count(*) AS rows, count(DISTINCT value) AS seqs, min(value) AS lo, max(value) AS hi
                          FROM data2flow_pipeline.telemetry WHERE organization_id = :org""")
                .param("org", ORG).query().singleRow();
        long raws = jdbc.sql("SELECT count(*) FROM data2flow_pipeline.raw_messages WHERE organization_id = :org")
                .param("org", ORG).query(Long.class).single();
        long ok = jdbc.sql("SELECT count(*) FROM data2flow_pipeline.raw_messages WHERE organization_id = :org AND status = 'OK'")
                .param("org", ORG).query(Long.class).single();
        System.out.printf("[NFR-02.05] 보냄 %d, telemetry 행 %s, 고유 seq %s, 원본 행 %d(OK %d), kill 후 처리 완료까지 %ds%n", TOTAL,
                result.get("rows"), result.get("seqs"), raws, ok, recovery.toSeconds());

        assertThat(((Number) result.get("rows")).longValue()).as("유실 0").isEqualTo(TOTAL);
        assertThat(((Number) result.get("seqs")).longValue()).as("중복 0").isEqualTo(TOTAL);
        assertThat(((Number) result.get("lo")).doubleValue()).isEqualTo(1);
        assertThat(((Number) result.get("hi")).doubleValue()).isEqualTo(TOTAL);
        assertThat(raws).as("원본 중복 기록 0(다시 읽은 메시지는 재발행만)").isEqualTo(TOTAL);
        assertThat(ok).isEqualTo(TOTAL);
        var byInstance = jdbc.sql("""
                        SELECT processing_trace->>'instance' AS instance, count(*) AS n FROM data2flow_pipeline.raw_messages
                         WHERE organization_id = :org GROUP BY 1""").param("org", ORG).query().listOfRows();
        System.out.println("[NFR-02.05] 인스턴스별 처리: " + byInstance);
        assertThat(byInstance).as("두 인스턴스가 파티션을 나눠 처리했고 죽은 쪽 파티션을 넘겨받았다")
                .anySatisfy(r -> assertThat(r.get("instance")).isEqualTo("pipeline-nfr-a"))
                .anySatisfy(r -> assertThat(r.get("instance")).isEqualTo("pipeline-nfr-b"));
        assertThat(recovery).as("NFR-02.03 2분 안 수집 재개").isLessThan(Duration.ofMinutes(2));
        await().atMost(Duration.ofMinutes(1)).until(() -> published.telemetry().stream()
                .map(t -> t.messageId()).distinct().count() == TOTAL);
        published.close();
        assertThat(b.alive()).isTrue();
    }
}
