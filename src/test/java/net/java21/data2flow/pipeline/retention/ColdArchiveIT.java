package net.java21.data2flow.pipeline.retention;

import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.service.CoreDirectory;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import net.java21.data2flow.pipeline.partition.repository.PartitionRepository;
import net.java21.data2flow.pipeline.quality.repository.DataQualityRepository;
import net.java21.data2flow.pipeline.retention.repository.RetentionRepository;
import net.java21.data2flow.pipeline.retention.service.ArchiveService;
import net.java21.data2flow.pipeline.retention.service.RetentionPolicyCache;
import net.java21.data2flow.pipeline.retention.service.RetentionService;
import net.java21.data2flow.pipeline.retention.service.S3ObjectStore;
import net.java21.data2flow.pipeline.script.repository.ScriptOpsRepository;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * TSD-05.02·02.03 콜드 보관(BR-TSD-07·18): 보관 기간이 지난 한 달 원본을 Parquet(zstd)으로 S3 호환 저장소(SeaweedFS 컨테이너,
 * Apache-2.0 — 운영의 storage.java21.net과 같은 S3 API)에 올리고, 내려받아 체크섬을 확인하고, core에 등록한 뒤에만 지운다.
 */
class ColdArchiveIT extends IntegrationTestSupport {

    private static final String ACCESS = "d2f-test";
    private static final String SECRET = "d2f-test-secret";
    @SuppressWarnings("resource")
    private static final GenericContainer<?> SEAWEED = new GenericContainer<>("chrislusf/seaweedfs:3.97")
            .withCopyToContainer(Transferable.of("""
                    {"identities":[{"name":"d2f","credentials":[{"accessKey":"%s","secretKey":"%s"}],
                      "actions":["Admin","Read","Write","List","Tagging"]}]}""".formatted(ACCESS, SECRET)), "/etc/s3.json")
            .withCommand("server", "-dir=/data", "-s3", "-s3.port=8333", "-s3.config=/etc/s3.json", "-master.volumeSizeLimitMB=64")
            .withExposedPorts(8333)
            .waitingFor(Wait.forListeningPorts(8333).withStartupTimeout(Duration.ofMinutes(3)));

    static {
        SEAWEED.start();
    }

    @Autowired
    private RetentionRepository repository;
    @Autowired
    private PartitionRepository partitions;
    @Autowired
    private CoreDirectory core;
    @Autowired
    private DeviceDirectory devices;
    @Autowired
    private DomainEventPublisher events;
    @Autowired
    private DataQualityRepository quality;
    @Autowired
    private ScriptOpsRepository scriptOps;
    @Autowired
    private TransactionTemplate tx;
    @Autowired
    private PipelineProperties properties;

    private S3ObjectStore store;
    private RetentionService retention;

    @BeforeEach
    void storage() {
        PipelineProperties.Archive settings = new PipelineProperties.Archive(
                "http://" + SEAWEED.getHost() + ":" + SEAWEED.getMappedPort(8333), "data2flow-test", "us-east-1", ACCESS, SECRET,
                "archive");
        // 서명 시각은 실제 시각이어야 한다(저장소가 15분 넘는 시각 차이를 거부). 업무 시계(MutableClock)는 쓰지 않는다
        store = new S3ObjectStore(settings, java.time.Clock.systemUTC());
        await().atMost(Duration.ofMinutes(1)).ignoreExceptions().untilAsserted(() -> store.createBucket());
        retention = new RetentionService(repository, partitions, new RetentionPolicyCache(core, properties.retention(), clock),
                new ArchiveService(store, core, repository, settings.prefix()), devices, events, quality, scriptOps, tx,
                properties, clock);
    }

    /** 2025년 1월 한 달, 기기 2대 × 1분 간격(약 8.9만 행) */
    private void seedMonth(Instant from, Instant to, String partition) {
        if (!partitions.exists(partition)) {
            partitions.createPartition("telemetry", partition, from, to);
        }
        partitions.register(partition, "telemetry", from, to, clock.instant());
        jdbc.sql("""
                        INSERT INTO data2flow_pipeline.telemetry (device_id, metric_key, time, organization_id, value, quality,
                            received_at, raw_message_id)
                        SELECT d, 'temperature', t, 1, round((20 + 5 * sin(extract(epoch FROM t) / 3600.0))::numeric, 2), 0, t,
                               CASE WHEN d = 71 THEN NULL ELSE 1000 END
                          FROM generate_series(:from, CAST(:to AS timestamptz) - interval '1 minute', interval '1 minute') AS t,
                               (VALUES (71), (72)) AS v(d)""")
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to)).update();
        jdbc.sql("INSERT INTO data2flow_pipeline.device_state (device_id, organization_id) VALUES (71, 1), (72, 1)").update();
    }

    @Test
    @DisplayName("[TSD-05.02][AT-TSD-09.1] TC-TSD-045·TC-TSD-129 기간이 지난 달 → Parquet(zstd) 크기 ≤ DB 원본의 1/5, 체크섬 확인·core 등록 뒤 원본 삭제·파티션 ARCHIVED, retention.purged(archived=true)")
    void archivesThenDeletes() {
        Instant from = Instant.parse("2025-01-01T00:00:00Z");
        Instant to = Instant.parse("2025-02-01T00:00:00Z");
        seedMonth(from, to, "telemetry_y2025m01");
        long rows = count("SELECT count(*) FROM data2flow_pipeline.telemetry_y2025m01");
        CORE.retentionPolicy(1, "ORG", null, "TELEMETRY", 365, true, null);

        RetentionService.Report report = retention.run();

        assertThat(report.archived).singleElement().asString()
                .isEqualTo("archive/org-1/telemetry/2025/telemetry-1-20250101-20250201.parquet");
        assertThat(CORE.archiveFiles()).singleElement().satisfies(f -> {
            assertThat(f.get("organizationId").asString()).isEqualTo("1");
            assertThat(f.get("dataClass").asString()).isEqualTo("TELEMETRY");
            assertThat(f.get("rowsCount").asLong()).isEqualTo(rows);
            assertThat(f.get("format").asString()).isEqualTo("PARQUET");
        });
        Map<String, Object> export = jdbc.sql("SELECT * FROM data2flow_pipeline.archive_exports").query().singleRow();
        assertThat(export).containsEntry("registered", true);
        long bytes = ((Number) export.get("bytes")).longValue();
        long sourceBytes = ((Number) export.get("source_bytes")).longValue();
        assertThat(bytes * 5).as("Parquet 크기 ≤ DB 원본의 1/5(TSD-02.03)").isLessThanOrEqualTo(sourceBytes);

        byte[] file = store.get((String) export.get("object_key"));
        assertThat(new String(Arrays.copyOfRange(file, 0, 4), StandardCharsets.US_ASCII)).isEqualTo("PAR1");
        assertThat(S3ObjectStore.hex(S3ObjectStore.sha256(file))).isEqualTo(((String) export.get("checksum")).trim());
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry WHERE time < '2025-02-01'")).isZero();
        assertThat(report.droppedPartitions).contains("telemetry_y2025m01");
        assertThat(jdbc.sql("SELECT state FROM data2flow_pipeline.partition_registries WHERE partition_name = 'telemetry_y2025m01'")
                .query(String.class).single()).isEqualTo("ARCHIVED");
        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(events("retention.purged"))
                .anySatisfy(e -> {
                    JsonNode p = e.get("payload");
                    assertThat(p.get("archived").asBoolean()).isTrue();
                    assertThat(p.get("rows").asLong()).isEqualTo(rows);
                }));
    }

    @Test
    @DisplayName("[TSD-05.02][BR-TSD-18] core 등록이 실패하면 원본을 지우지 않고, 다음 밤에 다시 올려 등록한 뒤 지운다")
    void keepsRowsUntilRegistered() {
        Instant from = Instant.parse("2025-02-01T00:00:00Z");
        Instant to = Instant.parse("2025-03-01T00:00:00Z");
        seedMonth(from, to, "telemetry_y2025m02");
        long rows = count("SELECT count(*) FROM data2flow_pipeline.telemetry_y2025m02");
        CORE.retentionPolicy(1, "ORG", null, "TELEMETRY", 365, true, null);
        CORE.archiveRegistrationFails(true);

        RetentionService.Report failed = retention.run();

        assertThat(failed.failures).isPositive();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_y2025m02")).isEqualTo(rows);
        assertThat(partitions.exists("telemetry_y2025m02")).isTrue();
        assertThat(jdbc.sql("SELECT registered FROM data2flow_pipeline.archive_exports").query(Boolean.class).single()).isFalse();

        CORE.archiveRegistrationFails(false);
        retention.run();

        assertThat(CORE.archiveFiles()).hasSize(1);
        assertThat(partitions.exists("telemetry_y2025m02")).isFalse();
        assertThat(jdbc.sql("SELECT registered FROM data2flow_pipeline.archive_exports").query(Boolean.class).single()).isTrue();
    }
}
