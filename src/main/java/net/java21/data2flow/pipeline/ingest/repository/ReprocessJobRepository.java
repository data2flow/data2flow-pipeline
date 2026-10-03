package net.java21.data2flow.pipeline.ingest.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** {@code reprocess_jobs}(ING-01.04, API-ING-23). 한 소스에 동시 작업 하나(uq_reprocess_jobs_source_running) */
@Repository
public class ReprocessJobRepository {

    private final JdbcClient jdbc;

    public ReprocessJobRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public long insert(long organizationId, long sourceId, Long[] deviceIds, Instant from, Instant to, long total,
                       String decoderVersion, String scriptVersions, long requestedBy, String memo, Instant now) {
        return jdbc.sql("""
                        INSERT INTO data2flow_pipeline.reprocess_jobs (organization_id, source_id, device_ids, period_from,
                            period_to, total, decoder_version, script_versions, requested_by, memo, created_at)
                        VALUES (:org, :source, :devices, :from, :to, :total, :decoder, CAST(:scripts AS jsonb), :by, :memo, :now)
                        RETURNING id""")
                .param("org", organizationId).param("source", sourceId).param("devices", deviceIds)
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to)).param("total", total)
                .param("decoder", decoderVersion).param("scripts", scriptVersions).param("by", requestedBy)
                .param("memo", memo).param("now", Timestamp.from(now))
                .query(Long.class).single();
    }

    /** 대상 원본 수(미리 보기·진행률 분모) */
    public long countTargets(long organizationId, long sourceId, Long[] deviceIds, Instant from, Instant to, boolean onlyFailed) {
        return jdbc.sql("""
                        SELECT count(*) FROM data2flow_pipeline.raw_messages
                         WHERE organization_id = :org AND source_id = :source AND received_at >= :from AND received_at < :to
                           AND (CAST(:devices AS bigint[]) IS NULL OR device_id = ANY(CAST(:devices AS bigint[])))
                           AND (NOT :failed OR status IN ('DECODE_ERROR','SCRIPT_ERROR','STORE_ERROR','PUBLISH_ERROR',
                                                         'UNKNOWN_DEVICE_REJECTED'))""")
                .param("org", organizationId).param("source", sourceId).param("devices", deviceIds)
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to)).param("failed", onlyFailed)
                .query(Long.class).single();
    }

    /** 다음 처리할 원본 ID들(id 순서, afterId 다음부터) */
    public List<RawRef> listTargets(long organizationId, long sourceId, Long[] deviceIds, Instant from, Instant to,
                                    boolean onlyFailed, long afterId, int limit) {
        return jdbc.sql("""
                        SELECT id, received_at FROM data2flow_pipeline.raw_messages
                         WHERE organization_id = :org AND source_id = :source AND received_at >= :from AND received_at < :to
                           AND (CAST(:devices AS bigint[]) IS NULL OR device_id = ANY(CAST(:devices AS bigint[])))
                           AND (NOT :failed OR status IN ('DECODE_ERROR','SCRIPT_ERROR','STORE_ERROR','PUBLISH_ERROR',
                                                         'UNKNOWN_DEVICE_REJECTED'))
                           AND id > :after AND status <> 'DUPLICATE'
                         ORDER BY id LIMIT :limit""")
                .param("org", organizationId).param("source", sourceId).param("devices", deviceIds)
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to)).param("failed", onlyFailed)
                .param("after", afterId).param("limit", limit)
                .query((rs, n) -> new RawRef(rs.getLong(1), rs.getTimestamp(2).toInstant())).list();
    }

    public Optional<String> findStatus(long organizationId, long jobId) {
        return jdbc.sql("SELECT status FROM data2flow_pipeline.reprocess_jobs WHERE organization_id = :org AND id = :id")
                .param("org", organizationId).param("id", jobId).query(String.class).optional();
    }

    public void updateProgress(long organizationId, long jobId, String status, long processed, long failed, Instant startedAt,
                               Instant finishedAt) {
        jdbc.sql("""
                        UPDATE data2flow_pipeline.reprocess_jobs SET status = :status, processed = :processed, failed = :failed,
                               started_at = coalesce(started_at, :started), finished_at = :finished
                         WHERE organization_id = :org AND id = :id AND status IN ('PENDING','RUNNING')""")
                .param("status", status).param("processed", processed).param("failed", failed)
                .param("started", startedAt == null ? null : Timestamp.from(startedAt))
                .param("finished", finishedAt == null ? null : Timestamp.from(finishedAt))
                .param("org", organizationId).param("id", jobId).update();
    }

    /** 취소: 끝나지 않은 작업만. 바꿨으면 true */
    public boolean updateCancelled(long organizationId, long jobId, Instant now) {
        return jdbc.sql("""
                        UPDATE data2flow_pipeline.reprocess_jobs SET status = 'CANCELLED', finished_at = :now
                         WHERE organization_id = :org AND id = :id AND status IN ('PENDING','RUNNING')""")
                .param("now", Timestamp.from(now)).param("org", organizationId).param("id", jobId).update() == 1;
    }

    /** 다른 인스턴스가 죽어 멈춘 작업은 정리하지 않는다(보고만, M5에서 넘겨받기) */
    @OrganizationScopeExempt("운영 지표용 전체 실행 중 작업 수")
    public long countRunning() {
        return jdbc.sql("SELECT count(*) FROM data2flow_pipeline.reprocess_jobs WHERE status = 'RUNNING'")
                .query(Long.class).single();
    }

    public record RawRef(long id, Instant receivedAt) {
    }
}
