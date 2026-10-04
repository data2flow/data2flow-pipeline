package net.java21.data2flow.pipeline.ingest.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * {@code reprocess_jobs}(ING-01.04, API-ING-23). 한 소스에 동시 작업 하나(uq_reprocess_jobs_source_running).
 * 작업은 맡은 인스턴스({@code owner_instance})만 진행을 기록하고, 생존 신호({@code heartbeat_at})가 멈추면 다른 인스턴스가
 * {@link #claim}으로 넘겨받아 {@code last_raw_id} 다음부터 잇는다.
 */
@Repository
public class ReprocessJobRepository {

    private static final String COLUMNS = """
            id, organization_id, source_id, device_ids, period_from, period_to, status, total, processed, failed, skipped,
            last_raw_id, only_failed, requested_by, pinned_bundle::text AS pinned_bundle, owner_instance, heartbeat_at,
            started_at, finished_at, error""";
    private static final String FAILED_STATUSES = "('DECODE_ERROR','SCRIPT_ERROR','STORE_ERROR','PUBLISH_ERROR','UNKNOWN_DEVICE_REJECTED')";

    private final JdbcClient jdbc;

    public ReprocessJobRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public long insert(NewJob job) {
        return jdbc.sql("""
                        INSERT INTO data2flow_pipeline.reprocess_jobs (organization_id, source_id, device_ids, period_from,
                            period_to, total, decoder_version, script_versions, requested_by, memo, created_at, only_failed,
                            pinned_bundle, owner_instance, heartbeat_at)
                        VALUES (:org, :source, :devices, :from, :to, :total, :decoder, CAST(:scripts AS jsonb), :by, :memo, :now,
                            :onlyFailed, CAST(:bundle AS jsonb), :owner, :now)
                        RETURNING id""")
                .param("org", job.organizationId()).param("source", job.sourceId()).param("devices", job.deviceIds())
                .param("from", Timestamp.from(job.from())).param("to", Timestamp.from(job.to())).param("total", job.total())
                .param("decoder", job.decoderVersion()).param("scripts", job.scriptVersions())
                .param("by", job.requestedBy()).param("memo", job.memo()).param("now", Timestamp.from(job.now()))
                .param("onlyFailed", job.onlyFailed()).param("bundle", job.pinnedBundle()).param("owner", job.owner())
                .query(Long.class).single();
    }

    /** 대상 원본 수(미리 보기·진행률 분모) */
    public long countTargets(long organizationId, long sourceId, Long[] deviceIds, Instant from, Instant to, boolean onlyFailed) {
        return jdbc.sql("""
                        SELECT count(*) FROM data2flow_pipeline.raw_messages
                         WHERE organization_id = :org AND source_id = :source AND received_at >= :from AND received_at < :to
                           AND (CAST(:devices AS bigint[]) IS NULL OR device_id = ANY(CAST(:devices AS bigint[])))
                           AND (NOT :failed OR status IN """ + FAILED_STATUSES + ")"
                        + " AND status <> 'DUPLICATE'")
                .param("org", organizationId).param("source", sourceId).param("devices", deviceIds)
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to)).param("failed", onlyFailed)
                .query(Long.class).single();
    }

    /** 다음 처리할 원본 ID들(id 순서, afterId 다음부터) */
    public List<RawRef> listTargets(long organizationId, Job job, long afterId, int limit) {
        return jdbc.sql("""
                        SELECT id, received_at FROM data2flow_pipeline.raw_messages
                         WHERE organization_id = :org AND source_id = :source AND received_at >= :from AND received_at < :to
                           AND (CAST(:devices AS bigint[]) IS NULL OR device_id = ANY(CAST(:devices AS bigint[])))
                           AND (NOT :failed OR status IN """ + FAILED_STATUSES + """
                        )
                           AND id > :after AND status <> 'DUPLICATE'
                         ORDER BY id LIMIT :limit""")
                .param("org", organizationId).param("source", job.sourceId()).param("devices", job.deviceIdArray())
                .param("from", Timestamp.from(job.from())).param("to", Timestamp.from(job.to()))
                .param("failed", job.onlyFailed()).param("after", afterId).param("limit", limit)
                .query((rs, n) -> new RawRef(rs.getLong(1), rs.getTimestamp(2).toInstant())).list();
    }

    public Optional<String> findStatus(long organizationId, long jobId) {
        return jdbc.sql("SELECT status FROM data2flow_pipeline.reprocess_jobs WHERE organization_id = :org AND id = :id")
                .param("org", organizationId).param("id", jobId).query(String.class).optional();
    }

    public Optional<Job> find(long organizationId, long jobId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_pipeline.reprocess_jobs WHERE organization_id = :org AND id = :id")
                .param("org", organizationId).param("id", jobId).query(ReprocessJobRepository::map).optional();
    }

    /**
     * 작업을 맡는다: 대기(PENDING) 작업이거나, 생존 신호가 {@code staleBefore}보다 오래된 작업(맡은 인스턴스가 죽음)만.
     * 맡았으면 작업, 아니면 빈 값(다른 인스턴스가 이미 맡음·끝남·취소됨).
     */
    @OrganizationScopeExempt("작업 ID로 넘겨받기(ID는 조직이 정한 작업만 들어 있음)")
    public Optional<Job> claim(long jobId, String instance, Instant now, Instant staleBefore) {
        return jdbc.sql("""
                        UPDATE data2flow_pipeline.reprocess_jobs SET status = 'RUNNING', owner_instance = :instance,
                               heartbeat_at = :now, started_at = coalesce(started_at, :now)
                         WHERE id = :id AND status IN ('PENDING','RUNNING')
                           AND ((status = 'PENDING' AND (owner_instance = :instance OR heartbeat_at IS NULL
                                                         OR heartbeat_at < :stale))
                             OR (status = 'RUNNING' AND heartbeat_at < :stale))
                        RETURNING """ + COLUMNS)
                .param("instance", instance).param("now", Timestamp.from(now)).param("id", jobId)
                .param("stale", Timestamp.from(staleBefore))
                .query(ReprocessJobRepository::map).optional();
    }

    /** 넘겨받을 작업: 생존 신호가 멈춘 대기·실행 작업 */
    @OrganizationScopeExempt("모든 조직의 멈춘 작업 찾기(인스턴스 장애 복구)")
    public List<Long> findStale(Instant staleBefore) {
        return jdbc.sql("""
                        SELECT id FROM data2flow_pipeline.reprocess_jobs
                         WHERE status IN ('PENDING','RUNNING') AND (heartbeat_at IS NULL OR heartbeat_at < :stale)
                         ORDER BY id""")
                .param("stale", Timestamp.from(staleBefore)).query(Long.class).list();
    }

    /** 진행 기록 + 생존 신호. 맡은 인스턴스가 아니거나 더는 RUNNING이 아니면(취소·넘겨줌) false */
    public boolean updateProgress(long organizationId, long jobId, String instance, Progress p, Instant now) {
        return jdbc.sql("""
                        UPDATE data2flow_pipeline.reprocess_jobs SET processed = :processed, failed = :failed, skipped = :skipped,
                               last_raw_id = :last, heartbeat_at = :now
                         WHERE organization_id = :org AND id = :id AND status = 'RUNNING' AND owner_instance = :instance""")
                .param("processed", p.processed()).param("failed", p.failed()).param("skipped", p.skipped())
                .param("last", p.lastRawId()).param("now", Timestamp.from(now)).param("org", organizationId)
                .param("id", jobId).param("instance", instance).update() == 1;
    }

    /** 끝: 맡은 인스턴스가 RUNNING 작업을 COMPLETED·FAILED로. 바꿨으면 true */
    public boolean finish(long organizationId, long jobId, String instance, String status, Progress p, String error,
                          Instant now) {
        return jdbc.sql("""
                        UPDATE data2flow_pipeline.reprocess_jobs SET status = :status, processed = :processed, failed = :failed,
                               skipped = :skipped, last_raw_id = :last, finished_at = :now, heartbeat_at = :now, error = :error
                         WHERE organization_id = :org AND id = :id AND status = 'RUNNING' AND owner_instance = :instance""")
                .param("status", status).param("processed", p.processed()).param("failed", p.failed())
                .param("skipped", p.skipped()).param("last", p.lastRawId()).param("now", Timestamp.from(now))
                .param("error", error == null ? null : error.length() > 500 ? error.substring(0, 500) : error)
                .param("org", organizationId).param("id", jobId).param("instance", instance).update() == 1;
    }

    /** 취소: 끝나지 않은 작업만. 바꿨으면 취소된 작업 */
    public Optional<Job> updateCancelled(long organizationId, long jobId, Instant now) {
        return jdbc.sql("""
                        UPDATE data2flow_pipeline.reprocess_jobs SET status = 'CANCELLED', finished_at = :now
                         WHERE organization_id = :org AND id = :id AND status IN ('PENDING','RUNNING')
                        RETURNING """ + COLUMNS)
                .param("now", Timestamp.from(now)).param("org", organizationId).param("id", jobId)
                .query(ReprocessJobRepository::map).optional();
    }

    @OrganizationScopeExempt("운영 지표용 전체 실행 중 작업 수")
    public long countRunning() {
        return jdbc.sql("SELECT count(*) FROM data2flow_pipeline.reprocess_jobs WHERE status = 'RUNNING'")
                .query(Long.class).single();
    }

    static Job map(ResultSet rs, int n) throws SQLException {
        Array devices = rs.getArray("device_ids");
        List<Long> deviceIds = devices == null ? List.of() : Arrays.stream((Long[]) devices.getArray()).toList();
        Timestamp heartbeat = rs.getTimestamp("heartbeat_at");
        Timestamp started = rs.getTimestamp("started_at");
        Timestamp finished = rs.getTimestamp("finished_at");
        long requestedBy = rs.getLong("requested_by");
        return new Job(rs.getLong("id"), rs.getLong("organization_id"), rs.getLong("source_id"), deviceIds,
                rs.getTimestamp("period_from").toInstant(), rs.getTimestamp("period_to").toInstant(), rs.getString("status"),
                rs.getLong("total"), new Progress(rs.getLong("processed"), rs.getLong("failed"), rs.getLong("skipped"),
                rs.getLong("last_raw_id")), rs.getBoolean("only_failed"), requestedBy, rs.getString("pinned_bundle"),
                rs.getString("owner_instance"), heartbeat == null ? null : heartbeat.toInstant(),
                started == null ? null : started.toInstant(), finished == null ? null : finished.toInstant(),
                rs.getString("error"));
    }

    /** 새 작업 */
    public record NewJob(long organizationId, long sourceId, Long[] deviceIds, Instant from, Instant to, long total,
                         String decoderVersion, String scriptVersions, long requestedBy, String memo, boolean onlyFailed,
                         String pinnedBundle, String owner, Instant now) {
    }

    /** 진행 */
    public record Progress(long processed, long failed, long skipped, long lastRawId) {
    }

    /** 작업 한 행 */
    public record Job(long id, long organizationId, long sourceId, List<Long> deviceIds, Instant from, Instant to,
                      String status, long total, Progress progress, boolean onlyFailed, long requestedBy,
                      String pinnedBundle, String owner, Instant heartbeatAt, Instant startedAt, Instant finishedAt,
                      String error) {

        public Long[] deviceIdArray() {
            return deviceIds.isEmpty() ? null : deviceIds.toArray(Long[]::new);
        }
    }

    public record RawRef(long id, Instant receivedAt) {
    }
}
