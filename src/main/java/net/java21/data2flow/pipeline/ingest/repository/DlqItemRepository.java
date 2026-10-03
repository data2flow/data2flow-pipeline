package net.java21.data2flow.pipeline.ingest.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import net.java21.data2flow.pipeline.ingest.domain.DlqStage;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** {@code data2flow_pipeline.dlq_items}(ING-07.03). 상태: OPEN → REPROCESSING → RESOLVED | OPEN, OPEN → DISCARDED */
@Repository
public class DlqItemRepository {

    private final JdbcClient jdbc;

    public DlqItemRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 같은 원본의 열린 항목이 있으면 시도 횟수만 늘린다(uq_dlq_items_raw_message_open) */
    public void insert(long organizationId, long rawMessageId, Instant rawReceivedAt, long sourceId, Long deviceId,
                       DlqStage stage, String errorCode, String errorMessage, Instant now) {
        jdbc.sql("""
                        INSERT INTO data2flow_pipeline.dlq_items (organization_id, raw_message_id, raw_received_at, source_id,
                            device_id, stage, error_code, error_message, attempts, created_at)
                        VALUES (:org, :raw, :ra, :source, :device, :stage, :code, :message, 1, :now)
                        ON CONFLICT (raw_message_id) WHERE status IN ('OPEN','REPROCESSING')
                        DO UPDATE SET attempts = dlq_items.attempts + 1, stage = EXCLUDED.stage,
                            error_code = EXCLUDED.error_code, error_message = EXCLUDED.error_message,
                            status = 'OPEN', locked_by = NULL, locked_until = NULL""")
                .param("org", organizationId).param("raw", rawMessageId).param("ra", Timestamp.from(rawReceivedAt))
                .param("source", sourceId).param("device", deviceId).param("stage", stage.name())
                .param("code", errorCode).param("message", errorMessage == null ? "" : errorMessage)
                .param("now", Timestamp.from(now))
                .update();
    }

    /** 재처리 잠금(TTL 5분, ING_DLQ_ITEM_LOCKED). 잠근 항목만 돌려준다 */
    public List<DlqItem> lockForReprocess(long organizationId, Collection<Long> ids, long userId, Instant now, Instant until) {
        if (ids.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("""
                        UPDATE data2flow_pipeline.dlq_items
                           SET status = 'REPROCESSING', locked_by = :user, locked_until = :until
                         WHERE organization_id = :org AND id IN (:ids)
                           AND (status = 'OPEN' OR (status = 'REPROCESSING' AND locked_until < :now))
                        RETURNING id, raw_message_id, raw_received_at, stage, error_code, status, attempts""")
                .param("user", userId).param("until", Timestamp.from(until)).param("org", organizationId)
                .param("ids", ids).param("now", Timestamp.from(now))
                .query((rs, n) -> new DlqItem(rs.getLong("id"), rs.getLong("raw_message_id"),
                        rs.getTimestamp("raw_received_at").toInstant(), DlqStage.valueOf(rs.getString("stage")),
                        rs.getString("error_code"), rs.getString("status"), rs.getInt("attempts")))
                .list();
    }

    public List<DlqItem> findByIds(long organizationId, Collection<Long> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("""
                        SELECT id, raw_message_id, raw_received_at, stage, error_code, status, attempts
                          FROM data2flow_pipeline.dlq_items WHERE organization_id = :org AND id IN (:ids)""")
                .param("org", organizationId).param("ids", ids)
                .query((rs, n) -> new DlqItem(rs.getLong("id"), rs.getLong("raw_message_id"),
                        rs.getTimestamp("raw_received_at").toInstant(), DlqStage.valueOf(rs.getString("stage")),
                        rs.getString("error_code"), rs.getString("status"), rs.getInt("attempts")))
                .list();
    }

    public Optional<DlqItem> findOpenByRawMessage(long organizationId, long rawMessageId) {
        return jdbc.sql("""
                        SELECT id, raw_message_id, raw_received_at, stage, error_code, status, attempts
                          FROM data2flow_pipeline.dlq_items
                         WHERE organization_id = :org AND raw_message_id = :raw AND status IN ('OPEN','REPROCESSING')""")
                .param("org", organizationId).param("raw", rawMessageId)
                .query((rs, n) -> new DlqItem(rs.getLong("id"), rs.getLong("raw_message_id"),
                        rs.getTimestamp("raw_received_at").toInstant(), DlqStage.valueOf(rs.getString("stage")),
                        rs.getString("error_code"), rs.getString("status"), rs.getInt("attempts")))
                .optional();
    }

    /** 재처리 성공: RESOLVED */
    public void markResolved(long organizationId, long rawMessageId, Instant now) {
        jdbc.sql("""
                        UPDATE data2flow_pipeline.dlq_items SET status = 'RESOLVED', resolved_at = :now,
                               locked_by = NULL, locked_until = NULL
                         WHERE organization_id = :org AND raw_message_id = :raw AND status IN ('OPEN','REPROCESSING')""")
                .param("now", Timestamp.from(now)).param("org", organizationId).param("raw", rawMessageId).update();
    }

    /** 재처리 실패: 다시 OPEN(시도 수 증가) */
    public void markReopened(long organizationId, long id, String errorCode, String errorMessage) {
        jdbc.sql("""
                        UPDATE data2flow_pipeline.dlq_items SET status = 'OPEN', attempts = attempts + 1, locked_by = NULL,
                               locked_until = NULL, error_code = :code, error_message = :message
                         WHERE organization_id = :org AND id = :id AND status IN ('OPEN','REPROCESSING')""")
                .param("code", errorCode).param("message", errorMessage == null ? "" : errorMessage)
                .param("org", organizationId).param("id", id).update();
    }

    /** 일괄 폐기(API-ING-11): 원본은 그대로 둔다 */
    public int discard(long organizationId, Collection<Long> ids, String reason, Instant now) {
        if (ids.isEmpty()) {
            return 0;
        }
        return jdbc.sql("""
                        UPDATE data2flow_pipeline.dlq_items SET status = 'DISCARDED', discard_reason = :reason,
                               resolved_at = :now, locked_by = NULL, locked_until = NULL
                         WHERE organization_id = :org AND id IN (:ids) AND status IN ('OPEN','REPROCESSING')""")
                .param("reason", reason).param("now", Timestamp.from(now)).param("org", organizationId)
                .param("ids", ids).update();
    }

    /** 보관 14일이 지난 항목 삭제(BR-ING-14) */
    @OrganizationScopeExempt("모든 조직을 도는 보관 정리 작업")
    public int deleteCreatedBefore(Instant before) {
        return jdbc.sql("DELETE FROM data2flow_pipeline.dlq_items WHERE created_at < :before")
                .param("before", Timestamp.from(before)).update();
    }

    public record DlqItem(long id, long rawMessageId, Instant rawReceivedAt, DlqStage stage, String errorCode, String status,
                          int attempts) {
    }
}
