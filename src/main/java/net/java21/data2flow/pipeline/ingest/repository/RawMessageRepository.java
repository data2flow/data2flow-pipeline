package net.java21.data2flow.pipeline.ingest.repository;

import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** {@code data2flow_pipeline.raw_messages}(일 파티션). 조회는 언제나 조직 조건을 건다 */
@Repository
public class RawMessageRepository {

    private static final String COLUMNS = """
            id, organization_id, source_id, device_id, message_id, source_type, topic, payload, payload_encoding,
            ingress_instance, dedup_key, stream_partition, stream_offset, external_id, status, error_code,
            error_detail::text AS error_detail, processing_trace::text AS processing_trace, metric_count, dropped,
            is_virtual, received_at, processed_at, signature_status""";

    private final JdbcClient jdbc;

    public RawMessageRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 원본 ID를 먼저 받는다(표준 메시지의 rawMessageId를 같은 트랜잭션에서 쓰기 위해) */
    public long allocateId() {
        return jdbc.sql("SELECT nextval(pg_get_serial_sequence('data2flow_pipeline.raw_messages', 'id'))")
                .query(Long.class).single();
    }

    /** 같은 스트림 메시지(messageId)를 이미 기록했는지(다시 읽음 → 재발행). received_at으로 파티션을 좁힌다 */
    public Optional<RawMessageRow> findByMessageId(long organizationId, Instant receivedAt, UUID messageId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_pipeline.raw_messages "
                        + "WHERE organization_id = :org AND received_at = :ra AND message_id = :mid ORDER BY id LIMIT 1")
                .param("org", organizationId).param("ra", Timestamp.from(receivedAt)).param("mid", messageId)
                .query(RawMessageRepository::map).optional();
    }

    /** 중복 판정 ①(BR-ING-07): 창 안에 같은 키로 이미 처리한 다른 메시지가 있는지 */
    public boolean existsDuplicate(long organizationId, String dedupKey, Instant from, Instant to, UUID messageId) {
        return jdbc.sql("""
                        SELECT EXISTS (SELECT 1 FROM data2flow_pipeline.raw_messages
                         WHERE organization_id = :org AND dedup_key = :key AND received_at >= :from AND received_at <= :to
                           AND message_id <> :mid AND status <> 'RECEIVED')""")
                .param("org", organizationId).param("key", dedupKey).param("from", Timestamp.from(from))
                .param("to", Timestamp.from(to)).param("mid", messageId)
                .query(Boolean.class).single();
    }

    public Optional<RawMessageRow> findById(long organizationId, long id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_pipeline.raw_messages WHERE organization_id = :org AND id = :id")
                .param("org", organizationId).param("id", id)
                .query(RawMessageRepository::map).optional();
    }

    public void insert(RawMessageRow r) {
        jdbc.sql("""
                        INSERT INTO data2flow_pipeline.raw_messages (id, organization_id, source_id, device_id, message_id,
                            source_type, topic, payload, payload_encoding, ingress_instance, dedup_key, stream_partition,
                            stream_offset, external_id, status, error_code, error_detail, processing_trace, metric_count,
                            dropped, is_virtual, received_at, processed_at, signature_status)
                        OVERRIDING SYSTEM VALUE
                        VALUES (:id, :org, :source, :device, :mid, :sourceType, :topic, :payload, :encoding, :ingress, :key,
                            :partition, :offset, :external, :status, :errorCode, CAST(:errorDetail AS jsonb),
                            CAST(:trace AS jsonb), :metricCount, :dropped, :virtual, :ra, :processedAt, :signature)""")
                .param("id", r.id()).param("org", r.organizationId()).param("source", r.sourceId())
                .param("device", r.deviceId()).param("mid", r.messageId()).param("sourceType", r.sourceType())
                .param("topic", r.topic()).param("payload", r.payload()).param("encoding", r.payloadEncoding())
                .param("ingress", r.ingressInstance()).param("key", r.dedupKey()).param("partition", r.streamPartition())
                .param("offset", r.streamOffset()).param("external", r.externalId()).param("status", r.status().name())
                .param("errorCode", r.errorCode()).param("errorDetail", r.errorDetail())
                .param("trace", r.processingTrace()).param("metricCount", r.metricCount()).param("dropped", r.dropped())
                .param("virtual", r.virtual()).param("ra", Timestamp.from(r.receivedAt()))
                .param("processedAt", r.processedAt() == null ? null : Timestamp.from(r.processedAt()))
                .param("signature", r.signatureStatus())
                .update();
    }

    /** 재처리 결과로 같은 행을 갱신한다(ING domain-model: 재처리는 같은 행의 상태를 갱신) */
    public void updateResult(long organizationId, RawMessageRow r) {
        jdbc.sql("""
                        UPDATE data2flow_pipeline.raw_messages
                           SET device_id = :device, external_id = :external, dedup_key = :key, status = :status,
                               error_code = :errorCode, error_detail = CAST(:errorDetail AS jsonb),
                               processing_trace = CAST(:trace AS jsonb), metric_count = :metricCount, dropped = :dropped,
                               processed_at = :processedAt
                         WHERE organization_id = :org AND id = :id AND received_at = :ra""")
                .param("device", r.deviceId()).param("external", r.externalId()).param("key", r.dedupKey())
                .param("status", r.status().name()).param("errorCode", r.errorCode())
                .param("errorDetail", r.errorDetail()).param("trace", r.processingTrace())
                .param("metricCount", r.metricCount()).param("dropped", r.dropped())
                .param("processedAt", r.processedAt() == null ? null : Timestamp.from(r.processedAt()))
                .param("org", organizationId).param("id", r.id()).param("ra", Timestamp.from(r.receivedAt()))
                .update();
    }

    static RawMessageRow map(ResultSet rs, int row) throws SQLException {
        Timestamp processed = rs.getTimestamp("processed_at");
        long device = rs.getLong("device_id");
        boolean deviceNull = rs.wasNull();
        int metricCount = rs.getInt("metric_count");
        boolean metricNull = rs.wasNull();
        return new RawMessageRow(rs.getLong("id"), rs.getLong("organization_id"), rs.getLong("source_id"),
                deviceNull ? null : device, rs.getObject("message_id", UUID.class), rs.getString("source_type"),
                rs.getString("topic"), rs.getBytes("payload"), rs.getString("payload_encoding"),
                rs.getString("ingress_instance"), rs.getString("dedup_key"), rs.getInt("stream_partition"),
                rs.getLong("stream_offset"), rs.getString("external_id"), RawMessageStatus.valueOf(rs.getString("status")),
                rs.getString("error_code"), rs.getString("error_detail"), rs.getString("processing_trace"),
                metricNull ? null : metricCount, rs.getBoolean("dropped"), rs.getBoolean("is_virtual"),
                rs.getTimestamp("received_at").toInstant(), processed == null ? null : processed.toInstant(),
                rs.getString("signature_status"));
    }
}
