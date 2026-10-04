package net.java21.data2flow.pipeline.script.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** 스크립트 운영 기록 표({@code script_stats_1m}·{@code script_stats_1h}·{@code script_errors}·{@code script_logs}, SCR-03.05·05.01·05.02) */
@Repository
public class ScriptOpsRepository {

    private static final String STAT_COLUMNS = """
            script_id, version_id, minute, organization_id, version_no, processed, errors, timeouts, avg_ms, p95_ms, max_ms,
            max_input_bytes, logs_dropped""";

    private final JdbcTemplate jdbc;

    public ScriptOpsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** 1분 지표를 더한다(건수 합, 평균은 가중 평균, p95·최대는 큰 값) */
    public void upsertStats(List<StatRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        jdbc.batchUpdate("INSERT INTO data2flow_pipeline.script_stats_1m AS s (" + STAT_COLUMNS + """
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (script_id, version_id, minute) DO UPDATE SET
                    avg_ms = (s.avg_ms * s.processed + EXCLUDED.avg_ms * EXCLUDED.processed)
                             / nullif(s.processed + EXCLUDED.processed, 0),
                    processed = s.processed + EXCLUDED.processed, errors = s.errors + EXCLUDED.errors,
                    timeouts = s.timeouts + EXCLUDED.timeouts, p95_ms = greatest(s.p95_ms, EXCLUDED.p95_ms),
                    max_ms = greatest(s.max_ms, EXCLUDED.max_ms),
                    max_input_bytes = greatest(s.max_input_bytes, EXCLUDED.max_input_bytes),
                    logs_dropped = s.logs_dropped + EXCLUDED.logs_dropped""", rows, rows.size(), (ps, r) -> {
            ps.setLong(1, r.scriptId());
            ps.setLong(2, r.versionId());
            ps.setTimestamp(3, Timestamp.from(r.minute()));
            ps.setLong(4, r.organizationId());
            ps.setInt(5, r.versionNo());
            ps.setInt(6, r.processed());
            ps.setInt(7, r.errors());
            ps.setInt(8, r.timeouts());
            ps.setDouble(9, r.avgMs());
            ps.setDouble(10, r.p95Ms());
            ps.setDouble(11, r.maxMs());
            ps.setInt(12, r.maxInputBytes());
            ps.setInt(13, r.logsDropped());
        });
    }

    /** 오류 스냅샷을 넣고 스크립트별 최근 {@code keep}건만 남긴다(SCR-05.01) */
    @Transactional
    public void insertErrors(List<ErrorRow> rows, int keep) {
        if (rows.isEmpty()) {
            return;
        }
        jdbc.batchUpdate("""
                INSERT INTO data2flow_pipeline.script_errors (organization_id, script_id, version_id, version_no, occurred_at,
                    error_code, message, line, col, stack, input_snapshot, device_id, raw_message_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?, ?)""", rows, rows.size(), (ps, r) -> {
            ps.setLong(1, r.organizationId());
            ps.setLong(2, r.scriptId());
            ps.setLong(3, r.versionId());
            ps.setInt(4, r.versionNo());
            ps.setTimestamp(5, Timestamp.from(r.occurredAt()));
            ps.setString(6, r.errorCode());
            ps.setString(7, r.message());
            ps.setObject(8, r.line(), Types.INTEGER);
            ps.setObject(9, r.col(), Types.INTEGER);
            ps.setString(10, r.stack());
            ps.setString(11, r.inputSnapshot());
            ps.setObject(12, r.deviceId(), Types.BIGINT);
            ps.setObject(13, r.rawMessageId(), Types.BIGINT);
        });
        Set<Long> scripts = new LinkedHashSet<>();
        rows.forEach(r -> scripts.add(r.scriptId()));
        for (Long scriptId : scripts) {
            jdbc.update("""
                    DELETE FROM data2flow_pipeline.script_errors WHERE script_id = ? AND id NOT IN (
                        SELECT id FROM data2flow_pipeline.script_errors WHERE script_id = ?
                         ORDER BY occurred_at DESC, id DESC LIMIT ?)""", scriptId, scriptId, keep);
        }
    }

    public void insertLogs(List<LogRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        jdbc.batchUpdate("""
                INSERT INTO data2flow_pipeline.script_logs (organization_id, script_id, version_no, at, device_id, message)
                VALUES (?, ?, ?, ?, ?, ?)""", rows, rows.size(), (ps, r) -> {
            ps.setLong(1, r.organizationId());
            ps.setLong(2, r.scriptId());
            ps.setInt(3, r.versionNo());
            ps.setTimestamp(4, Timestamp.from(r.at()));
            ps.setObject(5, r.deviceId(), Types.BIGINT);
            ps.setString(6, r.message());
        });
    }

    /** 지표 조회(API-SCR-12의 원천). step 1m이면 1분 표, 1h이면 1시간 표 + 아직 묶지 않은 1분 표 */
    public List<StatRow> findStats(long organizationId, long scriptId, Instant from, Instant to, boolean hourly) {
        String table = hourly ? "script_stats_1h" : "script_stats_1m";
        List<StatRow> rows = new java.util.ArrayList<>(jdbc.query("SELECT " + STAT_COLUMNS + " FROM data2flow_pipeline." + table
                        + " WHERE organization_id = ? AND script_id = ? AND minute >= ? AND minute < ? ORDER BY minute, version_id",
                (rs, n) -> map(rs), organizationId, scriptId, Timestamp.from(from), Timestamp.from(to)));
        if (hourly) {
            rows.addAll(jdbc.query("SELECT script_id, version_id, date_trunc('hour', minute) AS minute, max(organization_id) AS "
                            + "organization_id, max(version_no) AS version_no, sum(processed)::int AS processed, "
                            + "sum(errors)::int AS errors, sum(timeouts)::int AS timeouts, "
                            + "sum(avg_ms * processed) / nullif(sum(processed), 0) AS avg_ms, max(p95_ms) AS p95_ms, "
                            + "max(max_ms) AS max_ms, max(max_input_bytes) AS max_input_bytes, sum(logs_dropped)::int AS logs_dropped "
                            + "FROM data2flow_pipeline.script_stats_1m WHERE organization_id = ? AND script_id = ? AND minute >= ? "
                            + "AND minute < ? AND NOT EXISTS (SELECT 1 FROM data2flow_pipeline.script_stats_1h h "
                            + "WHERE h.script_id = script_stats_1m.script_id AND h.version_id = script_stats_1m.version_id "
                            + "AND h.minute = date_trunc('hour', script_stats_1m.minute)) "
                            + "GROUP BY script_id, version_id, date_trunc('hour', minute) ORDER BY 3",
                    (rs, n) -> map(rs), organizationId, scriptId, Timestamp.from(from), Timestamp.from(to)));
        }
        return rows;
    }

    /** 7일 지난 1분 지표를 1시간으로 묶고 지운다. 1시간 지표·오류·로그 보관 정리. 지운 행 수 */
    @Transactional
    @OrganizationScopeExempt("모든 조직의 보관 정리")
    public int rollupAndPurge(Instant minuteBefore, Instant hourBefore, Instant logsBefore) {
        jdbc.update("""
                INSERT INTO data2flow_pipeline.script_stats_1h AS h (script_id, version_id, minute, organization_id, version_no,
                    processed, errors, timeouts, avg_ms, p95_ms, max_ms, max_input_bytes, logs_dropped)
                SELECT script_id, version_id, date_trunc('hour', minute), max(organization_id), max(version_no),
                       sum(processed)::int, sum(errors)::int, sum(timeouts)::int,
                       coalesce(sum(avg_ms * processed) / nullif(sum(processed), 0), 0), max(p95_ms), max(max_ms),
                       max(max_input_bytes), sum(logs_dropped)::int
                  FROM data2flow_pipeline.script_stats_1m WHERE minute < ?
                 GROUP BY script_id, version_id, date_trunc('hour', minute)
                ON CONFLICT (script_id, version_id, minute) DO UPDATE SET
                    avg_ms = (h.avg_ms * h.processed + EXCLUDED.avg_ms * EXCLUDED.processed) / nullif(h.processed + EXCLUDED.processed, 0),
                    processed = h.processed + EXCLUDED.processed, errors = h.errors + EXCLUDED.errors,
                    timeouts = h.timeouts + EXCLUDED.timeouts, p95_ms = greatest(h.p95_ms, EXCLUDED.p95_ms),
                    max_ms = greatest(h.max_ms, EXCLUDED.max_ms), max_input_bytes = greatest(h.max_input_bytes, EXCLUDED.max_input_bytes),
                    logs_dropped = h.logs_dropped + EXCLUDED.logs_dropped""", Timestamp.from(minuteBefore));
        int purged = jdbc.update("DELETE FROM data2flow_pipeline.script_stats_1m WHERE minute < ?", Timestamp.from(minuteBefore));
        purged += jdbc.update("DELETE FROM data2flow_pipeline.script_stats_1h WHERE minute < ?", Timestamp.from(hourBefore));
        purged += jdbc.update("DELETE FROM data2flow_pipeline.script_logs WHERE at < ?", Timestamp.from(logsBefore));
        return purged;
    }

    private static StatRow map(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new StatRow(rs.getLong("script_id"), rs.getLong("version_id"), rs.getTimestamp("minute").toInstant(),
                rs.getLong("organization_id"), rs.getInt("version_no"), rs.getInt("processed"), rs.getInt("errors"),
                rs.getInt("timeouts"), rs.getDouble("avg_ms"), rs.getDouble("p95_ms"), rs.getDouble("max_ms"),
                rs.getInt("max_input_bytes"), rs.getInt("logs_dropped"));
    }

    public record StatRow(long scriptId, long versionId, Instant minute, long organizationId, int versionNo, int processed,
                          int errors, int timeouts, double avgMs, double p95Ms, double maxMs, int maxInputBytes,
                          int logsDropped) {
    }

    public record ErrorRow(long organizationId, long scriptId, long versionId, int versionNo, Instant occurredAt,
                           String errorCode, String message, Integer line, Integer col, String stack, String inputSnapshot,
                           Long deviceId, Long rawMessageId) {
    }

    public record LogRow(long organizationId, long scriptId, int versionNo, Instant at, Long deviceId, String message) {
    }
}
