package net.java21.data2flow.pipeline.telemetry.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;

/**
 * 시계열 원본 쓰기(TSD-01.01·01.02). 같은 (기기, 측정 키, 시각)은 한 번만 저장한다({@code ON CONFLICT DO NOTHING}, BR-ING-07 ②).
 * 재처리는 같은 키를 덮어쓴다(BR-ING-12).
 */
@Repository
public class TelemetryRepository {

    private static final String INSERT = """
            INSERT INTO data2flow_pipeline.telemetry (device_id, metric_key, time, organization_id, value, quality, flags,
                is_virtual, received_at, raw_message_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (device_id, metric_key, time) DO NOTHING""";
    private static final String UPSERT = """
            INSERT INTO data2flow_pipeline.telemetry (device_id, metric_key, time, organization_id, value, quality, flags,
                is_virtual, received_at, raw_message_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (device_id, metric_key, time) DO UPDATE SET value = EXCLUDED.value, quality = EXCLUDED.quality,
                flags = EXCLUDED.flags, received_at = EXCLUDED.received_at, raw_message_id = EXCLUDED.raw_message_id""";

    private final JdbcTemplate jdbc;

    public TelemetryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return 실제로 새로 들어간 행 수 */
    public int insertAll(List<TelemetryRow> rows, boolean overwrite) {
        if (rows.isEmpty()) {
            return 0;
        }
        int[] counts = jdbc.batchUpdate(overwrite ? UPSERT : INSERT, rows, rows.size(), (ps, r) -> {
            ps.setLong(1, r.deviceId());
            ps.setString(2, r.metricKey());
            ps.setTimestamp(3, Timestamp.from(r.time()));
            ps.setLong(4, r.organizationId());
            ps.setDouble(5, r.value());
            ps.setShort(6, (short) r.quality());
            ps.setShort(7, (short) r.flags());
            ps.setBoolean(8, r.virtual());
            ps.setTimestamp(9, Timestamp.from(r.receivedAt()));
            if (r.rawMessageId() == null) {
                ps.setNull(10, Types.BIGINT);
            } else {
                ps.setLong(10, r.rawMessageId());
            }
        })[0];
        int inserted = 0;
        for (int c : counts) {
            inserted += Math.max(c, 0);
        }
        return inserted;
    }

    public void insertLinks(List<LinkQualityRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        jdbc.batchUpdate("""
                INSERT INTO data2flow_pipeline.link_qualities (device_id, gateway_eui, time, organization_id, rssi, snr, f_cnt)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (device_id, gateway_eui, time) DO NOTHING""", rows, rows.size(), (ps, r) -> {
            ps.setLong(1, r.deviceId());
            ps.setString(2, r.gatewayEui());
            ps.setTimestamp(3, Timestamp.from(r.time()));
            ps.setLong(4, r.organizationId());
            ps.setObject(5, r.rssi(), Types.NUMERIC);
            ps.setObject(6, r.snr(), Types.NUMERIC);
            ps.setObject(7, r.frameCounter(), Types.BIGINT);
        });
    }

    /**
     * 별칭 키 행을 표준 키로 옮긴다(API-TSD-51). 같은 키·시각이 이미 있으면 기존 값을 두고, 바뀐 (기기, 키, 분)을 REMAP 재계산 구간으로 남긴다.
     *
     * @return 옮긴 행 수
     */
    @org.springframework.transaction.annotation.Transactional
    public int remapMetric(long organizationId, String alias, String targetKey, Instant from, Instant to) {
        Timestamp f = Timestamp.from(from == null ? Instant.EPOCH : from);
        Timestamp t = Timestamp.from(to == null ? Instant.parse("9999-01-01T00:00:00Z") : to);
        jdbc.update("""
                INSERT INTO data2flow_pipeline.agg_dirty_ranges (organization_id, level, device_id, metric_key, from_ts, to_ts, reason)
                SELECT DISTINCT organization_id, '1m', device_id, k, date_trunc('minute', time),
                       date_trunc('minute', time) + interval '1 minute', 'REMAP'
                  FROM data2flow_pipeline.telemetry, unnest(ARRAY[?, ?]) AS k
                 WHERE organization_id = ? AND metric_key = ? AND time >= ? AND time < ?""",
                alias, targetKey, organizationId, alias, f, t);
        int moved = jdbc.update("""
                INSERT INTO data2flow_pipeline.telemetry (device_id, metric_key, time, organization_id, value, quality, flags,
                    is_virtual, received_at, raw_message_id)
                SELECT device_id, ?, time, organization_id, value, quality, flags | 4, is_virtual, received_at, raw_message_id
                  FROM data2flow_pipeline.telemetry
                 WHERE organization_id = ? AND metric_key = ? AND time >= ? AND time < ?
                ON CONFLICT (device_id, metric_key, time) DO NOTHING""", targetKey, organizationId, alias, f, t);
        jdbc.update("DELETE FROM data2flow_pipeline.telemetry WHERE organization_id = ? AND metric_key = ? AND time >= ? AND time < ?",
                organizationId, alias, f, t);
        return moved;
    }

    /** 기기의 최근 값(창 채우기, BR-SCR-19): {@code since} 이후, 늦은 값 제외, 측정 키별 시각 순 */
    @net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt("처리 중인 메시지의 기기(이미 조직을 확인함) 창 채우기")
    public java.util.Map<String, java.util.List<double[]>> loadRecent(long deviceId, Instant since) {
        java.util.Map<String, java.util.List<double[]>> out = new java.util.LinkedHashMap<>();
        jdbc.query("""
                SELECT metric_key, time, value FROM data2flow_pipeline.telemetry
                 WHERE device_id = ? AND time >= ? AND flags & 1 = 0 ORDER BY metric_key, time""", rs -> {
            out.computeIfAbsent(rs.getString(1), k -> new java.util.ArrayList<>())
                    .add(new double[]{rs.getTimestamp(2).toInstant().toEpochMilli(), rs.getDouble(3)});
        }, deviceId, Timestamp.from(since));
        return out;
    }

    /** 기기·측정 키들의 원본 점(수식 미리 보기, 시각 순) */
    public java.util.List<Point> findPoints(long organizationId, long deviceId, java.util.List<String> keys, Instant from,
                                            Instant to) {
        if (keys.isEmpty()) {
            return java.util.List.of();
        }
        return jdbc.query("""
                SELECT metric_key, time, value FROM data2flow_pipeline.telemetry
                 WHERE organization_id = ? AND device_id = ? AND metric_key = ANY(?) AND time >= ? AND time < ?
                 ORDER BY time, metric_key LIMIT 20000""",
                (rs, n) -> new Point(rs.getString(1), rs.getTimestamp(2).toInstant(), rs.getDouble(3)),
                organizationId, deviceId, keys.toArray(String[]::new), Timestamp.from(from), Timestamp.from(to));
    }

    /** 원본 점 */
    public record Point(String metricKey, Instant time, double value) {
    }

    /** 집계 워터마크(이 시각까지 정상 갱신 완료). 없으면 null */
    public Instant currentWatermark(String level) {
        return jdbc.query("SELECT processed_until FROM data2flow_pipeline.agg_watermarks WHERE level = ?",
                rs -> rs.next() ? rs.getTimestamp(1).toInstant() : null, level);
    }

    /** 이미 집계된 구간에 늦게 들어온 값: 다시 계산할 구간을 남긴다(BR-TSD-06) */
    public void insertDirtyRange(long organizationId, String level, long deviceId, String metricKey, Instant from,
                                 Instant to, String reason) {
        jdbc.update("""
                INSERT INTO data2flow_pipeline.agg_dirty_ranges (organization_id, level, device_id, metric_key, from_ts,
                    to_ts, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?)""", organizationId, level, deviceId, metricKey, Timestamp.from(from),
                Timestamp.from(to), reason);
    }
}
