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
