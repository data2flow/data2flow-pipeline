package net.java21.data2flow.pipeline.aggregate.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

/**
 * 1m·1h·1d 집계 테이블(TSD-02.02, BR-TSD-04·05·06). 집계 작업은 모든 조직을 한 번에 처리하므로 조직 조건이 없다.
 * 정상 품질(0, 4)만 avg·min·max·sum·count·twa에 넣고, count_all과 first·last는 품질과 무관하다.
 */
@Repository
@OrganizationScopeExempt("모든 조직을 한 번에 처리하는 집계 작업(design/erd/pipeline.md §3.4)")
public class AggregateRepository {

    private static final String MINUTE_SQL = """
            WITH src AS (
                SELECT device_id, metric_key, organization_id, time, value, quality, is_virtual,
                       date_bin('1 minute', time, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS bucket
                  FROM data2flow_pipeline.telemetry
                 WHERE time >= :from AND time < :to %s
            ), ordered AS (
                SELECT src.*,
                       lead(time) OVER w AS next_time,
                       lag(value) OVER w AS prev_value
                  FROM src WINDOW w AS (PARTITION BY device_id, metric_key, bucket ORDER BY time)
            )
            INSERT INTO data2flow_pipeline.telemetry_1m AS t (device_id, metric_key, bucket, organization_id, count, count_all,
                avg, min, max, sum, first, last, first_time, last_time, twa, state_on_sec, state_changes, is_virtual)
            SELECT device_id, metric_key, bucket, max(organization_id),
                   count(*) FILTER (WHERE quality IN (0, 4)),
                   count(*),
                   avg(value) FILTER (WHERE quality IN (0, 4)),
                   min(value) FILTER (WHERE quality IN (0, 4)),
                   max(value) FILTER (WHERE quality IN (0, 4)),
                   sum(value) FILTER (WHERE quality IN (0, 4)),
                   (array_agg(value ORDER BY time))[1],
                   (array_agg(value ORDER BY time DESC))[1],
                   min(time), max(time),
                   CASE WHEN count(*) FILTER (WHERE quality IN (0, 4)) = 0 THEN NULL
                        WHEN max(time) > min(time)
                        THEN coalesce(sum(value * extract(epoch FROM (next_time - time)))
                                      FILTER (WHERE next_time IS NOT NULL AND quality IN (0, 4))
                                      / nullif(sum(extract(epoch FROM (next_time - time)))
                                      FILTER (WHERE next_time IS NOT NULL AND quality IN (0, 4)), 0),
                                      avg(value) FILTER (WHERE quality IN (0, 4)))
                        ELSE avg(value) FILTER (WHERE quality IN (0, 4)) END,
                   CASE WHEN metric_key = ANY(:stateKeys)
                        THEN coalesce(sum(extract(epoch FROM (coalesce(next_time, bucket + interval '1 minute') - time)))
                                      FILTER (WHERE value = 1), 0)::int END,
                   CASE WHEN metric_key = ANY(:stateKeys)
                        THEN (count(*) FILTER (WHERE prev_value IS NOT NULL AND prev_value <> value))::int END,
                   bool_or(is_virtual)
              FROM ordered
             GROUP BY device_id, metric_key, bucket
            ON CONFLICT (device_id, metric_key, bucket) DO UPDATE SET
                organization_id = EXCLUDED.organization_id, count = EXCLUDED.count, count_all = EXCLUDED.count_all,
                avg = EXCLUDED.avg, min = EXCLUDED.min, max = EXCLUDED.max, sum = EXCLUDED.sum, first = EXCLUDED.first,
                last = EXCLUDED.last, first_time = EXCLUDED.first_time, last_time = EXCLUDED.last_time, twa = EXCLUDED.twa,
                state_on_sec = EXCLUDED.state_on_sec, state_changes = EXCLUDED.state_changes, is_virtual = EXCLUDED.is_virtual
            """;

    private static final String ROLLUP_SQL = """
            INSERT INTO data2flow_pipeline.%s AS t (device_id, metric_key, bucket, organization_id, count, count_all, avg,
                min, max, sum, first, last, first_time, last_time, twa, state_on_sec, state_changes, is_virtual)
            SELECT device_id, metric_key, %s AS b, max(organization_id),
                   sum(count)::int, sum(count_all)::int,
                   CASE WHEN sum(count) > 0 THEN sum(sum) / sum(count) END,
                   min(min), max(max), sum(sum),
                   (array_agg(first ORDER BY bucket))[1],
                   (array_agg(last ORDER BY bucket DESC))[1],
                   min(first_time), max(last_time),
                   CASE WHEN sum(count) FILTER (WHERE twa IS NOT NULL) > 0
                        THEN sum(twa * count) FILTER (WHERE twa IS NOT NULL) / sum(count) FILTER (WHERE twa IS NOT NULL) END,
                   sum(state_on_sec)::int, sum(state_changes)::int,
                   bool_or(is_virtual)
              FROM data2flow_pipeline.%s
             WHERE bucket >= :from AND bucket < :to %s
             GROUP BY device_id, metric_key, b
            ON CONFLICT (device_id, metric_key, bucket) DO UPDATE SET
                organization_id = EXCLUDED.organization_id, count = EXCLUDED.count, count_all = EXCLUDED.count_all,
                avg = EXCLUDED.avg, min = EXCLUDED.min, max = EXCLUDED.max, sum = EXCLUDED.sum, first = EXCLUDED.first,
                last = EXCLUDED.last, first_time = EXCLUDED.first_time, last_time = EXCLUDED.last_time, twa = EXCLUDED.twa,
                state_on_sec = EXCLUDED.state_on_sec, state_changes = EXCLUDED.state_changes, is_virtual = EXCLUDED.is_virtual
            """;

    private final JdbcClient jdbc;

    public AggregateRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Instant> findWatermark(String level) {
        return jdbc.sql("SELECT processed_until FROM data2flow_pipeline.agg_watermarks WHERE level = :level")
                .param("level", level).query((rs, n) -> rs.getTimestamp(1).toInstant()).optional();
    }

    public void saveWatermark(String level, Instant until, Instant now) {
        jdbc.sql("""
                        INSERT INTO data2flow_pipeline.agg_watermarks (level, processed_until, updated_at) VALUES (:level, :until, :now)
                        ON CONFLICT (level) DO UPDATE SET processed_until = EXCLUDED.processed_until, updated_at = EXCLUDED.updated_at""")
                .param("level", level).param("until", Timestamp.from(until)).param("now", Timestamp.from(now)).update();
    }

    /** [from, to) 원본 → 1m(전체 또는 한 기기·측정 키) */
    public int aggregateMinutes(Instant from, Instant to, String[] stateKeys, Long deviceId, String metricKey) {
        boolean one = deviceId != null;
        if (one) {
            jdbc.sql("""
                            DELETE FROM data2flow_pipeline.telemetry_1m
                             WHERE device_id = :device AND metric_key = :key AND bucket >= :from AND bucket < :to""")
                    .param("device", deviceId).param("key", metricKey).param("from", Timestamp.from(from))
                    .param("to", Timestamp.from(to)).update();
        }
        var spec = jdbc.sql(MINUTE_SQL.formatted(one ? "AND device_id = :device AND metric_key = :key" : ""))
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to)).param("stateKeys", stateKeys);
        if (one) {
            spec = spec.param("device", deviceId).param("key", metricKey);
        }
        return spec.update();
    }

    /** 1m → 1h(UTC 시) */
    public int aggregateHours(Instant from, Instant to, Long deviceId, String metricKey) {
        return rollup("telemetry_1h", "telemetry_1m", "date_trunc('hour', bucket)", from, to, deviceId, metricKey);
    }

    /** 1h → 1d(사이트 시간대 자정, BR-TSD-05) */
    public int aggregateDays(Instant from, Instant to, ZoneId zone, Long deviceId, String metricKey) {
        String zoneLiteral = "'" + zone.getId().replace("'", "") + "'";
        return rollup("telemetry_1d", "telemetry_1h",
                "(date_trunc('day', bucket AT TIME ZONE " + zoneLiteral + ") AT TIME ZONE " + zoneLiteral + ")",
                from, to, deviceId, metricKey);
    }

    private int rollup(String target, String source, String bucketExpr, Instant from, Instant to, Long deviceId,
                       String metricKey) {
        boolean one = deviceId != null;
        if (one) {
            jdbc.sql("DELETE FROM data2flow_pipeline." + target
                            + " WHERE device_id = :device AND metric_key = :key AND bucket >= :from AND bucket < :to")
                    .param("device", deviceId).param("key", metricKey).param("from", Timestamp.from(from))
                    .param("to", Timestamp.from(to)).update();
        }
        var spec = jdbc.sql(ROLLUP_SQL.formatted(target, bucketExpr, source,
                        one ? "AND device_id = :device AND metric_key = :key" : ""))
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to));
        if (one) {
            spec = spec.param("device", deviceId).param("key", metricKey);
        }
        return spec.update();
    }

    /** 다시 계산할 구간(오래된 것부터 limit개) */
    public List<DirtyRange> findDirty(String level, int limit) {
        return jdbc.sql("""
                        SELECT id, organization_id, level, device_id, metric_key, from_ts, to_ts, reason
                          FROM data2flow_pipeline.agg_dirty_ranges WHERE level = :level ORDER BY created_at, id LIMIT :limit""")
                .param("level", level).param("limit", limit)
                .query((rs, n) -> new DirtyRange(rs.getLong("id"), rs.getLong("organization_id"), rs.getString("level"),
                        rs.getLong("device_id"), rs.getString("metric_key"), rs.getTimestamp("from_ts").toInstant(),
                        rs.getTimestamp("to_ts").toInstant(), rs.getString("reason")))
                .list();
    }

    public void deleteDirty(List<Long> ids) {
        if (!ids.isEmpty()) {
            jdbc.sql("DELETE FROM data2flow_pipeline.agg_dirty_ranges WHERE id IN (:ids)").param("ids", ids).update();
        }
    }

    public void insertDirty(long organizationId, String level, long deviceId, String metricKey, Instant from, Instant to,
                            String reason) {
        jdbc.sql("""
                        INSERT INTO data2flow_pipeline.agg_dirty_ranges (organization_id, level, device_id, metric_key, from_ts,
                            to_ts, reason) VALUES (:org, :level, :device, :key, :from, :to, :reason)""")
                .param("org", organizationId).param("level", level).param("device", deviceId).param("key", metricKey)
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to)).param("reason", reason).update();
    }

    /** 원본에서 가장 이른 시각(첫 집계 시작점). 없으면 빈 값 */
    public Optional<Instant> findEarliestTelemetry(Instant notBefore) {
        Timestamp min = jdbc.sql("SELECT min(time) FROM data2flow_pipeline.telemetry WHERE time >= :from")
                .param("from", Timestamp.from(notBefore)).query((rs, n) -> rs.getTimestamp(1)).list().getFirst();
        return Optional.ofNullable(min).map(Timestamp::toInstant);
    }

    public record DirtyRange(long id, long organizationId, String level, long deviceId, String metricKey, Instant from,
                             Instant to, String reason) {
    }
}
