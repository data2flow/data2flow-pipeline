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

    /**
     * 원본 → 1m. 상태형 측정 항목(door 등)의 켜짐 시간·변화 수는 구간 경계를 넘는 값을 이어 보려고 1시간 앞의 점까지 읽는다
     * (구간 첫 점 앞의 [구간 시작, 첫 점) 부분은 직전 값으로 센다). 점이 없는 분은 행이 없다.
     */
    private static final String MINUTE_SQL = """
            WITH src AS (
                SELECT device_id, metric_key, organization_id, time, value, quality, is_virtual,
                       date_bin('1 minute', time, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS bucket
                  FROM data2flow_pipeline.telemetry
                 WHERE time >= :lookback AND time < :to %s
            ), ordered AS (
                SELECT src.*,
                       lead(time) OVER wa AS next_all,
                       lag(value) OVER wa AS prev_all,
                       lead(time) OVER wb AS next_time,
                       row_number() OVER wb AS rn
                  FROM src
                WINDOW wa AS (PARTITION BY device_id, metric_key ORDER BY time),
                       wb AS (PARTITION BY device_id, metric_key, bucket ORDER BY time)
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
                        THEN round(coalesce(sum(extract(epoch FROM (least(coalesce(next_all, bucket + interval '1 minute'),
                                      bucket + interval '1 minute') - time))) FILTER (WHERE value = 1), 0)
                             + coalesce(max(extract(epoch FROM (time - bucket))) FILTER (WHERE rn = 1 AND prev_all = 1), 0))::int
                   END,
                   CASE WHEN metric_key = ANY(:stateKeys)
                        THEN (count(*) FILTER (WHERE prev_all IS NOT NULL AND prev_all <> value))::int END,
                   bool_or(is_virtual)
              FROM ordered
             WHERE bucket >= :from
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

    /**
     * 1h → 1d, 기기마다 사이트 시간대 자정(BR-TSD-05). 시간대는 {@code device_state.timezone}(core 기기 정보), 없으면 조직 기본.
     * [from, to)와 겹치는 날 구간만 다시 계산하고, 그 날의 1h 행을 모두 읽도록 앞뒤 2일을 더 읽는다(DST로 하루가 25시간이어도 안전).
     */
    private static final String DAY_SQL = """
            WITH src AS (
                SELECT h.*, (date_trunc('day', h.bucket AT TIME ZONE z.tz) AT TIME ZONE z.tz) AS b
                  FROM data2flow_pipeline.telemetry_1h h
                  CROSS JOIN LATERAL (SELECT coalesce((SELECT ds.timezone FROM data2flow_pipeline.device_state ds
                                                        WHERE ds.device_id = h.device_id), :zone) AS tz) z
                 WHERE h.bucket >= CAST(:from AS timestamptz) - interval '2 days'
                   AND h.bucket < CAST(:to AS timestamptz) + interval '2 days'
            )
            INSERT INTO data2flow_pipeline.telemetry_1d AS t (device_id, metric_key, bucket, organization_id, count, count_all, avg,
                min, max, sum, first, last, first_time, last_time, twa, state_on_sec, state_changes, is_virtual)
            SELECT device_id, metric_key, b, max(organization_id),
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
              FROM src
             WHERE b + interval '1 day' > CAST(:from AS timestamptz) AND b < CAST(:to AS timestamptz)
             GROUP BY device_id, metric_key, b
            ON CONFLICT (device_id, metric_key, bucket) DO UPDATE SET
                organization_id = EXCLUDED.organization_id, count = EXCLUDED.count, count_all = EXCLUDED.count_all,
                avg = EXCLUDED.avg, min = EXCLUDED.min, max = EXCLUDED.max, sum = EXCLUDED.sum, first = EXCLUDED.first,
                last = EXCLUDED.last, first_time = EXCLUDED.first_time, last_time = EXCLUDED.last_time, twa = EXCLUDED.twa,
                state_on_sec = EXCLUDED.state_on_sec, state_changes = EXCLUDED.state_changes, is_virtual = EXCLUDED.is_virtual
            """;

    /**
     * 상태형 측정 항목의 시간별 켜짐 시간·변화 수를 원본 점에서 바로 계산한다(TSD-05.03 "열려 있던 총 시간"이 정확하도록). 값이 바뀔
     * 때만 저장하면(ON_CHANGE, 1시간마다 하트비트) 점이 없는 분에는 1m 행이 없어 1m 합으로는 시간이 모자란다. 점마다 다음 점(없으면
     * 구간 끝)까지를 그 값의 구간으로 보고 시간 버킷과 겹치는 만큼 센다. 앞 시간의 값을 잇기 위해 2시간 앞부터 읽는다.
     */
    private static final String HOUR_STATE_SQL = """
            WITH pts AS (
                SELECT device_id, metric_key, organization_id, is_virtual, time, value,
                       lag(value) OVER w AS prev, lead(time) OVER w AS nxt
                  FROM data2flow_pipeline.telemetry
                 WHERE metric_key = ANY(:keys) AND time >= CAST(:from AS timestamptz) - interval '2 hours'
                   AND time < CAST(:to AS timestamptz) %s
                WINDOW w AS (PARTITION BY device_id, metric_key ORDER BY time)
            ), spans AS (
                SELECT pts.*, coalesce(nxt, CAST(:to AS timestamptz)) AS until FROM pts
            ), hours AS (
                SELECT s.device_id, s.metric_key, max(s.organization_id) AS organization_id, bool_or(s.is_virtual) AS is_virtual,
                       h.b AS bucket,
                       sum(CASE WHEN s.value = 1
                                THEN extract(epoch FROM least(s.until, h.b + interval '1 hour') - greatest(s.time, h.b))
                                ELSE 0 END) AS on_sec,
                       count(*) FILTER (WHERE s.time >= h.b AND s.time < h.b + interval '1 hour' AND s.prev IS NOT NULL
                                          AND s.prev <> s.value) AS changes
                  FROM spans s
                  CROSS JOIN LATERAL generate_series(date_trunc('hour', s.time),
                                                     date_trunc('hour', s.until - interval '1 microsecond'),
                                                     interval '1 hour') AS h(b)
                 WHERE s.until > s.time AND h.b >= CAST(:from AS timestamptz) AND h.b < CAST(:to AS timestamptz)
                 GROUP BY s.device_id, s.metric_key, h.b
            )
            INSERT INTO data2flow_pipeline.telemetry_1h AS t (device_id, metric_key, bucket, organization_id, count, count_all,
                state_on_sec, state_changes, is_virtual)
            SELECT device_id, metric_key, bucket, organization_id, 0, 0, round(on_sec)::int, changes::int, is_virtual FROM hours
            ON CONFLICT (device_id, metric_key, bucket) DO UPDATE SET
                state_on_sec = EXCLUDED.state_on_sec, state_changes = EXCLUDED.state_changes
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
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to))
                .param("lookback", Timestamp.from(from.minus(java.time.Duration.ofHours(1)))).param("stateKeys", stateKeys);
        if (one) {
            spec = spec.param("device", deviceId).param("key", metricKey);
        }
        return spec.update();
    }

    /** 1m → 1h(UTC 시) */
    public int aggregateHours(Instant from, Instant to, Long deviceId, String metricKey) {
        return rollup("telemetry_1h", "telemetry_1m", "date_trunc('hour', bucket)", from, to, deviceId, metricKey);
    }

    /** 상태형 측정 항목의 1h 켜짐 시간·변화 수를 원본 점으로 다시 센다(전체 또는 한 기기·키) */
    public int aggregateHourStates(Instant from, Instant to, String[] stateKeys, Long deviceId, String metricKey) {
        if (stateKeys == null || stateKeys.length == 0) {
            return 0;
        }
        boolean one = deviceId != null;
        var spec = jdbc.sql(HOUR_STATE_SQL.formatted(one ? "AND device_id = :device AND metric_key = :key" : ""))
                .param("keys", stateKeys).param("from", Timestamp.from(from)).param("to", Timestamp.from(to));
        if (one) {
            spec = spec.param("device", deviceId).param("key", metricKey);
        }
        return spec.update();
    }

    /** 1h → 1d, 모든 기기(기기별 사이트 시간대 자정, BR-TSD-05). {@code defaultZone}은 시간대를 모르는 기기용 */
    public int aggregateDaysAllDevices(Instant from, Instant to, ZoneId defaultZone) {
        return jdbc.sql(DAY_SQL).param("from", Timestamp.from(from)).param("to", Timestamp.from(to))
                .param("zone", defaultZone.getId()).update();
    }

    /** 기기의 사이트 시간대(device_state.timezone). 없으면 빈 값 */
    public Optional<ZoneId> findZone(long deviceId) {
        return jdbc.sql("SELECT timezone FROM data2flow_pipeline.device_state WHERE device_id = :device AND timezone IS NOT NULL")
                .param("device", deviceId).query(String.class).optional().flatMap(AggregateRepository::zoneOf);
    }

    static Optional<ZoneId> zoneOf(String id) {
        try {
            return Optional.of(ZoneId.of(id));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /** 1h → 1d(사이트 시간대 자정, BR-TSD-05). 한 기기·측정 키(다시 계산) */
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
