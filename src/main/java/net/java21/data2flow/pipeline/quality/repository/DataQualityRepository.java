package net.java21.data2flow.pipeline.quality.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import net.java21.data2flow.pipeline.quality.domain.DataQualityScore;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/** {@code data_quality_daily}(ING-06.01). 확정한 날은 다시 쓰지 않는다(늦은 데이터는 확정 뒤 반영 안 함, TC-ING-070) */
@Repository
public class DataQualityRepository {

    private final JdbcClient jdbc;

    public DataQualityRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 점수를 낼 기기(한 번이라도 수신한 기기)와 사이트 시간대 */
    @OrganizationScopeExempt("모든 조직의 기기를 도는 야간 점수 작업")
    public List<DeviceDay> listDevices() {
        return jdbc.sql("SELECT device_id, organization_id, timezone FROM data2flow_pipeline.device_state ORDER BY device_id")
                .query((rs, n) -> new DeviceDay(rs.getLong(1), rs.getLong(2), rs.getString(3))).list();
    }

    public boolean exists(long organizationId, long deviceId, LocalDate day) {
        return jdbc.sql("""
                        SELECT EXISTS (SELECT 1 FROM data2flow_pipeline.data_quality_daily
                                        WHERE organization_id = :org AND device_id = :device AND day = :day)""")
                .param("org", organizationId).param("device", deviceId).param("day", day).query(Boolean.class).single();
    }

    /** [from, to) 기기의 수신·값 집계: 측정 시각 수, 늦은 도착 시각 수, 값 수, 범위 초과, 의심 */
    public Counts countDay(long organizationId, long deviceId, Instant from, Instant to) {
        return jdbc.sql("""
                        SELECT count(DISTINCT time)::int AS received,
                               count(DISTINCT time) FILTER (WHERE flags & 1 = 1)::int AS late,
                               count(*)::int AS vals,
                               count(*) FILTER (WHERE quality = 1)::int AS oor,
                               count(*) FILTER (WHERE quality = 3)::int AS suspect
                          FROM data2flow_pipeline.telemetry
                         WHERE organization_id = :org AND device_id = :device AND time >= :from AND time < :to
                           AND flags & 2 = 0""")
                .param("org", organizationId).param("device", deviceId).param("from", Timestamp.from(from))
                .param("to", Timestamp.from(to))
                .query((rs, n) -> new Counts(rs.getInt("received"), rs.getInt("late"), rs.getInt("vals"), rs.getInt("oor"),
                        rs.getInt("suspect"))).single();
    }

    /** 확정: 이미 있으면 그대로 둔다(멱등). 새로 썼으면 true */
    public boolean insert(long organizationId, long deviceId, LocalDate day, DataQualityScore s, Instant now) {
        return jdbc.sql("""
                        INSERT INTO data2flow_pipeline.data_quality_daily (device_id, day, organization_id, score, completeness,
                            timeliness, validity, stability, expected_count, received_count, late_count, out_of_range_count,
                            suspect_count, created_at)
                        VALUES (:device, :day, :org, :score, :c, :t, :v, :s, :expected, :received, :late, :oor, :suspect, :now)
                        ON CONFLICT (device_id, day) DO NOTHING""")
                .param("device", deviceId).param("day", day).param("org", organizationId).param("score", s.score())
                .param("c", s.completeness()).param("t", s.timeliness()).param("v", s.validity()).param("s", s.stability())
                .param("expected", s.expectedCount()).param("received", s.receivedCount()).param("late", s.lateCount())
                .param("oor", s.outOfRangeCount()).param("suspect", s.suspectCount()).param("now", Timestamp.from(now))
                .update() == 1;
    }

    /** 보관(BR-ING-14: 품질 점수 1년) */
    @OrganizationScopeExempt("모든 조직의 보관 정리")
    public int deleteBefore(LocalDate day) {
        return jdbc.sql("DELETE FROM data2flow_pipeline.data_quality_daily WHERE day < :day").param("day", day).update();
    }

    public record DeviceDay(long deviceId, long organizationId, String timezone) {
    }

    public record Counts(int received, int late, int values, int outOfRange, int suspect) {
    }
}
