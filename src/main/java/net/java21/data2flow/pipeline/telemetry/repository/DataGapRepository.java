package net.java21.data2flow.pipeline.telemetry.repository;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;

/** {@code data_gaps}(ING-06.05, BR-ING-17) */
@Repository
public class DataGapRepository {

    private final JdbcClient jdbc;

    public DataGapRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** @return 새로 기록했으면 true(같은 시작 시각이면 이미 있음) */
    public boolean insert(long organizationId, long deviceId, Instant gapStart, Instant gapEnd, int expectedCount,
                          Instant now) {
        return jdbc.sql("""
                        INSERT INTO data2flow_pipeline.data_gaps (organization_id, device_id, gap_start, gap_end,
                            expected_count, detected_at)
                        VALUES (:org, :device, :start, :end, :count, :now)
                        ON CONFLICT (device_id, gap_start) DO NOTHING""")
                .param("org", organizationId).param("device", deviceId).param("start", Timestamp.from(gapStart))
                .param("end", Timestamp.from(gapEnd)).param("count", expectedCount).param("now", Timestamp.from(now))
                .update() == 1;
    }
}
