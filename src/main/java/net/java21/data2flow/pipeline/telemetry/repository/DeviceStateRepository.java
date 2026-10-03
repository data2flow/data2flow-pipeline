package net.java21.data2flow.pipeline.telemetry.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** {@code device_state}(기기당 1행, ING-05.02·DEV-02.05). core-api는 읽기만 한다 */
@Repository
public class DeviceStateRepository {

    private final JdbcClient jdbc;

    public DeviceStateRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 처리 트랜잭션 안에서 행을 잠그고 읽는다(동시 갱신 경합에도 최신 값 유지, TC-ING-066) */
    public Optional<DeviceState> lockState(long organizationId, long deviceId) {
        return jdbc.sql("""
                        SELECT device_id, organization_id, last_seen_at, last_measured_at, connectivity
                          FROM data2flow_pipeline.device_state
                         WHERE organization_id = :org AND device_id = :device FOR UPDATE""")
                .param("org", organizationId).param("device", deviceId)
                .query(DeviceStateRepository::map).optional();
    }

    /**
     * 수신 반영. {@code latest}·측정 시각·배터리·신호는 더 최신 측정일 때만 바꾼다(BR-ING-08). 수신하면 ONLINE.
     *
     * @param latestJson {@code {metricKey: {v, t, q, unit}}}
     */
    public void upsertReceived(long organizationId, long deviceId, Instant receivedAt, Instant measuredAt, String latestJson,
                               Double battery, Double rssi, Double snr, String bestGateway, boolean connectivityChanged) {
        jdbc.sql("""
                        INSERT INTO data2flow_pipeline.device_state AS s (device_id, organization_id, last_seen_at,
                            last_measured_at, connectivity, connectivity_changed_at, latest, battery, rssi, snr,
                            best_gateway_eui, updated_at)
                        VALUES (:device, :org, :seen, :measured, 'ONLINE', :seen, CAST(:latest AS jsonb), :battery, :rssi,
                            :snr, :gateway, :seen)
                        ON CONFLICT (device_id) DO UPDATE SET
                            last_seen_at = GREATEST(s.last_seen_at, EXCLUDED.last_seen_at),
                            connectivity = 'ONLINE',
                            connectivity_changed_at = CASE WHEN :changed THEN EXCLUDED.last_seen_at
                                                           ELSE s.connectivity_changed_at END,
                            latest = CASE WHEN s.last_measured_at IS NULL OR EXCLUDED.last_measured_at >= s.last_measured_at
                                          THEN s.latest || EXCLUDED.latest ELSE s.latest END,
                            last_measured_at = GREATEST(s.last_measured_at, EXCLUDED.last_measured_at),
                            battery = CASE WHEN EXCLUDED.battery IS NOT NULL AND (s.last_measured_at IS NULL
                                           OR EXCLUDED.last_measured_at >= s.last_measured_at) THEN EXCLUDED.battery
                                           ELSE s.battery END,
                            rssi = CASE WHEN EXCLUDED.rssi IS NOT NULL AND (s.last_measured_at IS NULL
                                        OR EXCLUDED.last_measured_at >= s.last_measured_at) THEN EXCLUDED.rssi ELSE s.rssi END,
                            snr = CASE WHEN EXCLUDED.snr IS NOT NULL AND (s.last_measured_at IS NULL
                                       OR EXCLUDED.last_measured_at >= s.last_measured_at) THEN EXCLUDED.snr ELSE s.snr END,
                            best_gateway_eui = CASE WHEN EXCLUDED.best_gateway_eui IS NOT NULL AND (s.last_measured_at IS NULL
                                       OR EXCLUDED.last_measured_at >= s.last_measured_at) THEN EXCLUDED.best_gateway_eui
                                       ELSE s.best_gateway_eui END,
                            updated_at = EXCLUDED.updated_at""")
                .param("device", deviceId).param("org", organizationId).param("seen", Timestamp.from(receivedAt))
                .param("measured", Timestamp.from(measuredAt)).param("latest", latestJson).param("battery", battery)
                .param("rssi", rssi).param("snr", snr).param("gateway", bestGateway).param("changed", connectivityChanged)
                .update();
    }

    /** TRANSFORM 직전 값(ctx.last)용 최근값 JSON {@code {key: {v, t, q, unit}}}. 없으면 빈 값 */
    public Optional<String> findLatest(long organizationId, long deviceId) {
        return jdbc.sql("SELECT latest::text FROM data2flow_pipeline.device_state WHERE organization_id = :org AND device_id = :device")
                .param("org", organizationId).param("device", deviceId).query(String.class).optional();
    }

    /** 오프라인 판정 후보: 아직 OFFLINE이 아니고 마지막 수신이 기준보다 오래된 기기(DEV-02.05) */
    @OrganizationScopeExempt("모든 조직을 도는 오프라인 판정 작업")
    public List<DeviceState> findSeenBefore(Instant before) {
        return jdbc.sql("""
                        SELECT device_id, organization_id, last_seen_at, last_measured_at, connectivity
                          FROM data2flow_pipeline.device_state
                         WHERE connectivity <> 'OFFLINE' AND last_seen_at IS NOT NULL AND last_seen_at < :before""")
                .param("before", Timestamp.from(before))
                .query(DeviceStateRepository::map).list();
    }

    /** 오프라인으로 바꾼다. 그 사이 다시 수신했으면(last_seen_at이 바뀜) 바꾸지 않는다 */
    public boolean updateOffline(long organizationId, long deviceId, Instant lastSeenAt, Instant now) {
        return jdbc.sql("""
                        UPDATE data2flow_pipeline.device_state SET connectivity = 'OFFLINE', connectivity_changed_at = :now,
                               updated_at = :now
                         WHERE organization_id = :org AND device_id = :device AND connectivity <> 'OFFLINE'
                           AND last_seen_at = :seen""")
                .param("now", Timestamp.from(now)).param("org", organizationId).param("device", deviceId)
                .param("seen", Timestamp.from(lastSeenAt)).update() == 1;
    }

    /** 화면 표시용 최근 24시간 수신 수(1시간마다) */
    @OrganizationScopeExempt("모든 조직을 도는 정리 작업")
    public int updateMessageCounts(Instant since) {
        return jdbc.sql("""
                        UPDATE data2flow_pipeline.device_state s SET msg_count_24h = c.n
                          FROM (SELECT device_id, count(*)::int AS n FROM data2flow_pipeline.raw_messages
                                 WHERE received_at >= :since AND device_id IS NOT NULL AND status = 'OK'
                                 GROUP BY device_id) c
                         WHERE s.device_id = c.device_id AND s.msg_count_24h <> c.n""")
                .param("since", Timestamp.from(since)).update();
    }

    static DeviceState map(ResultSet rs, int n) throws SQLException {
        Timestamp seen = rs.getTimestamp("last_seen_at");
        Timestamp measured = rs.getTimestamp("last_measured_at");
        return new DeviceState(rs.getLong("device_id"), rs.getLong("organization_id"),
                seen == null ? null : seen.toInstant(), measured == null ? null : measured.toInstant(),
                rs.getString("connectivity"));
    }

    public record DeviceState(long deviceId, long organizationId, Instant lastSeenAt, Instant lastMeasuredAt,
                              String connectivity) {
    }
}
