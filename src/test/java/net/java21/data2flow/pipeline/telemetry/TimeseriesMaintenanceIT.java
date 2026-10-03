package net.java21.data2flow.pipeline.telemetry;

import net.java21.data2flow.pipeline.aggregate.service.AggregationService;
import net.java21.data2flow.pipeline.connectivity.service.OfflineDetector;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.partition.service.PartitionMaintenanceService;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 시계열 유지(TSD-01.01·02.02, ADR-019): 월·일 파티션, 보관 정리, 1m·1h·1d 집계와 다시 계산, 오프라인 판정(DEV-02.05) */
class TimeseriesMaintenanceIT extends IntegrationTestSupport {

    @Autowired
    private PartitionMaintenanceService partitions;
    @Autowired
    private AggregationService aggregation;
    @Autowired
    private OfflineDetector offline;
    @Autowired
    private DeviceDirectory devices;

    private void insert(long device, String key, Instant time, double value, int quality) {
        jdbc.sql("""
                        INSERT INTO data2flow_pipeline.telemetry (device_id, metric_key, time, organization_id, value, quality,
                            received_at) VALUES (:d, :k, :t, 1, :v, :q, :t)""")
                .param("d", device).param("k", key).param("t", Timestamp.from(time)).param("v", value).param("q", quality)
                .update();
    }

    private List<String> partitionsOf(String parent) {
        return jdbc.sql("""
                        SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent
                         WHERE p.relname = :p ORDER BY 1""").param("p", parent).query(String.class).list();
    }

    @Test
    @DisplayName("[TSD-01.01][ADR-019] 파티션 스케줄러가 월 파티션을 3개월 앞까지·원본 일 파티션을 7일 앞까지 만들고 등록한다(pg_partman 없음)")
    void createsPartitionsAhead() {
        clock.set(Instant.parse("2026-12-20T00:00:00Z"));

        PartitionMaintenanceService.Result result = partitions.maintain();

        assertThat(result.failed()).isEmpty();
        assertThat(partitionsOf("telemetry")).contains("telemetry_default", "telemetry_y2026m12", "telemetry_y2027m03");
        assertThat(partitionsOf("telemetry_1m")).contains("telemetry_1m_y2027m03");
        assertThat(partitionsOf("link_qualities")).contains("link_qualities_y2027m03");
        assertThat(partitionsOf("raw_messages")).contains("raw_messages_y2026m12d27");
        assertThat(partitionsOf("telemetry_1h")).contains("telemetry_1h_y2026", "telemetry_1h_y2027");
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.partition_registries WHERE partition_name = 'telemetry_y2027m03'"))
                .isEqualTo(1);
        assertThat(partitions.maintain().created()).as("다시 실행해도 같은 결과(멱등)").isEmpty();
    }

    @Test
    @DisplayName("[TSD-01.01][BR-TSD-01] 범위 밖 시각은 DEFAULT로 들어가고 경고(partition.warning), 파티션을 만들면 그 범위 행을 옮긴다")
    void defaultPartitionRows() {
        insert(1, "temperature", Instant.parse("2027-08-15T00:00:00Z"), 20, 0);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_default")).isEqualTo(1);

        partitions.maintain();
        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(events("partition.warning"))
                .anySatisfy(e -> assertThat(e.get("payload").get("reason").asString()).isEqualTo("default_partition_rows")));

        clock.set(Instant.parse("2027-06-01T00:00:00Z"));
        partitions.maintain();

        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_default")).isZero();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_y2027m08")).isEqualTo(1);
    }

    @Test
    @DisplayName("[ING-01.01][AT-ING-01.4] TC-ING-004 원본 보관 30일: 31일이 지난 일 파티션은 DETACH 후 DROP, 등록 상태 DROPPED")
    void rawRetention() {
        partitions.maintain();
        assertThat(partitionsOf("raw_messages")).contains("raw_messages_y2026m10d02");

        clock.set(MutableClockT0.plus(Duration.ofDays(33)));
        PartitionMaintenanceService.Result result = partitions.maintain();

        assertThat(result.dropped()).contains("raw_messages_y2026m10d02");
        assertThat(partitionsOf("raw_messages")).doesNotContain("raw_messages_y2026m10d02").contains("raw_messages_y2026m11d05");
        assertThat(jdbc.sql("SELECT state FROM data2flow_pipeline.partition_registries WHERE partition_name = 'raw_messages_y2026m10d02'")
                .query(String.class).single()).isEqualTo("DROPPED");
    }

    @Test
    @DisplayName("[TSD-02.02][AT-TSD-07.2] TC-TSD-037 1분 간격 하루 1,440점: 1m·1h·1d의 avg·min·max·sum·count·last가 SQL 기준값과 같고, 품질 1 값은 count_all에만")
    void aggregationCorrectness() {
        Instant day = Instant.parse("2026-10-02T15:00:00Z"); // Asia/Seoul 10-03 00:00
        for (int i = 0; i < 1440; i++) {
            insert(7, "temperature", day.plus(Duration.ofMinutes(i)), 20 + (i % 60) / 10.0, i == 100 ? 1 : 0);
            insert(7, "door", day.plus(Duration.ofMinutes(i)).plusSeconds(30), (i / 30) % 2, 0);
        }
        clock.set(day.plus(Duration.ofDays(1)).plus(Duration.ofMinutes(2)));
        for (String level : List.of("1m", "1h", "1d")) {
            jdbc.sql("INSERT INTO data2flow_pipeline.agg_watermarks (level, processed_until) VALUES (:l, :t)")
                    .param("l", level).param("t", Timestamp.from(day)).update();
        }

        aggregation.aggregateMinutes();
        aggregation.aggregateHours();
        aggregation.aggregateDays();

        Map<String, Object> expected = jdbc.sql("""
                        SELECT count(*) FILTER (WHERE quality IN (0,4)) AS count, count(*) AS count_all,
                               avg(value) FILTER (WHERE quality IN (0,4)) AS avg, min(value) FILTER (WHERE quality IN (0,4)) AS min,
                               max(value) FILTER (WHERE quality IN (0,4)) AS max, sum(value) FILTER (WHERE quality IN (0,4)) AS sum
                          FROM data2flow_pipeline.telemetry WHERE device_id = 7 AND metric_key = 'temperature'""")
                .query().singleRow();
        Map<String, Object> daily = jdbc.sql("""
                        SELECT count, count_all, avg, min, max, sum, last FROM data2flow_pipeline.telemetry_1d
                         WHERE device_id = 7 AND metric_key = 'temperature'""").query().singleRow();
        assertThat(((Number) daily.get("count")).longValue()).isEqualTo(((Number) expected.get("count")).longValue())
                .isEqualTo(1439);
        assertThat(((Number) daily.get("count_all")).longValue()).isEqualTo(1440);
        assertThat((Double) daily.get("avg")).isCloseTo(((Number) expected.get("avg")).doubleValue(),
                org.assertj.core.data.Offset.offset(1e-9));
        assertThat((Double) daily.get("sum")).isCloseTo(((Number) expected.get("sum")).doubleValue(),
                org.assertj.core.data.Offset.offset(1e-6));
        assertThat((Double) daily.get("min")).isEqualTo(((Number) expected.get("min")).doubleValue());
        assertThat((Double) daily.get("max")).isEqualTo(((Number) expected.get("max")).doubleValue());
        assertThat((Double) daily.get("last")).isEqualTo(20 + (1439 % 60) / 10.0);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_1m WHERE device_id = 7 AND metric_key = 'temperature'"))
                .isEqualTo(1440);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.telemetry_1h WHERE device_id = 7 AND metric_key = 'temperature'"))
                .isEqualTo(24);
        assertThat(jdbc.sql("SELECT bucket FROM data2flow_pipeline.telemetry_1d WHERE device_id = 7 AND metric_key = 'temperature'")
                .query(Timestamp.class).single().toInstant()).as("Asia/Seoul 자정(BR-TSD-05)").isEqualTo(day);
        Map<String, Object> door = jdbc.sql("""
                        SELECT state_on_sec, state_changes FROM data2flow_pipeline.telemetry_1h
                         WHERE device_id = 7 AND metric_key = 'door' AND bucket = :b""")
                .param("b", Timestamp.from(day)).query().singleRow();
        assertThat(((Number) door.get("state_changes")).intValue()).isEqualTo(1);
        // 30분 30초에 1이 되어 시 끝(60분)까지: 29분 30초 = 1,770초
        assertThat(((Number) door.get("state_on_sec")).intValue()).isEqualTo(1770);
    }

    @Test
    @DisplayName("[TSD-02.02][AT-TSD-07.1] TC-TSD-038·TC-ING-075 이미 집계한 구간에 늦게 온 값 → 다시 계산할 구간 기록 → 다음 주기에 1m·1h·1d 재계산, aggregates.recomputed")
    void lateDataRecompute() {
        Instant hour = Instant.parse("2026-10-02T22:00:00Z");
        for (int i = 0; i < 60; i++) {
            insert(8, "co2", hour.plus(Duration.ofMinutes(i)), 600, 0);
        }
        clock.set(Instant.parse("2026-10-03T00:00:00Z"));
        aggregation.aggregateMinutes();
        aggregation.aggregateHours();
        aggregation.aggregateDays();
        double before = jdbc.sql("SELECT avg FROM data2flow_pipeline.telemetry_1h WHERE device_id = 8 AND bucket = :b")
                .param("b", Timestamp.from(hour)).query(Double.class).single();
        CORE.device(8, 1, 3, "24e1240000000888", "ACTIVE", null, null, 60);

        publish(envelope(3, "MQTT_SUBSCRIBE", "application/a/device/24e1240000000888/event/up", """
                {"deduplicationId":"aaaaaaaa-0000-0000-0000-000000000001","deviceInfo":{"devEui":"24e1240000000888"},"fCnt":1,
                 "object":{"co2":1200},"rxInfo":[{"gatewayId":"g","rssi":-50,"snr":5,"nsTime":"2026-10-02T22:30:30Z"}]}
                """.getBytes(), clock.instant()));
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 1);
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.agg_dirty_ranges WHERE level = '1m'")).isEqualTo(1);

        aggregation.aggregateMinutes();
        aggregation.aggregateHours();
        aggregation.aggregateDays();

        double after = jdbc.sql("SELECT avg FROM data2flow_pipeline.telemetry_1h WHERE device_id = 8 AND bucket = :b")
                .param("b", Timestamp.from(hour)).query(Double.class).single();
        assertThat(before).isEqualTo(600);
        assertThat(after).isCloseTo((600 * 60 + 1200) / 61.0, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.agg_dirty_ranges")).isZero();
        assertThat(jdbc.sql("SELECT count FROM data2flow_pipeline.telemetry_1d WHERE device_id = 8").query(Integer.class).single())
                .isEqualTo(61);
        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(events("aggregates.recomputed"))
                .extracting(e -> e.get("payload").get("level").asString()).contains("1m", "1h", "1d"));
    }

    @Test
    @DisplayName("[DEV-02.05] TC-DEV-059 주기 60초 × 배수 3: 3분 넘게 조용한 기기는 OFFLINE + device.connectivity.changed, 다시 수신하면 ONLINE")
    void offlineDetection() {
        CORE.device(9, 1, 3, "24e1240000000999", "ACTIVE", null, null, 60);
        String uplink = """
                {"deduplicationId":"%s","deviceInfo":{"devEui":"24e1240000000999"},"fCnt":%d,"object":{"temperature":20},
                 "rxInfo":[{"gatewayId":"g","rssi":-50,"snr":5,"nsTime":"%s"}]}""";
        publish(envelope(3, "MQTT_SUBSCRIBE", "application/a/device/24e1240000000999/event/up",
                uplink.formatted("bbbbbbbb-0000-0000-0000-000000000001", 1, "2026-10-03T00:00:00Z").getBytes(), clock.instant()));
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 1);
        devices.warm(clock.instant());

        clock.advance(Duration.ofMinutes(2));
        assertThat(offline.detect()).isEmpty();
        clock.advance(Duration.ofMinutes(2));
        assertThat(offline.detect()).containsExactly(9L);
        assertThat(offline.detect()).as("한 번만").isEmpty();

        assertThat(jdbc.sql("SELECT connectivity FROM data2flow_pipeline.device_state WHERE device_id = 9").query(String.class)
                .single()).isEqualTo("OFFLINE");
        publish(envelope(3, "MQTT_SUBSCRIBE", "application/a/device/24e1240000000999/event/up",
                uplink.formatted("bbbbbbbb-0000-0000-0000-000000000002", 2, "2026-10-03T00:04:00Z").getBytes(), clock.instant()));
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 2);
        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(events("device.connectivity.changed"))
                .extracting(e -> e.get("payload").get("to").asString()).containsExactly("ONLINE", "OFFLINE", "ONLINE"));
        assertThat(events("device.connectivity.changed").get(1).get("payload").get("expectedIntervalSec").asInt()).isEqualTo(60);
    }

    private static final Instant MutableClockT0 = net.java21.data2flow.pipeline.support.MutableClock.T0;
}
