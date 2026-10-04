package net.java21.data2flow.pipeline.quality;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.pipeline.aggregate.service.AggregationService;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.quality.service.DataQualityService;
import net.java21.data2flow.pipeline.support.IntegrationTestSupport;
import net.java21.data2flow.pipeline.support.TestStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 데이터 품질(ING-04.01 의심, ING-06.01 일일 점수, ING-06.04 시계 오차), 하트비트 카나리 통과 기록(ING-07.05),
 * 1d 집계 사이트 시간대(BR-TSD-05)를 실제 수집 경로로 확인한다.
 */
class DataQualityIT extends IntegrationTestSupport {

    private static final long SOURCE = 41;

    @Autowired
    private DataQualityService quality;
    @Autowired
    private DeviceDirectory devices;
    @Autowired
    private AggregationService aggregation;

    @BeforeEach
    void source() {
        CORE.source(SOURCE, 1, "generic-json", "AUTO_REGISTER", null);
        CORE.sourceDecoderConfig(SOURCE, CODEC.mapper().readTree("""
                {"deviceIdFrom":"topic[1]","timeFrom":"$.ts","metrics":[{"path":"$.t","key":"temperature"},
                 {"path":"$.door","key":"door"}]}"""));
    }

    private RawEnvelope message(String device, Instant measured, Instant received, double t) {
        return envelope(SOURCE, "MQTT_SUBSCRIBE", "devices/" + device + "/up",
                ("{\"ts\":\"" + measured + "\",\"t\":" + t + "}").getBytes(StandardCharsets.UTF_8), received);
    }

    @Test
    @DisplayName("[ING-06.04][AT-ING-09.3] TC-ING-077 측정 시각이 수신보다 7분 앞서는 상태가 30분 이어지면 ingest.clock-skew.suspected(평균 +420초), 기기 상태에 평균 오차 저장")
    void clockSkewEvent() {
        CORE.device(411, 1, SOURCE, "skew-01", "ACTIVE", null, null, 60);
        Instant t0 = clock.instant().minus(Duration.ofHours(1));
        for (int i = 0; i <= 31; i++) {
            Instant received = t0.plus(Duration.ofMinutes(i));
            publish(message("skew-01", received.plus(Duration.ofMinutes(7)), received, 20 + (i % 3)));
        }
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 32);

        await().atMost(TestStreams.timeout()).untilAsserted(() -> assertThat(events("ingest.clock-skew.suspected"))
                .singleElement().satisfies(e -> {
                    assertThat(e.get("payload").get("deviceId").asLong()).isEqualTo(411);
                    assertThat(e.get("payload").get("avgSkewSec").asDouble()).isEqualTo(420.0);
                    assertThat(Instant.parse(e.get("payload").get("since").asString())).isEqualTo(t0);
                }));
        Map<String, Object> state = jdbc.sql("""
                        SELECT clock_skew_avg_sec, clock_skew_suspected FROM data2flow_pipeline.device_state WHERE device_id = 411""")
                .query().singleRow();
        assertThat(state).containsEntry("clock_skew_avg_sec", 420).containsEntry("clock_skew_suspected", true);
        assertThat(telemetry.telemetry().getLast().metric("temperature").quality()).as("미래 시각은 보정(quality 4)").isEqualTo(4);
    }

    @Test
    @DisplayName("[ING-04.01][AT-ING-04.1] TC-ING-054 같은 값 12회째부터 quality 3(값 멈춤), 실시간 경로")
    void stuckValueIsSuspect() {
        CORE.device(412, 1, SOURCE, "stuck-01", "ACTIVE", null, null, 60);
        Instant t0 = clock.instant().minus(Duration.ofMinutes(20));
        for (int i = 0; i < 13; i++) {
            Instant at = t0.plus(Duration.ofMinutes(i));
            publish(message("stuck-01", at, at, 22.5));
        }
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 13);

        List<Integer> qualities = telemetry.telemetry().stream().map(t -> t.metric("temperature").quality()).toList();
        assertThat(qualities.subList(0, 11)).containsOnly(0);
        assertThat(qualities.subList(11, 13)).containsOnly(3);
    }

    @Test
    @DisplayName("[ING-07.05] 하트비트 카나리 기기(__heartbeat__)의 표준 메시지에 앞 단계 기록을 잇고 pipeline 통과 시각을 덧붙인다")
    void heartbeatStages() {
        CORE.device(413, 1, SOURCE, "__heartbeat__", "ACTIVE", null, null, 10);
        Instant at = clock.instant();
        publish(envelope(SOURCE, "MQTT_SUBSCRIBE", "devices/__heartbeat__/up", ("""
                {"ts":"%s","t":1,"meta":{"heartbeat":{"sentAt":"%s","stages":[{"name":"simulator","at":"%s"}]}}}"""
                .formatted(at, at, at)).getBytes(StandardCharsets.UTF_8), at));
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 1);

        CanonicalTelemetry t = telemetry.telemetry().getFirst();
        JsonNode hb = t.meta().extra().get("heartbeat");
        assertThat(hb.get("sentAt").asString()).isEqualTo(at.toString());
        assertThat(hb.get("stages")).extracting(s -> s.get("name").asString()).containsExactly("simulator", "ingress", "pipeline");
    }

    @Test
    @DisplayName("[ING-06.01][AT-ING-09.1] TC-ING-070 Asia/Seoul 00:30(UTC 15:30)에 전날 점수 확정, 다시 실행해도 같은 행, 확정 뒤 늦은 데이터는 반영 안 함")
    void dailyScore() {
        CORE.device(414, 1, SOURCE, "q-01", "ACTIVE", null, null, 60);
        devices.warm(clock.instant());
        jdbc.sql("INSERT INTO data2flow_pipeline.device_state (device_id, organization_id, timezone) VALUES (414, 1, 'Asia/Seoul')")
                .update();
        Instant dayStart = Instant.parse("2026-10-01T15:00:00Z"); // 10-02 00:00 KST
        for (int i = 0; i < 720; i++) {
            Instant t = dayStart.plus(Duration.ofMinutes(i * 2L));
            int quality = i < 72 ? 1 : i < 108 ? 3 : 0;
            int flags = i % 10 == 0 ? 1 : 0;
            jdbc.sql("""
                            INSERT INTO data2flow_pipeline.telemetry (device_id, metric_key, time, organization_id, value, quality,
                                flags, received_at) VALUES (414, 'temperature', :t, 1, 20, :q, :f, :t)""")
                    .param("t", Timestamp.from(t)).param("q", quality).param("f", flags).update();
        }

        clock.set(Instant.parse("2026-10-02T15:29:00Z"));
        quality.confirmDue();
        assertThat(count("SELECT count(*) FROM data2flow_pipeline.data_quality_daily WHERE day = '2026-10-02'"))
                .as("00:29 KST에는 아직").isZero();

        clock.set(Instant.parse("2026-10-02T15:30:00Z"));
        assertThat(quality.confirmDue()).isEqualTo(1);
        Map<String, Object> row = jdbc.sql("SELECT * FROM data2flow_pipeline.data_quality_daily WHERE device_id = 414 AND day = :d")
                .param("d", LocalDate.parse("2026-10-02")).query().singleRow();
        assertThat(row).containsEntry("completeness", 50).containsEntry("expected_count", 1440)
                .containsEntry("received_count", 720).containsEntry("late_count", 72).containsEntry("out_of_range_count", 72)
                .containsEntry("suspect_count", 36).containsEntry("timeliness", 90).containsEntry("validity", 90)
                .containsEntry("stability", 95);
        assertThat(((Number) row.get("score")).intValue()).isEqualTo((int) Math.round(50 * 0.4 + 90 * 0.2 + 90 * 0.2 + 95 * 0.2));

        jdbc.sql("""
                INSERT INTO data2flow_pipeline.telemetry (device_id, metric_key, time, organization_id, value, quality, flags,
                    received_at) VALUES (414, 'temperature', :t, 1, 20, 0, 1, :r)""")
                .param("t", Timestamp.from(dayStart.plusSeconds(61))).param("r", Timestamp.from(clock.instant())).update();
        clock.advance(Duration.ofMinutes(30));
        quality.confirmDue();
        assertThat(jdbc.sql("SELECT received_count FROM data2flow_pipeline.data_quality_daily WHERE device_id = 414 AND day = :d")
                .param("d", LocalDate.parse("2026-10-02")).query(Integer.class).single()).isEqualTo(720);
    }

    @Test
    @DisplayName("[TSD-02.02][BR-TSD-05] 1d 집계는 기기별 사이트 시간대 자정(기기 정보 timezone → device_state), 없으면 Asia/Seoul")
    void dailyBucketBySiteZone() {
        CORE.device(415, 1, SOURCE, "ny-01", "ACTIVE", null, null, 60).put("timezone", "America/New_York");
        CORE.device(416, 1, SOURCE, "seoul-01", "ACTIVE", null, null, 60);
        Instant at = Instant.parse("2026-10-02T12:00:00Z");
        publish(message("ny-01", at, at, 21));
        publish(message("seoul-01", at, at, 22));
        await().atMost(TestStreams.timeout()).until(() -> telemetry.telemetry().size() == 2);
        assertThat(jdbc.sql("SELECT timezone FROM data2flow_pipeline.device_state WHERE device_id = 415").query(String.class).single())
                .isEqualTo("America/New_York");

        clock.set(Instant.parse("2026-10-03T00:00:00Z"));
        for (String level : List.of("1m", "1h", "1d")) {
            jdbc.sql("INSERT INTO data2flow_pipeline.agg_watermarks (level, processed_until) VALUES (:l, :t)")
                    .param("l", level).param("t", Timestamp.from(Instant.parse("2026-10-02T00:00:00Z"))).update();
        }
        aggregation.aggregateMinutes();
        aggregation.aggregateHours();
        aggregation.aggregateDays();

        assertThat(jdbc.sql("SELECT bucket FROM data2flow_pipeline.telemetry_1d WHERE device_id = 415").query(Timestamp.class)
                .single().toInstant()).as("뉴욕 자정(EDT, UTC-4)").isEqualTo(Instant.parse("2026-10-02T04:00:00Z"));
        assertThat(jdbc.sql("SELECT bucket FROM data2flow_pipeline.telemetry_1d WHERE device_id = 416").query(Timestamp.class)
                .single().toInstant()).as("서울 자정").isEqualTo(Instant.parse("2026-10-01T15:00:00Z"));
    }
}
