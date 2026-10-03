package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.IngestAlert;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import net.java21.data2flow.pipeline.common.Backoff;
import net.java21.data2flow.pipeline.common.PayloadEncoding;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.common.TransientFailures;
import net.java21.data2flow.pipeline.device.service.CoreUnavailableException;
import net.java21.data2flow.pipeline.device.service.FakeCoreDirectory;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import net.java21.data2flow.pipeline.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** 처리 단계 부품 단위 테스트: 중복 키·파티션 캐시(TC-ING-058), 처리 지연 경보(TC-ING-092), 일시 장애 분류, 백오프, 게이트웨이 보고 */
class IngestComponentsTest {

    private static final Instant T = Instant.parse("2026-07-01T00:00:00Z");

    @Test
    @DisplayName("[ING-04.04][AT-ING-01.2] TC-ING-058 ChirpStack deduplicationId 우선, 없으면 sha256(source, topic, payload)")
    void dedupKeys() {
        byte[] chirp = "{\"deduplicationId\":\"3F1E9B2A-0C4D-4E5F-8A6B-7C8D9E0F1A2B\",\"x\":1}".getBytes(StandardCharsets.UTF_8);
        assertThat(DedupKeys.detect(3, "t", chirp)).isEqualTo("chirpstack:3f1e9b2a-0c4d-4e5f-8a6b-7c8d9e0f1a2b");
        assertThat(DedupKeys.detect(3, "t", "22.8".getBytes(StandardCharsets.UTF_8))).startsWith("sha256:");
        assertThat(DedupKeys.contentInBucket(3, "t", new byte[]{1}, T, Duration.ofSeconds(30)))
                .isEqualTo(DedupKeys.contentInBucket(3, "t", new byte[]{1}, T.plusSeconds(10), Duration.ofSeconds(30)))
                .isNotEqualTo(DedupKeys.contentInBucket(3, "t", new byte[]{1}, T.plusSeconds(60), Duration.ofSeconds(30)));
    }

    @Test
    @DisplayName("[ING-04.04][AT-ING-01.2] TC-ING-058 10분 창: 9분 59초 뒤 같은 키는 중복, 10분 1초 뒤는 새 메시지, 파티션별·조직별")
    void dedupWindow() {
        DedupGuard guard = new DedupGuard(Duration.ofMinutes(10));
        guard.record(1, 1, "k", T);

        assertThat(guard.seen(1, 1, "k", T.plusSeconds(599))).isTrue();
        assertThat(guard.seen(1, 1, "k", T.plusSeconds(601))).isFalse();
        assertThat(guard.seen(2, 1, "k", T)).isFalse();
        assertThat(guard.seen(1, 2, "k", T)).isFalse();
        guard.clear(1);
        assertThat(guard.seen(1, 1, "k", T)).isFalse();
    }

    @Test
    @DisplayName("[ING-07.04][AT-ING-05.2] TC-ING-092 lag 59초 없음, 90초 WARNING 1건, 5분 1초 CRITICAL 승격, 회복 시 해제")
    void lagMonitor() {
        MutableClock clock = MutableClock.atUtc("2026-07-01T00:00:00Z");
        List<LagMonitorPartition> state = new ArrayList<>();
        DomainEventPublisher events = mock(DomainEventPublisher.class);
        LagMonitor monitor = new LagMonitor(() -> state.stream()
                .map(s -> new LagMonitor.PartitionLag(s.partition, s.backlog, s.receivedAt, 1)).toList(),
                events, new PipelineProperties.Lag(Duration.ofSeconds(60), Duration.ofSeconds(300)), clock);

        state.add(new LagMonitorPartition(0, true, clock.instant().minusSeconds(59)));
        assertThat(monitor.evaluate()).isNull();
        state.set(0, new LagMonitorPartition(0, true, clock.instant().minusSeconds(90)));
        assertThat(monitor.evaluate()).isEqualTo(IngestAlert.Level.WARNING);
        assertThat(monitor.evaluate()).isEqualTo(IngestAlert.Level.WARNING);
        state.set(0, new LagMonitorPartition(0, true, clock.instant().minusSeconds(301)));
        assertThat(monitor.evaluate()).isEqualTo(IngestAlert.Level.CRITICAL);
        assertThat(monitor.lagSeconds()).isEqualTo(301);
        state.set(0, new LagMonitorPartition(0, false, clock.instant().minusSeconds(900)));
        assertThat(monitor.evaluate()).isNull();

        verify(events, times(2)).publish(eq(EventType.INGEST_ALERT_RAISED), eq(1L), any(IngestAlert.class));
        verify(events, times(1)).publish(eq(EventType.INGEST_ALERT_CLEARED), eq(1L), any(IngestAlert.class));
    }

    record LagMonitorPartition(int partition, boolean backlog, Instant receivedAt) {
    }

    @Test
    @DisplayName("[NFR-02.02] DB 연결·core 장애·발행 확인 실패는 일시 장애(다시 시도), 제약 위반은 아님")
    void transientClassification() {
        assertThat(TransientFailures.isTransient(new CannotGetJdbcConnectionException("x"))).isTrue();
        assertThat(TransientFailures.isTransient(new CoreUnavailableException("x", null))).isTrue();
        assertThat(TransientFailures.isTransient(new RuntimeException(new TransientFailures.PublishFailedException("x", null))))
                .isTrue();
        assertThat(TransientFailures.isTransient(new RuntimeException(new java.sql.SQLException("x", "08006")))).isTrue();
        assertThat(TransientFailures.isTransient(new DataIntegrityViolationException("x"))).isFalse();
        assertThat(TransientFailures.isTransient(new IllegalStateException("x"))).isFalse();
    }

    @Test
    @DisplayName("[NFR-02.02] 백오프는 두 배씩 늘어 최대값에서 멈추고, 종료 신호면 바로 그친다")
    void backoff() {
        Backoff backoff = new Backoff(Duration.ofMillis(1), Duration.ofMillis(4));
        assertThat(backoff.pause(() -> false)).isTrue();
        assertThat(backoff.current()).isEqualTo(Duration.ofMillis(2));
        backoff.pause(() -> false);
        backoff.pause(() -> false);
        assertThat(backoff.current()).isEqualTo(Duration.ofMillis(4));
        assertThat(backoff.pause(() -> true)).isFalse();
        backoff.reset();
        assertThat(backoff.current()).isEqualTo(Duration.ofMillis(1));
    }

    @Test
    @DisplayName("[DEV-05.01] 게이트웨이 수신을 모아 보내고, core 장애면 남겨 두었다가 다음에 보낸다")
    void gatewayToucher() {
        FakeCoreDirectory core = new FakeCoreDirectory();
        GatewayToucher toucher = new GatewayToucher(core);
        toucher.record(1, 3, "gw1", T);
        toucher.record(1, 3, "gw1", T.plusSeconds(5));
        toucher.record(1, 3, "gw2", T);
        core.down = true;
        toucher.flush();
        assertThat(toucher.pendingCount()).isEqualTo(2);
        core.down = false;
        toucher.flush();
        assertThat(toucher.pendingCount()).isZero();
        toucher.flush();
    }

    @Test
    @DisplayName("[ING-01.01] payload 보기 형식: JSON·TEXT·BINARY")
    void payloadEncoding() {
        assertThat(PayloadEncoding.detect("{\"a\":1}".getBytes(StandardCharsets.UTF_8))).isEqualTo(PayloadEncoding.JSON);
        assertThat(PayloadEncoding.detect("22.8".getBytes(StandardCharsets.UTF_8))).isEqualTo(PayloadEncoding.TEXT);
        assertThat(PayloadEncoding.detect(new byte[]{0x01, 0x75, (byte) 0xFF, 0x00})).isEqualTo(PayloadEncoding.BINARY);
        assertThat(PayloadEncoding.detect(new byte[]{0x01, 0x02})).isEqualTo(PayloadEncoding.BINARY);
    }

    @Test
    @DisplayName("[ING-07.04] 이벤트 발행기는 RabbitTemplate 확인을 기다린다(구성 확인)")
    void publisherConstructs() {
        assertThat(new DomainEventPublisher(mock(RabbitTemplate.class), MutableClock.atUtc("2026-07-01T00:00:00Z")))
                .isNotNull();
    }
}
