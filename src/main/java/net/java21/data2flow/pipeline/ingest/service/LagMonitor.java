package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.IngestAlert;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 처리 지연(lag) 경보(ING-07.04, BR-ING-16, TC-ING-092): 처리 대기 메시지가 있는 파티션의 지연(지금 − 마지막으로 처리한 메시지의
 * 수신 시각)이 1분을 넘으면 WARNING, 5분을 넘으면 CRITICAL {@code ingest.alert.raised}(EVT-ING-05 {@code INGEST_LAG}),
 * 기준 아래로 돌아오면 {@code ingest.alert.cleared}. 5초마다 평가하고 지표 {@code data2flow_ingest_lag_seconds}로도 내보낸다.
 */
public class LagMonitor {

    private static final Logger log = LoggerFactory.getLogger(LagMonitor.class);

    private final Supplier<List<PartitionLag>> lags;
    private final DomainEventPublisher events;
    private final PipelineProperties.Lag settings;
    private final Clock clock;
    private final AtomicLong lagSeconds = new AtomicLong();
    private volatile IngestAlert.Level raised;

    public LagMonitor(Supplier<List<PartitionLag>> lags, DomainEventPublisher events, PipelineProperties.Lag settings,
                      Clock clock) {
        this.lags = lags;
        this.events = events;
        this.settings = settings;
        this.clock = clock;
    }

    /** 평가 한 번. 현재 단계(없으면 null) */
    public IngestAlert.Level evaluate() {
        Instant now = clock.instant();
        long organizationId = 1;
        Duration worst = Duration.ZERO;
        for (PartitionLag p : lags.get()) {
            if (p.backlog() && p.lastReceivedAt() != null) {
                Duration lag = Duration.between(p.lastReceivedAt(), now);
                if (lag.compareTo(worst) > 0) {
                    worst = lag;
                    organizationId = p.organizationId();
                }
            }
        }
        lagSeconds.set(worst.toSeconds());
        IngestAlert.Level level = worst.compareTo(settings.critical()) > 0 ? IngestAlert.Level.CRITICAL
                : worst.compareTo(settings.warn()) > 0 ? IngestAlert.Level.WARNING : null;
        if (level != raised) {
            try {
                if (level == null) {
                    events.publish(EventType.INGEST_ALERT_CLEARED, organizationId, IngestAlert.of(IngestAlert.INGEST_LAG,
                            raised, null, worst.toSeconds(), settings.warn().toSeconds(), List.of(), now));
                } else {
                    events.publish(EventType.INGEST_ALERT_RAISED, organizationId, IngestAlert.of(IngestAlert.INGEST_LAG,
                            level, null, worst.toSeconds(),
                            (level == IngestAlert.Level.CRITICAL ? settings.critical() : settings.warn()).toSeconds(),
                            List.of("DB_LATENCY", "SCRIPT_ERROR_RATE", "INPUT_SURGE"), now));
                }
                raised = level;
            } catch (RuntimeException e) {
                log.warn("처리 지연 경보 발행 실패(다음 평가에 다시): {}", e.getMessage());
            }
        }
        return level;
    }

    public long lagSeconds() {
        return lagSeconds.get();
    }

    /**
     * @param backlog        처리하지 않은 메시지가 있음
     * @param lastReceivedAt 마지막으로 처리한 메시지의 수신 시각
     */
    public record PartitionLag(int partition, boolean backlog, Instant lastReceivedAt, long organizationId) {
    }
}
