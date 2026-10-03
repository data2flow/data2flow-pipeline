package net.java21.data2flow.pipeline.telemetry.service;

import net.java21.data2flow.pipeline.telemetry.repository.TelemetryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 별칭 재매핑(DEV-04.03, API-TSD-51): 지난 시계열의 별칭 키({@code illuminance})를 표준 키({@code illumination})로 바꾼다.
 * 같은 (기기, 표준 키, 시각)이 이미 있으면 그 값을 두고 별칭 행만 지운다. 바뀐 구간은 집계 재계산 구간(REMAP)으로 남긴다(BR-TSD-06).
 * 백그라운드로 돌고 작업 ID를 바로 돌려준다(202). 완료 이벤트 EVT-DEV-11은 계약(EventType)에 아직 없어 내지 않는다.
 */
public class MetricRemapService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MetricRemapService.class);

    private final TelemetryRepository telemetry;
    private final Clock clock;
    private final Map<String, String> jobs = new ConcurrentHashMap<>();
    private final ExecutorService runner = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("metric-remap").factory());

    public MetricRemapService(TelemetryRepository telemetry, Clock clock) {
        this.telemetry = telemetry;
        this.clock = clock;
    }

    public String start(long organizationId, String alias, String targetKey, Instant from, Instant to) {
        String jobId = UUID.randomUUID().toString();
        jobs.put(jobId, "RUNNING");
        runner.execute(() -> {
            try {
                int moved = telemetry.remapMetric(organizationId, alias, targetKey, from, to);
                jobs.put(jobId, "COMPLETED:" + moved);
                log.info("재매핑 {} → {} 완료(org={}, {}행)", alias, targetKey, organizationId, moved);
            } catch (RuntimeException e) {
                jobs.put(jobId, "FAILED");
                log.error("재매핑 {} → {} 실패: {}", alias, targetKey, e.getMessage(), e);
            }
        });
        return jobId;
    }

    /** 작업 상태(RUNNING, COMPLETED:행 수, FAILED). 모르면 null */
    public String status(String jobId) {
        return jobs.get(jobId);
    }

    @Override
    public void close() {
        runner.shutdownNow();
    }

    Clock clock() {
        return clock;
    }
}
