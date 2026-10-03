package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.pipeline.common.PipelineErrorCode;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.service.SourceContextCache;
import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;
import net.java21.data2flow.pipeline.ingest.dto.ReprocessJobDtos;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRepository;
import net.java21.data2flow.pipeline.ingest.repository.ReprocessJobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/**
 * 기간 재처리 작업(ING-01.04, API-ING-23). 보관된 원본을 현재 디코더·스크립트로 다시 처리하고 같은 행·같은 telemetry 키를 덮어쓴다
 * (BR-ING-12). 실시간 소비와 별도 스레드에서 초당 500건 한도로 돈다(BR-ING-13). 한 소스에 동시 작업 하나(409), 기간 31일 이하(400),
 * 원본 보관 30일 안(400). 취소하면 다음 묶음 전에 멈춘다(처리한 구간까지만 반영).
 *
 * <p>한계(M5에서 보강): 작업은 요청을 받은 인스턴스가 처리하므로 그 인스턴스가 죽으면 RUNNING으로 남는다. 시작 시점 스크립트 버전
 * 고정은 기록만 하고(현재 번들로 처리), 버전별 번들 재현은 아직 없다.
 */
public class ReprocessJobService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReprocessJobService.class);
    private static final int BATCH = 200;
    private static final int RATE_PER_SECOND = 500;

    private final ReprocessJobRepository jobs;
    private final RawMessageRepository raws;
    private final IngestProcessor processor;
    private final SourceContextCache sources;
    private final PipelineProperties properties;
    private final Clock clock;
    private final ExecutorService runner = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("reprocess-job").factory());

    public ReprocessJobService(ReprocessJobRepository jobs, RawMessageRepository raws, IngestProcessor processor,
                               SourceContextCache sources, PipelineProperties properties, Clock clock) {
        this.jobs = jobs;
        this.raws = raws;
        this.processor = processor;
        this.sources = sources;
        this.properties = properties;
        this.clock = clock;
    }

    public ReprocessJobDtos.CreateResponse create(ReprocessJobDtos.CreateRequest request) {
        Instant now = clock.instant();
        if (!request.to().isAfter(request.from()) || Duration.between(request.from(), request.to()).compareTo(Duration.ofDays(31)) > 0) {
            throw new BusinessException(PipelineErrorCode.ING_QUERY_RANGE_TOO_LARGE);
        }
        if (request.from().isBefore(now.minus(properties.partition().rawRetention()))) {
            throw new BusinessException(PipelineErrorCode.ING_REPROCESS_OUT_OF_RETENTION);
        }
        long org = request.organizationId();
        Long[] devices = request.deviceIds() == null || request.deviceIds().isEmpty() ? null
                : request.deviceIds().toArray(Long[]::new);
        long total = jobs.countTargets(org, request.sourceId(), devices, request.from(), request.to(), request.onlyFailed());
        String decoder = sources.get(request.sourceId()).map(s -> s.decoderKey()).orElse("unknown");
        long jobId;
        try {
            jobId = jobs.insert(org, request.sourceId(), devices, request.from(), request.to(), total, decoder, "{}",
                    request.requestedBy(), request.memo(), now);
        } catch (DuplicateKeyException e) {
            throw new BusinessException(PipelineErrorCode.ING_REPROCESS_ALREADY_RUNNING);
        }
        runner.execute(() -> run(jobId, request, devices, total));
        return new ReprocessJobDtos.CreateResponse(jobId, "QUEUED", total);
    }

    public ReprocessJobDtos.CancelResponse cancel(long jobId, ReprocessJobDtos.CancelRequest request) {
        if (!jobs.updateCancelled(request.organizationId(), jobId, clock.instant())) {
            if (jobs.findStatus(request.organizationId(), jobId).isEmpty()) {
                throw new BusinessException(net.java21.data2flow.contracts.error.CommonErrorCode.RESOURCE_NOT_FOUND);
            }
            throw new BusinessException(PipelineErrorCode.ING_REPROCESS_NOT_CANCELLABLE);
        }
        return new ReprocessJobDtos.CancelResponse(jobId, "CANCELLING");
    }

    private void run(long jobId, ReprocessJobDtos.CreateRequest request, Long[] devices, long total) {
        long org = request.organizationId();
        Instant started = clock.instant();
        long processed = 0;
        long failed = 0;
        long afterId = 0;
        long windowStart = System.nanoTime();
        int inWindow = 0;
        try {
            jobs.updateProgress(org, jobId, "RUNNING", 0, 0, started, null);
            while (true) {
                if (!"RUNNING".equals(jobs.findStatus(org, jobId).orElse(""))) {
                    log.info("재처리 작업 RJ-{} 취소됨(처리 {}건)", jobId, processed);
                    return;
                }
                List<ReprocessJobRepository.RawRef> batch = jobs.listTargets(org, request.sourceId(), devices,
                        request.from(), request.to(), request.onlyFailed(), afterId, BATCH);
                if (batch.isEmpty()) {
                    break;
                }
                for (ReprocessJobRepository.RawRef ref : batch) {
                    afterId = ref.id();
                    var raw = raws.findById(org, ref.id());
                    if (raw.isEmpty()) {
                        continue;
                    }
                    IngestProcessor.Outcome outcome = processor.reprocess(raw.get());
                    processed++;
                    if (outcome.status() != RawMessageStatus.OK && outcome.status() != RawMessageStatus.DUPLICATE) {
                        failed++;
                    }
                    if (++inWindow >= RATE_PER_SECOND) {
                        long rest = Duration.ofSeconds(1).toNanos() - (System.nanoTime() - windowStart);
                        if (rest > 0) {
                            LockSupport.parkNanos(rest);
                        }
                        windowStart = System.nanoTime();
                        inWindow = 0;
                    }
                }
                jobs.updateProgress(org, jobId, "RUNNING", processed, failed, null, null);
            }
            jobs.updateProgress(org, jobId, "COMPLETED", processed, failed, null, clock.instant());
            log.info("재처리 작업 RJ-{} 완료: {}/{}건, 실패 {}", jobId, processed, total, failed);
        } catch (RuntimeException e) {
            log.error("재처리 작업 RJ-{} 실패: {}", jobId, e.getMessage(), e);
            try {
                jobs.updateProgress(org, jobId, "FAILED", processed, failed, null, clock.instant());
            } catch (RuntimeException ignored) {
                // DB 장애: RUNNING으로 남는다
            }
        }
    }

    @Override
    public void close() {
        runner.shutdownNow();
    }
}
