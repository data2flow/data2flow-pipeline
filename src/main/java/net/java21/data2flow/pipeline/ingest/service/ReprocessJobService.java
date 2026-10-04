package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.ReprocessJobFinished;
import net.java21.data2flow.pipeline.common.PipelineErrorCode;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.service.SourceContextCache;
import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;
import net.java21.data2flow.pipeline.ingest.domain.ReprocessProgress;
import net.java21.data2flow.pipeline.ingest.dto.ReprocessJobDtos;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRepository;
import net.java21.data2flow.pipeline.ingest.repository.ReprocessJobRepository;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.service.BundleCodec;
import net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;
import java.util.function.DoubleSupplier;

/**
 * 기간 재처리 작업(ING-01.04, API-ING-23, BR-ING-12·13). 보관된 원본을 다시 처리해 같은 원본 행과 같은 telemetry 키를 덮어쓴다.
 *
 * <ul>
 *   <li><b>버전 고정:</b> 작업을 만들 때 조직의 스크립트 실행 번들(디코더·TRANSFORM·모듈·수식)을 {@code pinned_bundle}에 고정하고,
 *       처리 중 새 버전이 배포되어도 작업 전체를 그 번들로 처리한다.</li>
 *   <li><b>처리량:</b> 실시간 소비와 별도 스레드에서 초당 {@code ratePerSecond}(500)건. 실시간 처리 지연이 경고 기준(1분)을 넘으면
 *       {@code slowRatePerSecond}(100)로 낮춘다.</li>
 *   <li><b>인스턴스 장애:</b> 작업을 맡은 인스턴스가 10초마다 생존 신호를 남기고, 2분 넘게 멈춘 작업은 다른 인스턴스가 넘겨받아
 *       {@code last_raw_id} 다음부터 잇는다(이미 처리한 원본은 같은 키 덮어쓰기라 다시 처리해도 결과가 같다).</li>
 *   <li><b>결과 알림:</b> 재처리한 텔레메트리는 {@code data2flow.telemetry}에 다시 내지 않는다(플로우가 과거 값에 다시 반응하지 않게,
 *       ADR-048 후속 결정). 바뀐 구간은 집계 재계산({@code agg_dirty_ranges} → {@code aggregates.recomputed})으로, 작업 끝은
 *       {@code ingest.reprocess.finished}(EVT-ING-09)로 알린다. 취소는 취소 요청을 받은 쪽이 바로 알린다.</li>
 * </ul>
 */
public class ReprocessJobService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReprocessJobService.class);
    private static final int BATCH = 200;

    private final ReprocessJobRepository jobs;
    private final RawMessageRepository raws;
    private final IngestProcessor processor;
    private final SourceContextCache sources;
    private final ScriptRuntimeRegistry scripts;
    private final DomainEventPublisher events;
    private final PipelineProperties properties;
    private final DoubleSupplier realtimeLagSeconds;
    private final Clock clock;
    private final String instanceId;
    private final Set<Long> running = ConcurrentHashMap.newKeySet();
    private final ExecutorService runner = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("reprocess-job").factory());

    public ReprocessJobService(ReprocessJobRepository jobs, RawMessageRepository raws, IngestProcessor processor,
                               SourceContextCache sources, ScriptRuntimeRegistry scripts, DomainEventPublisher events,
                               PipelineProperties properties, DoubleSupplier realtimeLagSeconds, Clock clock) {
        this.jobs = jobs;
        this.raws = raws;
        this.processor = processor;
        this.sources = sources;
        this.scripts = scripts;
        this.events = events;
        this.properties = properties;
        this.realtimeLagSeconds = realtimeLagSeconds;
        this.clock = clock;
        this.instanceId = properties.instanceId();
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
        // BR-ING-12: 시작 시점 버전 고정. 최신 번들을 읽어(배포 직후 재처리 SCR-03.06도 방금 배포한 버전) 작업에 묶는다
        RuntimeBundle bundle = scripts.reload(org);
        long jobId;
        try {
            jobId = jobs.insert(new ReprocessJobRepository.NewJob(org, request.sourceId(), devices, request.from(),
                    request.to(), total, decoder == null ? "unknown" : decoder, scriptVersions(bundle),
                    request.requestedBy(), request.memo(), request.onlyFailed(), BundleCodec.write(bundle), instanceId, now));
        } catch (DuplicateKeyException e) {
            throw new BusinessException(PipelineErrorCode.ING_REPROCESS_ALREADY_RUNNING);
        }
        runner.execute(() -> claimAndRun(jobId));
        return new ReprocessJobDtos.CreateResponse(jobId, "QUEUED", total);
    }

    public ReprocessJobDtos.CancelResponse cancel(long jobId, ReprocessJobDtos.CancelRequest request) {
        Optional<ReprocessJobRepository.Job> cancelled = jobs.updateCancelled(request.organizationId(), jobId, clock.instant());
        if (cancelled.isEmpty()) {
            if (jobs.findStatus(request.organizationId(), jobId).isEmpty()) {
                throw new BusinessException(CommonErrorCode.RESOURCE_NOT_FOUND);
            }
            throw new BusinessException(PipelineErrorCode.ING_REPROCESS_NOT_CANCELLABLE);
        }
        publishFinished(cancelled.get(), ReprocessJobFinished.Status.CANCELLED, cancelled.get().progress(), null);
        return new ReprocessJobDtos.CancelResponse(jobId, "CANCELLING");
    }

    /**
     * 다른 인스턴스가 죽어 멈춘 작업을 넘겨받는다(모든 인스턴스가 30초마다 부른다. 넘겨받기는 원자적 UPDATE라 한 인스턴스만 맡는다).
     *
     * @return 넘겨받아 실행을 맡긴 작업 ID
     */
    public List<Long> recoverStale() {
        Instant now = clock.instant();
        List<Long> stale = jobs.findStale(now.minus(properties.reprocess().staleAfter()));
        List<Long> taken = new java.util.ArrayList<>();
        for (Long id : stale) {
            if (running.contains(id)) {
                continue;
            }
            Optional<ReprocessJobRepository.Job> job = jobs.claim(id, instanceId, now, now.minus(properties.reprocess().staleAfter()));
            if (job.isPresent()) {
                log.warn("재처리 작업 RJ-{}을(를) 넘겨받습니다(이전 담당 {}, 마지막 신호 {}, 처리 {}건부터)", id,
                        job.get().owner(), job.get().heartbeatAt(), job.get().progress().processed());
                taken.add(id);
                runner.execute(() -> run(job.get()));
            }
        }
        return taken;
    }

    private void claimAndRun(long jobId) {
        Instant now = clock.instant();
        Optional<ReprocessJobRepository.Job> job = jobs.claim(jobId, instanceId, now,
                now.minus(properties.reprocess().staleAfter()));
        job.ifPresent(this::run);
    }

    private void run(ReprocessJobRepository.Job job) {
        if (!running.add(job.id())) {
            return;
        }
        long org = job.organizationId();
        ReprocessJobRepository.Progress p = job.progress();
        long processed = p.processed();
        long failed = p.failed();
        long skipped = p.skipped();
        long afterId = p.lastRawId();
        ScriptRuntimeRegistry.Plan pinned = new ScriptRuntimeRegistry.Plan(BundleCodec.read(job.pinnedBundle()));
        RateLimiter limiter = new RateLimiter();
        Instant lastBeat = clock.instant();
        try {
            while (true) {
                List<ReprocessJobRepository.RawRef> batch = jobs.listTargets(org, job, afterId, BATCH);
                if (batch.isEmpty()) {
                    break;
                }
                for (ReprocessJobRepository.RawRef ref : batch) {
                    afterId = ref.id();
                    var raw = raws.findById(org, ref.id());
                    if (raw.isEmpty()) {
                        skipped++;
                        continue;
                    }
                    IngestProcessor.Outcome outcome = processor.reprocess(raw.get(), pinned);
                    if (outcome.replayed()) {
                        skipped++; // 다시 처리해도 같은 결과(서명 거부 원본 등)
                    } else {
                        processed++;
                        if (outcome.status() != RawMessageStatus.OK && outcome.status() != RawMessageStatus.DUPLICATE) {
                            failed++;
                        }
                    }
                    limiter.acquire(currentRate());
                }
                ReprocessJobRepository.Progress progress = new ReprocessJobRepository.Progress(processed, failed, skipped, afterId);
                if (!jobs.updateProgress(org, job.id(), instanceId, progress, clock.instant())) {
                    log.info("재처리 작업 RJ-{} 중단(취소 또는 다른 인스턴스가 넘겨받음, 처리 {}건)", job.id(), processed);
                    return;
                }
                lastBeat = clock.instant();
            }
            ReprocessJobRepository.Progress done = new ReprocessJobRepository.Progress(processed, failed, skipped, afterId);
            if (jobs.finish(org, job.id(), instanceId, "COMPLETED", done, null, clock.instant())) {
                log.info("재처리 작업 RJ-{} 완료: {}/{}건, 실패 {}, 건너뜀 {}", job.id(), processed, job.total(), failed, skipped);
                publishFinished(job, ReprocessJobFinished.Status.COMPLETED, done, null);
            }
        } catch (RuntimeException e) {
            log.error("재처리 작업 RJ-{} 실패: {}", job.id(), e.getMessage(), e);
            ReprocessJobRepository.Progress done = new ReprocessJobRepository.Progress(processed, failed, skipped, afterId);
            try {
                if (jobs.finish(org, job.id(), instanceId, "FAILED", done, String.valueOf(e.getMessage()), clock.instant())) {
                    publishFinished(job, ReprocessJobFinished.Status.FAILED, done, String.valueOf(e.getMessage()));
                }
            } catch (RuntimeException ignored) {
                // DB 장애: RUNNING으로 남고 생존 신호가 멈추므로 2분 뒤 다른(또는 이) 인스턴스가 넘겨받는다
                log.debug("재처리 작업 RJ-{} 실패 기록 못 함(마지막 신호 {})", job.id(), lastBeat);
            }
        } finally {
            running.remove(job.id());
        }
    }

    private int currentRate() {
        PipelineProperties.Reprocess r = properties.reprocess();
        return realtimeLagSeconds.getAsDouble() > properties.lag().warn().toSeconds() ? r.slowRatePerSecond() : r.ratePerSecond();
    }

    private void publishFinished(ReprocessJobRepository.Job job, ReprocessJobFinished.Status status,
                                 ReprocessJobRepository.Progress p, String error) {
        try {
            events.publish(EventType.INGEST_REPROCESS_FINISHED, job.organizationId(), new ReprocessJobFinished(
                    Long.toString(job.id()), status, job.sourceId(), job.deviceIds(), job.from(), job.to(), job.total(),
                    p.processed(), p.failed(), p.skipped(), job.requestedBy(), error, clock.instant()));
        } catch (RuntimeException e) {
            log.warn("ingest.reprocess.finished 발행 실패(RJ-{}): {}", job.id(), e.getMessage());
        }
    }

    /** 진행률(UI-ING-05): 처리 건수 ÷ 대상 건수, 소수 1자리 */
    public static double percent(long done, long total) {
        return ReprocessProgress.percent(done, total);
    }

    private static String scriptVersions(RuntimeBundle bundle) {
        StringBuilder json = new StringBuilder("{");
        for (RuntimeBundle.Script s : bundle.scripts()) {
            if (json.length() > 1) {
                json.append(',');
            }
            json.append('"').append(s.scriptId()).append("\":").append(s.versionNo());
        }
        return json.append('}').toString();
    }

    @Override
    public void close() {
        runner.shutdownNow();
    }

    /** 초당 건수 제한(BR-ING-13). 1초 창마다 정한 건수를 넘으면 창 끝까지 기다린다 */
    private static final class RateLimiter {
        private long windowStart = System.nanoTime();
        private int inWindow;

        void acquire(int perSecond) {
            if (++inWindow >= perSecond) {
                long rest = Duration.ofSeconds(1).toNanos() - (System.nanoTime() - windowStart);
                if (rest > 0) {
                    LockSupport.parkNanos(rest);
                }
                windowStart = System.nanoTime();
                inWindow = 0;
            }
        }
    }
}
