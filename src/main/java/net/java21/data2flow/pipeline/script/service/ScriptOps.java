package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.repository.ScriptOpsRepository;
import net.java21.data2flow.script.sandbox.ScriptErrorCode;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 스크립트 운영 기록(SCR-03.05 지표, SCR-05.01 오류 스냅샷, SCR-05.02 로그 수집). 처리 경로는 메모리에 모으기만 하고
 * {@link #flush()}(5초마다)가 DB에 쓴다(처리 지연을 늘리지 않게). 진단 기록이라 인스턴스가 죽으면 마지막 몇 초 분은 잃을 수 있다.
 *
 * <ul>
 *   <li>지표: 스크립트·버전·분마다 처리·오류·시간 초과 수, 평균·p95·최대 실행 시간, 가장 큰 입력. 여러 인스턴스·여러 번 쓴 값은
 *       더하고(건수·합) p95·최대는 큰 값을 둔다({@code script_stats_1m}).</li>
 *   <li>오류: 메시지·줄·열·입력 스냅샷(64KB 상한)·버전. 스크립트별 최근 100건만 남긴다.</li>
 *   <li>로그: 수집을 켠 스크립트(번들 {@code logCaptureUntil}이 미래)만 초당 10건까지, 넘친 건수는 지표 {@code logs_dropped}.</li>
 * </ul>
 */
public class ScriptOps {

    private static final Logger log = LoggerFactory.getLogger(ScriptOps.class);
    public static final int MAX_SNAPSHOT_BYTES = 65_536;
    public static final int ERRORS_KEPT = 100;
    public static final int LOGS_PER_SECOND = 10;
    private static final int MAX_SAMPLES = 5_000;

    private final ScriptOpsRepository repository;
    private final Clock clock;
    private final Map<StatKey, Acc> stats = new ConcurrentHashMap<>();
    private final Queue<ScriptOpsRepository.ErrorRow> errors = new ConcurrentLinkedQueue<>();
    private final Queue<ScriptOpsRepository.LogRow> logs = new ConcurrentLinkedQueue<>();
    private final Map<Long, long[]> logRate = new ConcurrentHashMap<>();

    public ScriptOps(ScriptOpsRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    /** 운영 실행 한 번 */
    public void record(long organizationId, RuntimeBundle.Script script, ScriptOutcome outcome, int inputBytes,
                       String inputJson, Long deviceId, Long rawMessageId, Instant now) {
        Instant minute = now.truncatedTo(ChronoUnit.MINUTES);
        Acc acc = stats.computeIfAbsent(new StatKey(script.scriptId(), script.versionId(), minute),
                k -> new Acc(organizationId, script.versionNo()));
        boolean timeout = !outcome.ok() && outcome.failure().code() == ScriptErrorCode.SCRIPT_TIMEOUT;
        acc.add(outcome.durationMs(), !outcome.ok(), timeout, inputBytes);
        if (!outcome.ok()) {
            String snapshot = inputJson != null && inputJson.length() <= MAX_SNAPSHOT_BYTES ? inputJson : null;
            String message = outcome.failure().message() == null ? "" : outcome.failure().message();
            errors.add(new ScriptOpsRepository.ErrorRow(organizationId, script.scriptId(), script.versionId(),
                    script.versionNo(), now, outcome.failure().code().name(), message.length() > 500 ? message.substring(0, 500)
                    : message, outcome.failure().line(), outcome.failure().col(), null, snapshot, deviceId, rawMessageId));
        }
        if (script.capturingLogs(clock.instant()) && !outcome.logs().isEmpty()) {
            long second = now.getEpochSecond();
            for (String line : outcome.logs()) {
                if (allowLog(script.scriptId(), second)) {
                    logs.add(new ScriptOpsRepository.LogRow(organizationId, script.scriptId(), script.versionNo(), now, deviceId,
                            line.length() > 1100 ? line.substring(0, 1100) : line));
                } else {
                    acc.logDropped();
                }
            }
        }
    }

    /** 스크립트당 초당 10건(BR-SCR-17) */
    boolean allowLog(long scriptId, long epochSecond) {
        long[] window = logRate.computeIfAbsent(scriptId, k -> new long[]{epochSecond, 0});
        synchronized (window) {
            if (window[0] != epochSecond) {
                window[0] = epochSecond;
                window[1] = 0;
            }
            if (window[1] >= LOGS_PER_SECOND) {
                return false;
            }
            window[1]++;
            return true;
        }
    }

    /** 모은 것을 DB에 쓴다. 실패하면 지표는 버리고(다음 분에 영향 없음) 경고만 남긴다 */
    public void flush() {
        List<ScriptOpsRepository.StatRow> rows = new ArrayList<>();
        for (StatKey key : List.copyOf(stats.keySet())) {
            Acc acc = stats.remove(key);
            if (acc != null) {
                rows.add(acc.toRow(key));
            }
        }
        List<ScriptOpsRepository.ErrorRow> errorRows = drain(errors);
        List<ScriptOpsRepository.LogRow> logRows = drain(logs);
        try {
            repository.upsertStats(rows);
            repository.insertErrors(errorRows, ERRORS_KEPT);
            repository.insertLogs(logRows);
        } catch (RuntimeException e) {
            log.warn("스크립트 운영 기록을 쓰지 못했습니다(지표 {}행, 오류 {}건, 로그 {}건): {}", rows.size(), errorRows.size(),
                    logRows.size(), e.getMessage());
        }
    }

    private static <T> List<T> drain(Queue<T> queue) {
        List<T> out = new ArrayList<>();
        T item;
        while ((item = queue.poll()) != null) {
            out.add(item);
        }
        return out;
    }

    /** p95(최근접 순위) */
    static double p95(double[] sorted, int n) {
        if (n == 0) {
            return 0;
        }
        int rank = (int) Math.ceil(0.95 * n);
        return sorted[Math.max(0, Math.min(n - 1, rank - 1))];
    }

    private record StatKey(long scriptId, long versionId, Instant minute) {
    }

    private static final class Acc {
        final long organizationId;
        final int versionNo;
        int processed;
        int errors;
        int timeouts;
        double sumMs;
        double maxMs;
        int maxInputBytes;
        int logsDropped;
        double[] samples = new double[64];
        int n;

        Acc(long organizationId, int versionNo) {
            this.organizationId = organizationId;
            this.versionNo = versionNo;
        }

        synchronized void add(double ms, boolean error, boolean timeout, int inputBytes) {
            processed++;
            if (error) {
                errors++;
            }
            if (timeout) {
                timeouts++;
            }
            sumMs += ms;
            maxMs = Math.max(maxMs, ms);
            maxInputBytes = Math.max(maxInputBytes, inputBytes);
            if (n < MAX_SAMPLES) {
                if (n == samples.length) {
                    samples = Arrays.copyOf(samples, samples.length * 2);
                }
                samples[n++] = ms;
            }
        }

        synchronized void logDropped() {
            logsDropped++;
        }

        synchronized ScriptOpsRepository.StatRow toRow(StatKey key) {
            double[] sorted = Arrays.copyOf(samples, n);
            Arrays.sort(sorted);
            return new ScriptOpsRepository.StatRow(key.scriptId(), key.versionId(), key.minute(), organizationId, versionNo,
                    processed, errors, timeouts, processed == 0 ? 0 : sumMs / processed, p95(sorted, n), maxMs, maxInputBytes,
                    logsDropped);
        }
    }
}
