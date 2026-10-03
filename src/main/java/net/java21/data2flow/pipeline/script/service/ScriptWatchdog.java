package net.java21.data2flow.pipeline.script.service;

import org.graalvm.polyglot.Context;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 스크립트 실행 감시(SCR-02.02 TC-SCR-030, BR-SCR-02). 실행 스레드의 CPU 시간이 한도를 넘거나 벽시계 상한을 넘으면
 * {@code context.close(true)}로 강제 중단한다. 감시 스레드 하나가 2ms마다 활성 실행을 확인하고, 닫기는 별도 스레드에서 해서
 * 취소가 늦게 반영되는 실행이 다른 실행의 감시를 막지 않게 한다. 스레드는 실행 수와 무관하게 두 개로 고정이다(누수 없음).
 */
public final class ScriptWatchdog implements AutoCloseable {

    private static final long TICK_NANOS = Duration.ofMillis(2).toNanos();

    private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    private final boolean cpuTimeSupported;
    private final Map<Long, Watch> active = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private final ExecutorService closer = Executors.newSingleThreadExecutor(r -> daemon(r, "script-watchdog-closer"));
    private final Thread ticker;
    private volatile boolean running = true;

    public ScriptWatchdog() {
        boolean supported;
        try {
            supported = threads.isThreadCpuTimeSupported();
            if (supported && !threads.isThreadCpuTimeEnabled()) {
                threads.setThreadCpuTimeEnabled(true);
            }
        } catch (UnsupportedOperationException | SecurityException e) {
            supported = false;
        }
        this.cpuTimeSupported = supported;
        this.ticker = daemon(this::loop, "script-watchdog");
        this.ticker.start();
    }

    /** 감시를 시작한다. 실행이 끝나면 반드시 {@link Watch#close()}를 부른다 */
    public Watch start(Context context, Duration cpuLimit, Duration wallLimit) {
        Thread current = Thread.currentThread();
        long threadId = current.threadId();
        boolean cpu = cpuTimeSupported && !current.isVirtual();
        long cpuStart = cpu ? threads.getThreadCpuTime(threadId) : -1;
        Watch watch = new Watch(sequence.incrementAndGet(), context, threadId, cpu, cpuStart, System.nanoTime(),
                cpuLimit.toNanos(), wallLimit.toNanos());
        active.put(watch.id, watch);
        return watch;
    }

    /** 감시 중인 실행 수(테스트·지표용) */
    public int activeCount() {
        return active.size();
    }

    private void loop() {
        while (running) {
            LockSupport.parkNanos(TICK_NANOS);
            long now = System.nanoTime();
            for (Watch watch : active.values()) {
                if (watch.isExpired(now)) {
                    watch.fire();
                }
            }
        }
    }

    @Override
    public void close() {
        running = false;
        LockSupport.unpark(ticker);
        closer.shutdownNow();
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** 실행 한 건의 감시 */
    public final class Watch implements AutoCloseable {
        private final long id;
        private final Context context;
        private final long threadId;
        private final boolean cpu;
        private final long cpuStart;
        private final long wallStart;
        private final long cpuLimit;
        private final long wallLimit;
        private final AtomicBoolean fired = new AtomicBoolean();
        private volatile String reason;

        private Watch(long id, Context context, long threadId, boolean cpu, long cpuStart, long wallStart, long cpuLimit,
                      long wallLimit) {
            this.id = id;
            this.context = context;
            this.threadId = threadId;
            this.cpu = cpu;
            this.cpuStart = cpuStart;
            this.wallStart = wallStart;
            this.cpuLimit = cpuLimit;
            this.wallLimit = wallLimit;
        }

        private boolean isExpired(long now) {
            long wall = now - wallStart;
            if (wall > wallLimit) {
                reason = "wall " + wall / 1_000_000 + "ms";
                return true;
            }
            if (cpu) {
                long used = threads.getThreadCpuTime(threadId);
                if (used >= 0 && used - cpuStart > cpuLimit) {
                    reason = "cpu " + (used - cpuStart) / 1_000_000 + "ms";
                    return true;
                }
                return false;
            }
            if (wall > cpuLimit) {
                reason = "wall(cpu 측정 불가) " + wall / 1_000_000 + "ms";
                return true;
            }
            return false;
        }

        /** 강제 중단 사유(진단용). 중단되지 않았으면 null */
        public String reason() {
            return reason;
        }

        private void fire() {
            if (fired.compareAndSet(false, true)) {
                active.remove(id);
                closer.execute(() -> {
                    try {
                        context.close(true);
                    } catch (RuntimeException ignored) {
                        // 이미 닫혔거나 닫는 중
                    }
                });
            }
        }

        /** 한도를 넘어 강제 중단되었는지 */
        public boolean timedOut() {
            return fired.get();
        }

        /** 지금까지 쓴 CPU 시간(나노초). 잴 수 없으면 벽시계 시간 */
        public long elapsedNanos() {
            if (cpu) {
                long used = threads.getThreadCpuTime(threadId);
                if (used >= 0) {
                    return used - cpuStart;
                }
            }
            return System.nanoTime() - wallStart;
        }

        @Override
        public void close() {
            active.remove(id);
        }
    }
}
