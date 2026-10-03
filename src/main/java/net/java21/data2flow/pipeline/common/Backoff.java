package net.java21.data2flow.pipeline.common;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/**
 * 일시 장애 재시도 대기(지수 백오프, reliability-and-ha.md §2 ④: DB 장애가 계속되면 소비를 멈추고 기다린다).
 * {@code Thread.sleep} 대신 {@link LockSupport#parkNanos}로 짧게 나눠 기다리며, 종료 신호가 오면 바로 깬다.
 */
public final class Backoff {

    private static final long SLICE_NANOS = Duration.ofMillis(50).toNanos();

    private final Duration initial;
    private final Duration max;
    private Duration next;

    public Backoff(Duration initial, Duration max) {
        this.initial = initial;
        this.max = max;
        this.next = initial;
    }

    /** 다음 간격만큼 기다린다. 기다리는 동안 stop이 true가 되면 false를 돌려준다 */
    public boolean pause(BooleanSupplier stop) {
        long deadline = System.nanoTime() + next.toNanos();
        next = next.multipliedBy(2).compareTo(max) > 0 ? max : next.multipliedBy(2);
        while (System.nanoTime() < deadline) {
            if (stop.getAsBoolean() || Thread.currentThread().isInterrupted()) {
                return false;
            }
            LockSupport.parkNanos(Math.min(SLICE_NANOS, deadline - System.nanoTime()));
        }
        return !stop.getAsBoolean();
    }

    public void reset() {
        next = initial;
    }

    public Duration current() {
        return next;
    }
}
