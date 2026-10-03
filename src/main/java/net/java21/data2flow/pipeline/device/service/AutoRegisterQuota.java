package net.java21.data2flow.pipeline.device.service;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 미등록 기기 폭주 방지(ING-07.02, BR-ING-09 TC-ING-083): 소스당 고정 1시간 창에서 한도까지만 자동 등록을 요청한다.
 * 진짜 한도는 core(API-DEV-121, 429)가 지키고, 이것은 pipeline 인스턴스가 한도를 넘는 요청으로 core를 두드리지 않게 하는 1차 방어다.
 * 관리자가 한도를 풀면(API-ING-16 → 소스 설정 변경 이벤트) {@link #reset(long)}.
 */
public class AutoRegisterQuota {

    private static final long WINDOW_MS = Duration.ofHours(1).toMillis();

    private final Clock clock;
    private final Map<Long, Window> windows = new ConcurrentHashMap<>();

    public AutoRegisterQuota(Clock clock) {
        this.clock = clock;
    }

    /** 이번 창에서 한 대 더 등록해도 되는지(되면 수를 센다) */
    public Decision tryAcquire(long sourceId, int hourlyLimit) {
        long window = Math.floorDiv(clock.millis(), WINDOW_MS);
        Window w = windows.compute(sourceId, (k, old) -> old == null || old.window != window ? new Window(window) : old);
        synchronized (w) {
            if (w.blocked || w.used >= hourlyLimit) {
                boolean firstRejection = !w.blocked;
                w.blocked = true;
                return new Decision(false, firstRejection, w.used);
            }
            w.used++;
            return new Decision(true, false, w.used);
        }
    }

    /** core가 한도 초과(429)를 알려 오면 이 창을 막는다. 처음 막힌 것이면 true(알람 1회) */
    public boolean markExceeded(long sourceId) {
        long window = Math.floorDiv(clock.millis(), WINDOW_MS);
        Window w = windows.compute(sourceId, (k, old) -> old == null || old.window != window ? new Window(window) : old);
        synchronized (w) {
            boolean first = !w.blocked;
            w.blocked = true;
            return first;
        }
    }

    /** 한도 해제(API-ING-16) 또는 소스 설정 변경 */
    public void reset(long sourceId) {
        windows.remove(sourceId);
    }

    /**
     * @param allowed        등록 요청해도 됨
     * @param firstRejection 이 창에서 처음 거부(알람을 한 번만 낸다)
     * @param used           이 창에서 쓴 수
     */
    public record Decision(boolean allowed, boolean firstRejection, int used) {
    }

    private static final class Window {
        private final long window;
        private int used;
        private boolean blocked;

        private Window(long window) {
            this.window = window;
        }
    }
}
