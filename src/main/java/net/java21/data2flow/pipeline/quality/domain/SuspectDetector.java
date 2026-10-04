package net.java21.data2flow.pipeline.quality.domain;

import net.java21.data2flow.pipeline.metric.domain.MetricDefinition;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 의심 값 판정(ING-04.01 quality 3 "값 멈춤·급변", TC-ING-054). 기기·측정 항목마다 직전 값을 기억해
 *
 * <ul>
 *   <li><b>값 멈춤:</b> 같은 값이 {@code stuckCount}(12)번 이어지면 12번째부터 의심. 상태형(door 등)·불리언·열거형·배터리처럼
 *       같은 값이 정상인 항목은 보지 않는다.</li>
 *   <li><b>급변:</b> 유효 범위가 정의된 항목에서 직전 값과의 차이가 1분당 (범위 폭 × {@code jumpRangeFraction})를 넘으면 의심
 *       (-20~60℃ 온도면 15℃/분). 1분보다 가까운 두 점은 1분으로 보고 비교한다.</li>
 * </ul>
 * 늦게 온 값(ING-06.03)은 순서가 맞지 않으므로 판정하지 않고 상태도 바꾸지 않는다. 상태는 인스턴스 메모리(한 기기는 한 소비자).
 */
public class SuspectDetector {

    private static final Set<String> STUCK_EXEMPT = Set.of("battery");

    private final int stuckCount;
    private final double jumpRangeFraction;
    private final Map<String, State> states = new ConcurrentHashMap<>();

    public SuspectDetector(int stuckCount, double jumpRangeFraction) {
        this.stuckCount = stuckCount;
        this.jumpRangeFraction = jumpRangeFraction;
    }

    /** 이 값이 의심(quality 3)인가. 판정 대상이 아니면 false */
    public boolean observe(long deviceId, String key, MetricDefinition def, double value, Instant measuredAt, boolean late) {
        if (late) {
            return false;
        }
        State state = states.computeIfAbsent(deviceId + "|" + key, k -> new State());
        synchronized (state) {
            if (state.lastTime != null && !measuredAt.isAfter(state.lastTime)) {
                return false; // 같은 시각 재전송·순서 뒤바뀜은 판정하지 않는다
            }
            boolean suspect = false;
            if (state.lastTime != null) {
                if (value == state.lastValue) {
                    state.run++;
                } else {
                    state.run = 1;
                }
                if (stuckApplies(key, def) && state.run >= stuckCount) {
                    suspect = true;
                }
                if (def != null && def.validMin() != null && def.validMax() != null && def.validMax() > def.validMin()) {
                    double minutes = Math.max(1.0, (measuredAt.toEpochMilli() - state.lastTime.toEpochMilli()) / 60_000.0);
                    double perMinute = Math.abs(value - state.lastValue) / minutes;
                    if (perMinute > (def.validMax() - def.validMin()) * jumpRangeFraction) {
                        suspect = true;
                    }
                }
            } else {
                state.run = 1;
            }
            state.lastValue = value;
            state.lastTime = measuredAt;
            return suspect;
        }
    }

    private static boolean stuckApplies(String key, MetricDefinition def) {
        if (def == null || def.stateType() || STUCK_EXEMPT.contains(key)) {
            return false;
        }
        return def.valueType() == null || "NUMBER".equalsIgnoreCase(def.valueType());
    }

    public void reset() {
        states.clear();
    }

    private static final class State {
        double lastValue;
        Instant lastTime;
        int run;
    }
}
