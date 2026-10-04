package net.java21.data2flow.pipeline.quality.domain;

import net.java21.data2flow.contracts.message.event.ClockSkewSuspected;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 기기 시계 오차 감지(ING-06.04, BR-ING-18). 기기가 보낸 측정 시각(보정 전)과 수신 시각의 차이(측정 − 수신, 초)를 기기별로 최근 1시간 창에
 * 모아 평균을 낸다. 평균 차이의 크기가 5분을 넘는 상태가 30분 이어지면 한 번 "시계 오차 의심"({@code ingest.clock-skew.suspected},
 * EVT-ING-06)을 내고, 평균이 2분 이하로 돌아오면 해제한다(다시 넘으면 다시 30분을 센다). 시각은 수신 시각(사건 시각)으로 센다.
 *
 * <p>상태는 인스턴스 메모리에 둔다. 한 기기의 메시지는 한 파티션 소비자가 순서대로 처리하므로(라우팅 키) 기기 상태가 섞이지 않는다.
 * 인스턴스가 바뀌면 창이 비어 다시 모은다(기기 상세의 평균 오차는 {@code device_state.clock_skew_avg_sec}에 남는다).
 */
public class ClockSkewDetector {

    private final Duration threshold;
    private final Duration window;
    private final Duration sustain;
    private final Duration clear;
    private final Map<Long, State> states = new ConcurrentHashMap<>();

    public ClockSkewDetector(Duration threshold, Duration window, Duration sustain, Duration clear) {
        this.threshold = threshold;
        this.window = window;
        this.sustain = sustain;
        this.clear = clear;
    }

    /**
     * 측정 하나를 반영한다.
     *
     * @param measuredAt 기기가 보낸 측정 시각(보정 전). 없으면 반영하지 않는다
     * @param receivedAt 수신 시각
     * @return 반영 결과. 측정 시각이 없으면 null
     */
    public Observation observe(long deviceId, Instant measuredAt, Instant receivedAt) {
        if (measuredAt == null) {
            return null;
        }
        State state = states.computeIfAbsent(deviceId, id -> new State());
        synchronized (state) {
            double skew = (measuredAt.toEpochMilli() - receivedAt.toEpochMilli()) / 1000.0;
            state.samples.addLast(new double[]{receivedAt.toEpochMilli(), skew});
            state.sum += skew;
            long floor = receivedAt.minus(window).toEpochMilli();
            while (!state.samples.isEmpty() && state.samples.peekFirst()[0] < floor) {
                state.sum -= state.samples.removeFirst()[1];
            }
            double avg = state.sum / state.samples.size();
            ClockSkewSuspected event = null;
            boolean cleared = false;
            if (Math.abs(avg) > threshold.toSeconds()) {
                if (state.since == null) {
                    state.since = receivedAt;
                }
                if (!state.suspected && !receivedAt.isBefore(state.since.plus(sustain))) {
                    state.suspected = true;
                    event = new ClockSkewSuspected(deviceId, Math.round(avg * 10) / 10.0, state.since);
                }
            } else if (state.suspected) {
                if (Math.abs(avg) <= clear.toSeconds()) {
                    state.suspected = false;
                    state.since = null;
                    cleared = true;
                }
            } else {
                state.since = null;
            }
            return new Observation((int) Math.round(avg), state.since, state.suspected, event, cleared);
        }
    }

    /** 테스트·재시작 */
    public void reset() {
        states.clear();
    }

    /**
     * @param avgSkewSec 최근 1시간 평균 차이(초, 측정 − 수신)
     * @param since      기준을 처음 넘은 시각. 넘지 않았으면 null
     * @param suspected  의심 상태
     * @param event      이번에 새로 의심이 된 경우의 이벤트 페이로드. 아니면 null
     * @param cleared    이번에 해제됨
     */
    public record Observation(int avgSkewSec, Instant since, boolean suspected, ClockSkewSuspected event, boolean cleared) {
    }

    private static final class State {
        final Deque<double[]> samples = new ArrayDeque<>();
        double sum;
        Instant since;
        boolean suspected;
    }
}
