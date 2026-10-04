package net.java21.data2flow.pipeline.telemetry.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * 기기별 최근 값 창(TRANSFORM {@code ctx.window(key, duration)}, 수식 {@code rolling_*}, BR-SCR-19: 1분~24시간, 최대 1,440개).
 * 실행 엔진의 기기별 상태는 메모리 + PostgreSQL이다: 기기를 처음 쓸 때 {@code telemetry}에서 최근 24시간을 읽어 채우고(인스턴스가
 * 바뀌어도 이어짐), 그 뒤로는 저장한 값을 더한다. 늦게 온 값(ING-06.03)은 창에 넣지 않는다. 창이 필요한 기기만 메모리에 둔다
 * (오래 쓰지 않은 기기는 1시간 뒤 비운다).
 */
public class RecentValues {

    public static final Duration MAX_WINDOW = Duration.ofHours(24);
    public static final int MAX_POINTS = 1_440;

    private final BiFunction<Long, Instant, Map<String, List<double[]>>> loader;
    private final Cache<Long, DeviceWindow> devices = Caffeine.newBuilder().expireAfterAccess(Duration.ofHours(1))
            .maximumSize(50_000).build();

    /** @param loader 기기 ID, 이 시각 이후 → 측정 키별 [[epochMillis, value]…](시각 순) */
    public RecentValues(BiFunction<Long, Instant, Map<String, List<double[]>>> loader) {
        this.loader = loader;
    }

    /** 창 데이터: 키마다 {@code before} 이전 24시간 값(시각 순). 처음이면 DB에서 채운다 */
    public Map<String, List<double[]>> window(long deviceId, Set<String> keys, Instant before) {
        DeviceWindow w = devices.get(deviceId, id -> seed(id, before));
        Map<String, List<double[]>> out = new LinkedHashMap<>();
        long to = before.toEpochMilli();
        long from = before.minus(MAX_WINDOW).toEpochMilli();
        synchronized (w) {
            for (String key : keys) {
                Deque<double[]> points = w.points.get(key);
                List<double[]> list = new ArrayList<>();
                if (points != null) {
                    for (double[] p : points) {
                        if (p[0] >= from && p[0] < to) {
                            list.add(p);
                        }
                    }
                }
                out.put(key, list);
            }
        }
        return out;
    }

    /** 저장한 값 반영(창을 쓰는 기기만). 늦은 값은 넣지 않는다 */
    public void record(long deviceId, Instant measuredAt, Map<String, Double> values, boolean late) {
        if (late) {
            return;
        }
        DeviceWindow w = devices.getIfPresent(deviceId);
        if (w == null) {
            return;
        }
        long t = measuredAt.toEpochMilli();
        synchronized (w) {
            values.forEach((key, v) -> {
                Deque<double[]> points = w.points.computeIfAbsent(key, k -> new ArrayDeque<>());
                if (!points.isEmpty() && points.peekLast()[0] >= t) {
                    return;
                }
                points.addLast(new double[]{t, v});
                long floor = t - MAX_WINDOW.toMillis();
                while (!points.isEmpty() && (points.peekFirst()[0] < floor || points.size() > MAX_POINTS)) {
                    points.removeFirst();
                }
            });
        }
    }

    public void clear() {
        devices.invalidateAll();
    }

    private DeviceWindow seed(long deviceId, Instant before) {
        DeviceWindow w = new DeviceWindow();
        loader.apply(deviceId, before.minus(MAX_WINDOW)).forEach((key, list) -> {
            Deque<double[]> points = new ArrayDeque<>(list);
            while (points.size() > MAX_POINTS) {
                points.removeFirst();
            }
            w.points.put(key, points);
        });
        return w;
    }

    private static final class DeviceWindow {
        final Map<String, Deque<double[]>> points = new ConcurrentHashMap<>();
    }
}
