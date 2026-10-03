package net.java21.data2flow.pipeline.aggregate.service;

import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.AggregatesRecomputed;
import net.java21.data2flow.pipeline.aggregate.repository.AggregateRepository;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 1분·1시간·1일 집계 유지(TSD-02.02, BR-TSD-04·05·06). 워터마크까지 증분으로 계산하고, 늦게 온 값·재처리로 생긴 다시 계산할 구간
 * ({@code agg_dirty_ranges})을 1m → 1h → 1d 순서로 다시 계산한 뒤 {@code aggregates.recomputed}(EVT-TSD-03)를 낸다.
 *
 * <ul>
 *   <li>1m: 매분, 직전 완료 분(수신 지연 흡수 여유 {@code grace})까지</li>
 *   <li>1h: 5분마다, 현재 시의 부분 구간까지 다시 계산(완료된 시까지 워터마크)</li>
 *   <li>1d: 매시 10분, 사이트 시간대 자정 기준(현재 날의 부분 구간 포함)</li>
 * </ul>
 * 원본에서 언제든 다시 만들 수 있으므로 집계 테이블에는 FK가 없다.
 */
public class AggregationService {

    public static final String LEVEL_1M = "1m";
    public static final String LEVEL_1H = "1h";
    public static final String LEVEL_1D = "1d";
    private static final Duration MAX_CHUNK = Duration.ofHours(6);
    private static final int DIRTY_BATCH = 500;

    private final AggregateRepository repository;
    private final TransactionTemplate tx;
    private final DomainEventPublisher events;
    private final PipelineProperties.Aggregation settings;
    private final Supplier<String[]> stateKeys;
    private final Clock clock;

    public AggregationService(AggregateRepository repository, TransactionTemplate tx, DomainEventPublisher events,
                              PipelineProperties.Aggregation settings, Supplier<String[]> stateKeys, Clock clock) {
        this.repository = repository;
        this.tx = tx;
        this.events = events;
        this.settings = settings;
        this.stateKeys = stateKeys;
        this.clock = clock;
    }

    /** 1m 증분 + 다시 계산. 처리한 끝 시각(워터마크) */
    public Instant aggregateMinutes() {
        Instant now = clock.instant();
        Instant to = now.minus(settings.grace()).truncatedTo(ChronoUnit.MINUTES);
        Instant from = repository.findWatermark(LEVEL_1M).orElseGet(() -> start(to));
        String[] keys = stateKeys.get();
        while (from.isBefore(to)) {
            Instant chunkEnd = min(to, from.plus(MAX_CHUNK));
            Instant f = from;
            tx.executeWithoutResult(s -> {
                repository.aggregateMinutes(f, chunkEnd, keys, null, null);
                repository.saveWatermark(LEVEL_1M, chunkEnd, now);
            });
            from = chunkEnd;
        }
        recomputeDirty(LEVEL_1M, (r, k) -> {
            repository.aggregateMinutes(r.from(), r.to(), k, r.deviceId(), r.metricKey());
            Instant hour = r.from().truncatedTo(ChronoUnit.HOURS);
            repository.insertDirty(r.organizationId(), LEVEL_1H, r.deviceId(), r.metricKey(), hour,
                    max(hour.plus(Duration.ofHours(1)), r.to().truncatedTo(ChronoUnit.HOURS)), r.reason());
        }, keys);
        return repository.findWatermark(LEVEL_1M).orElse(to);
    }

    /** 1h: 완료된 1m까지(현재 시는 부분 구간으로 다시 계산) */
    public Instant aggregateHours() {
        Instant now = clock.instant();
        Instant minuteMark = repository.findWatermark(LEVEL_1M).orElse(null);
        if (minuteMark != null) {
            Instant from = repository.findWatermark(LEVEL_1H).orElseGet(() -> start(minuteMark))
                    .truncatedTo(ChronoUnit.HOURS);
            while (from.isBefore(minuteMark)) {
                Instant chunkEnd = min(minuteMark, from.plus(MAX_CHUNK));
                Instant f = from;
                tx.executeWithoutResult(s -> {
                    repository.aggregateHours(f, chunkEnd, null, null);
                    repository.saveWatermark(LEVEL_1H, chunkEnd.truncatedTo(ChronoUnit.HOURS), now);
                });
                from = chunkEnd;
            }
        }
        recomputeDirty(LEVEL_1H, (r, k) -> {
            Instant hourFrom = r.from().truncatedTo(ChronoUnit.HOURS);
            Instant hourTo = max(hourFrom.plus(Duration.ofHours(1)), ceilHour(r.to()));
            repository.aggregateHours(hourFrom, hourTo, r.deviceId(), r.metricKey());
            ZoneId zone = settings.defaultZone();
            Instant day = dayStart(hourFrom, zone);
            repository.insertDirty(r.organizationId(), LEVEL_1D, r.deviceId(), r.metricKey(), day,
                    max(nextDay(day, zone), dayStart(hourTo.minusNanos(1), zone).plus(Duration.ofDays(1))), r.reason());
        }, null);
        return repository.findWatermark(LEVEL_1H).orElse(null);
    }

    /** 1d: 사이트 시간대 자정 기준(BR-TSD-05). 현재 날은 부분 구간 */
    public Instant aggregateDays() {
        Instant now = clock.instant();
        ZoneId zone = settings.defaultZone();
        Instant hourMark = repository.findWatermark(LEVEL_1M).orElse(null);
        if (hourMark != null) {
            Instant from = dayStart(repository.findWatermark(LEVEL_1D).orElseGet(() -> start(hourMark)), zone);
            Instant to = hourMark;
            while (from.isBefore(to)) {
                Instant chunkEnd = min(nextDay(from, zone), to);
                Instant f = from;
                Instant end = nextDay(from, zone);
                tx.executeWithoutResult(s -> {
                    repository.aggregateDays(f, end, zone, null, null);
                    repository.saveWatermark(LEVEL_1D, dayStart(chunkEnd, zone), now);
                });
                from = end;
            }
        }
        recomputeDirty(LEVEL_1D, (r, k) -> repository.aggregateDays(dayStart(r.from(), zone),
                max(nextDay(dayStart(r.from(), zone), zone), r.to()), zone, r.deviceId(), r.metricKey()), null);
        return repository.findWatermark(LEVEL_1D).orElse(null);
    }

    private Instant start(Instant to) {
        Instant floor = to.minus(settings.backfill());
        return repository.findEarliestTelemetry(floor)
                .map(t -> t.truncatedTo(ChronoUnit.MINUTES))
                .map(t -> t.isBefore(to) ? t : to)
                .orElse(to);
    }

    private void recomputeDirty(String level, DirtyAction action, String[] keys) {
        while (true) {
            List<AggregateRepository.DirtyRange> batch = repository.findDirty(level, DIRTY_BATCH);
            if (batch.isEmpty()) {
                return;
            }
            Map<Long, List<AggregatesRecomputed.Item>> byOrg = new LinkedHashMap<>();
            tx.executeWithoutResult(s -> {
                for (AggregateRepository.DirtyRange r : batch) {
                    action.apply(r, keys);
                    byOrg.computeIfAbsent(r.organizationId(), o -> new ArrayList<>())
                            .add(new AggregatesRecomputed.Item(r.deviceId(), r.metricKey(), r.from(), r.to()));
                }
                repository.deleteDirty(batch.stream().map(AggregateRepository.DirtyRange::id).toList());
                byOrg.forEach((org, items) -> events.publish(EventType.AGGREGATES_RECOMPUTED, org,
                        new AggregatesRecomputed(level, items)));
            });
            if (batch.size() < DIRTY_BATCH) {
                return;
            }
        }
    }

    static Instant dayStart(Instant instant, ZoneId zone) {
        return instant.atZone(zone).toLocalDate().atStartOfDay(zone).toInstant();
    }

    static Instant nextDay(Instant dayStart, ZoneId zone) {
        return dayStart.atZone(zone).toLocalDate().plusDays(1).atStartOfDay(zone).toInstant();
    }

    private static Instant ceilHour(Instant instant) {
        Instant floor = instant.truncatedTo(ChronoUnit.HOURS);
        return floor.equals(instant) ? floor : floor.plus(Duration.ofHours(1));
    }

    private static Instant min(Instant a, Instant b) {
        return a.isBefore(b) ? a : b;
    }

    private static Instant max(Instant a, Instant b) {
        return a.isAfter(b) ? a : b;
    }

    @FunctionalInterface
    private interface DirtyAction {
        void apply(AggregateRepository.DirtyRange range, String[] stateKeys);
    }
}
