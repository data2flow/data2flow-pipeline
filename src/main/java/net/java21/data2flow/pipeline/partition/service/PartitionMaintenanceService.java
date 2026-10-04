package net.java21.data2flow.pipeline.partition.service;

import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.PartitionWarning;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.ingest.repository.DlqItemRepository;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import net.java21.data2flow.pipeline.partition.repository.PartitionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 파티션 관리 스케줄러(ADR-019, BR-TSD-01·02, BR-ING-14). pg_partman 없이 우리 코드가 한다.
 *
 * <ul>
 *   <li>월 파티션: {@code telemetry}·{@code telemetry_1m}·{@code link_qualities}를 과거 한도가 든 달부터 3개월 앞까지</li>
 *   <li>연 파티션: {@code telemetry_1h} 올해·내년</li>
 *   <li>일 파티션: {@code raw_messages} 어제부터 7일 앞까지. 31일이 지난 일 파티션은 DETACH(lock_timeout 5초) 후 DROP</li>
 *   <li>DEFAULT 파티션에 그 범위 행이 이미 있으면 떼었다 붙이며 옮긴다. DEFAULT에 행이 남아 있으면 {@code partition.warning}
 *       (EVT-TSD-06, {@code default_partition_rows}), 만들기에 실패하면 {@code create_failed}</li>
 *   <li>DLQ 14일 지난 항목 삭제</li>
 * </ul>
 * 매일 01:00 UTC와 시작할 때 실행한다(ShedLock으로 한 인스턴스만).
 */
public class PartitionMaintenanceService {

    private static final Logger log = LoggerFactory.getLogger(PartitionMaintenanceService.class);
    private static final DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("'y'yyyy'm'MM");
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("'y'yyyy'm'MM'd'dd");

    /** 월 파티션 테이블 → 파티션 키 열 */
    static final Map<String, String> MONTHLY = Map.of("telemetry", "time", "telemetry_1m", "bucket",
            "link_qualities", "time");

    private final PartitionRepository partitions;
    private final DlqItemRepository dlq;
    private final TransactionTemplate tx;
    private final DomainEventPublisher events;
    private final PipelineProperties properties;
    private final Clock clock;
    private final Map<String, LocalDate> warned = new ConcurrentHashMap<>();

    private final java.util.function.IntSupplier rawKeepDays;

    public PartitionMaintenanceService(PartitionRepository partitions, DlqItemRepository dlq, TransactionTemplate tx,
                                       DomainEventPublisher events, PipelineProperties properties, Clock clock) {
        this(partitions, dlq, tx, events, properties, () -> (int) properties.partition().rawRetention().toDays(), clock);
    }

    /** @param rawKeepDays 원본 메시지 보관 일수(모든 조직 중 가장 긴 값, TSD-02.01) */
    public PartitionMaintenanceService(PartitionRepository partitions, DlqItemRepository dlq, TransactionTemplate tx,
                                       DomainEventPublisher events, PipelineProperties properties,
                                       java.util.function.IntSupplier rawKeepDays, Clock clock) {
        this.rawKeepDays = rawKeepDays;
        this.partitions = partitions;
        this.dlq = dlq;
        this.tx = tx;
        this.events = events;
        this.properties = properties;
        this.clock = clock;
    }

    /** 한 번 실행. 만든 파티션 이름 */
    public Result maintain() {
        Instant now = clock.instant();
        List<String> created = new ArrayList<>();
        List<String> dropped = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        PipelineProperties.Partition p = properties.partition();
        YearMonth first = YearMonth.from(now.minus(properties.ingest().pastLimit()).atZone(ZoneOffset.UTC));
        YearMonth last = YearMonth.from(now.atZone(ZoneOffset.UTC)).plusMonths(p.monthsAhead());
        for (Map.Entry<String, String> table : MONTHLY.entrySet()) {
            for (YearMonth m = first; !m.isAfter(last); m = m.plusMonths(1)) {
                ensure(table.getKey(), table.getKey() + "_" + MONTH.format(m.atDay(1)), table.getValue(),
                        m.atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC),
                        m.plusMonths(1).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC), false, created, failed);
            }
        }
        int year = now.atZone(ZoneOffset.UTC).getYear();
        for (int y = year; y <= year + 1; y++) {
            ensure("telemetry_1h", "telemetry_1h_y" + y, "bucket",
                    LocalDate.of(y, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC),
                    LocalDate.of(y + 1, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC), false, created, failed);
        }
        LocalDate today = now.atZone(ZoneOffset.UTC).toLocalDate();
        for (LocalDate d = today.minusDays(1); !d.isAfter(today.plusDays(p.rawDaysAhead())); d = d.plusDays(1)) {
            ensure("raw_messages", "raw_messages_" + DAY.format(d), "received_at",
                    d.atStartOfDay().toInstant(ZoneOffset.UTC), d.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC),
                    true, created, failed);
        }
        dropExpiredRaw(today, now, dropped);
        int purged = dlq.deleteCreatedBefore(now.minus(p.dlqRetention()));
        warnDefaultRows(today);
        if (!created.isEmpty() || !dropped.isEmpty()) {
            log.info("파티션 관리: 만듦 {}, 지움 {}, DLQ 정리 {}건", created, dropped, purged);
        }
        return new Result(created, dropped, failed, purged);
    }

    private void ensure(String parent, String partition, String column, Instant from, Instant to, boolean identity,
                        List<String> created, List<String> failed) {
        if (partitions.exists(partition)) {
            return;
        }
        Instant now = clock.instant();
        try {
            tx.executeWithoutResult(s -> {
                partitions.setLockTimeout(properties.partition().lockTimeout().toMillis());
                partitions.createPartition(parent, partition, from, to);
                partitions.register(partition, parent, from, to, now);
            });
            created.add(partition);
        } catch (DataAccessException e) {
            if (String.valueOf(e.getMessage()).contains("default partition")) {
                try {
                    int moved = tx.execute(s -> {
                        partitions.setLockTimeout(properties.partition().lockTimeout().toMillis());
                        int n = partitions.createMovingDefaultRows(parent, partition, column, from, to, identity);
                        partitions.register(partition, parent, from, to, now);
                        return n;
                    });
                    log.warn("DEFAULT 파티션의 {}행을 {}로 옮겼습니다", moved, partition);
                    created.add(partition);
                    return;
                } catch (DataAccessException moveError) {
                    e = moveError;
                }
            }
            failed.add(partition);
            log.error("파티션 {}을(를) 만들지 못했습니다: {}", partition, e.getMessage());
            publishWarning(parent, PartitionWarning.CREATE_FAILED, partition + ": " + e.getMostSpecificCause().getMessage());
        }
    }

    private void dropExpiredRaw(LocalDate today, Instant now, List<String> dropped) {
        // 보관 30일 → 31일이 지난 일 파티션(design/erd/pipeline.md §2.1)
        int days = rawKeepDays.getAsInt();
        if (days <= 0) {
            return;
        }
        LocalDate keepFrom = today.minusDays(days + 1L);
        for (String name : partitions.listPartitions("raw_messages")) {
            LocalDate day = parseDay(name);
            if (day != null && day.isBefore(keepFrom)) {
                try {
                    tx.executeWithoutResult(s -> {
                        partitions.setLockTimeout(properties.partition().lockTimeout().toMillis());
                        partitions.detachAndDrop("raw_messages", name);
                        partitions.markDropped(name, now);
                    });
                    dropped.add(name);
                } catch (DataAccessException e) {
                    log.warn("원본 파티션 {} 정리 실패(다음 실행에 다시 시도): {}", name, e.getMessage());
                }
            }
        }
    }

    private void warnDefaultRows(LocalDate today) {
        for (String parent : List.of("telemetry", "telemetry_1m", "telemetry_1h", "link_qualities", "raw_messages")) {
            long rows = partitions.countDefaultRows(parent);
            if (rows > 0 && !today.equals(warned.get(parent))) {
                warned.put(parent, today);
                publishWarning(parent, PartitionWarning.DEFAULT_PARTITION_ROWS,
                        "DEFAULT 파티션에 " + rows + "행이 있습니다(파티션 범위 밖 시각)");
            }
        }
    }

    private void publishWarning(String table, String reason, String detail) {
        try {
            // 시스템 경고: 조직 구분이 없어 기본 조직(1)으로 낸다(core-api 운영 알람이 받음)
            events.publish(EventType.PARTITION_WARNING, 1, new PartitionWarning("data2flow_pipeline." + table, reason, detail));
        } catch (RuntimeException e) {
            log.warn("partition.warning 발행 실패: {}", e.getMessage());
        }
    }

    static LocalDate parseDay(String partition) {
        try {
            String suffix = partition.substring(partition.lastIndexOf('_') + 1);
            return LocalDate.parse(suffix, DAY);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** 실행 결과 */
    public record Result(List<String> created, List<String> dropped, List<String> failed, int dlqPurged) {
    }
}
