package net.java21.data2flow.pipeline.retention.service;

import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.RetentionPurged;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import net.java21.data2flow.pipeline.partition.repository.PartitionRepository;
import net.java21.data2flow.pipeline.quality.repository.DataQualityRepository;
import net.java21.data2flow.pipeline.retention.domain.RetentionPolicies;
import net.java21.data2flow.pipeline.retention.repository.RetentionRepository;
import net.java21.data2flow.pipeline.script.repository.ScriptOpsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 야간 보관 정리(TSD-02.01·02.03·05.01·05.02, NFR-04.03, BR-TSD-02·03·07·18). 매일 02:00 UTC 한 인스턴스(ShedLock)가 돈다.
 *
 * <ol>
 *   <li><b>원본 텔레메트리(TELEMETRY):</b> 조직마다 보관 기간(core 정책, 없으면 365일)이 지난 행을 1만 행씩 지운다. 측정 항목·모델별
 *       보관 기간이 더 짧으면 그 행을 먼저 지우고, 더 길면 조직 삭제에서 빼 둔다. 콜드 보관을 켠 조직은 기간이 다 지난 달을 Parquet으로
 *       올리고 체크섬 확인·core 등록이 끝난 달까지만 지운다(실패하면 다음 밤에 다시, 원본은 남김).</li>
 *   <li><b>파티션:</b> 모든 조직의 기간이 지난 월 파티션은 남은 연장 보관 행을 {@code telemetry_long}으로 옮기고 비었을 때만
 *       DETACH(lock_timeout 5초) 후 DROP한다. 집계·통신 품질도 같은 방식(연장 보관 없음, 가장 긴 기간 기준)이다.</li>
 *   <li><b>정렬 재작성:</b> 7일이 지난 원본 파티션을 한 번에 하나씩 (device_id, time) 순서로 다시 쓴다(BR-TSD-07).</li>
 *   <li>원본 메시지는 조직 기간이 기본보다 짧으면 행 단위로 지운다(일 파티션은 파티션 관리가 가장 긴 기간으로 지움). 품질 점수 1년,
 *       스크립트 지표 7일(→ 1시간 표 90일)·로그 7일.</li>
 * </ol>
 * 지울 때마다 {@code retention.purged}(EVT-TSD-04)를 조직별로 낸다.
 */
public class RetentionService {

    private static final Logger log = LoggerFactory.getLogger(RetentionService.class);
    static final String TELEMETRY = "TELEMETRY";

    private final RetentionRepository repository;
    private final PartitionRepository partitions;
    private final RetentionPolicyCache policies;
    private final ArchiveService archive;
    private final DeviceDirectory devices;
    private final DomainEventPublisher events;
    private final DataQualityRepository quality;
    private final ScriptOpsRepository scriptOps;
    private final TransactionTemplate tx;
    private final PipelineProperties properties;
    private final Clock clock;

    public RetentionService(RetentionRepository repository, PartitionRepository partitions, RetentionPolicyCache policies,
                            ArchiveService archive, DeviceDirectory devices, DomainEventPublisher events,
                            DataQualityRepository quality, ScriptOpsRepository scriptOps, TransactionTemplate tx,
                            PipelineProperties properties, Clock clock) {
        this.repository = repository;
        this.partitions = partitions;
        this.policies = policies;
        this.archive = archive;
        this.devices = devices;
        this.events = events;
        this.quality = quality;
        this.scriptOps = scriptOps;
        this.tx = tx;
        this.properties = properties;
        this.clock = clock;
    }

    /** 한 번 실행(야간 작업) */
    public Report run() {
        policies.refresh();
        Instant now = clock.instant();
        Report report = new Report();
        Set<Long> orgs = organizations();
        for (Long org : orgs) {
            safely(report, "TELEMETRY org " + org, () -> purgeTelemetry(org, now, report));
            for (Target t : Target.SIMPLE) {
                safely(report, t.dataClass() + " org " + org, () -> purgeRows(t, org, now, report));
            }
            safely(report, "RAW_MESSAGE org " + org, () -> purgeRaw(org, now, report));
        }
        safely(report, "TELEMETRY partitions", () -> dropTelemetryPartitions(now, orgs, report));
        for (Target t : Target.SIMPLE) {
            if (t.partitioned()) {
                safely(report, t.dataClass() + " partitions", () -> dropPartitions(t, now, report));
            }
        }
        safely(report, "sort rewrite", () -> compressOne(now, report));
        safely(report, "quality", () -> quality.deleteBefore(now.atZone(ZoneOffset.UTC).toLocalDate()
                .minusDays(properties.retention().qualityDays())));
        safely(report, "script ops", () -> scriptOps.rollupAndPurge(now.minus(Duration.ofDays(properties.retention()
                .scriptStats1mDays())), now.minus(Duration.ofDays(properties.retention().scriptStats1hDays())),
                now.minus(Duration.ofDays(properties.retention().scriptLogDays()))));
        log.info("보관 정리: 지움 {}행, 콜드 보관 {}개, 파티션 삭제 {}, 정렬 재작성 {}, 실패 {}", report.deletedRows,
                report.archived.size(), report.droppedPartitions, report.sorted, report.failures);
        return report;
    }

    private Set<Long> organizations() {
        Set<Long> orgs = new LinkedHashSet<>(repository.listKnownOrganizations());
        policies.known().forEach(p -> orgs.add(p.organizationId()));
        return orgs;
    }

    // ---- TELEMETRY ----

    private void purgeTelemetry(long org, Instant now, Report report) {
        RetentionPolicies p = policies.of(org);
        int days = policies.days(org, TELEMETRY);
        if (days <= 0) {
            return;
        }
        Instant cutoff = now.minus(Duration.ofDays(days));
        List<ScopeOverride> overrides = overrides(p, TELEMETRY);
        // 더 짧은 재정의: 그 행을 먼저 지운다(METRIC > MODEL > ORG)
        for (ScopeOverride o : overrides) {
            if (o.days() > 0 && o.days() < days) {
                long n = repository.deleteBefore("telemetry", "time", org, now.minus(Duration.ofDays(o.days())), o.sql(),
                        o.args(), properties.retention().deleteBatch());
                purged(org, TELEMETRY, Instant.EPOCH, now.minus(Duration.ofDays(o.days())), n, false, report);
            }
        }
        Exempt exempt = exempt(overrides, days);
        Instant deleteUntil = cutoff;
        if (p.archive(TELEMETRY) && archive != null) {
            deleteUntil = archiveMonths(org, cutoff, exempt, report);
        }
        if (deleteUntil.isAfter(Instant.EPOCH)) {
            long n = repository.deleteBefore("telemetry", "time", org, deleteUntil, exempt.notSql(), exempt.args(),
                    properties.retention().deleteBatch());
            purged(org, TELEMETRY, Instant.EPOCH, deleteUntil, n, p.archive(TELEMETRY) && archive != null, report);
        }
        // 연장 보관 표: 재정의 기간(없어진 재정의는 조직 기간)으로
        for (ScopeOverride o : overrides) {
            if (o.days() > days) {
                long n = repository.deleteBefore("telemetry_long", "time", org, now.minus(Duration.ofDays(o.days())), o.sql(),
                        o.args(), properties.retention().deleteBatch());
                purged(org, TELEMETRY, Instant.EPOCH, now.minus(Duration.ofDays(o.days())), n, false, report);
            }
        }
        long n = repository.deleteBefore("telemetry_long", "time", org, cutoff, exempt.notSql(), exempt.args(),
                properties.retention().deleteBatch());
        purged(org, TELEMETRY, Instant.EPOCH, cutoff, n, false, report);
    }

    /** 기간이 다 지난 달을 오래된 것부터 콜드 보관한다. 지워도 되는 끝 시각(보관을 마친 마지막 달의 끝) */
    private Instant archiveMonths(long org, Instant cutoff, Exempt exempt, Report report) {
        Instant safe = Instant.EPOCH;
        for (RetentionRepository.PartitionRange month : repository.listPartitionRanges("telemetry")) {
            if (month.to().isAfter(cutoff)) {
                break;
            }
            try {
                archive.archiveTelemetry(month.name(), org, month.from(), month.to(), exempt.sql(), exempt.args())
                        .ifPresent(a -> {
                            if (!a.already()) {
                                report.archived.add(a.objectKey());
                                repository.updateRegistryArchive(month.name(), "CREATED", a.bytes(), a.ratio(), clock.instant());
                            }
                        });
                safe = month.to();
            } catch (RuntimeException e) {
                log.warn("콜드 보관 실패(org={}, {}): {} — 원본을 지우지 않고 다음 밤에 다시 합니다", org, month.name(),
                        e.getMessage());
                report.failures++;
                break;
            }
        }
        return safe;
    }

    /** 모든 조직의 기간이 지난 원본 월 파티션: 연장 보관 행을 옮기고 비었으면 지운다 */
    private void dropTelemetryPartitions(Instant now, Set<Long> orgs, Report report) {
        int maxDays = policies.maxDays(TELEMETRY);
        if (maxDays <= 0) {
            return;
        }
        Instant before = now.minus(Duration.ofDays(maxDays));
        for (RetentionRepository.PartitionRange part : repository.listPartitionRanges("telemetry")) {
            if (part.to().isAfter(before)) {
                break;
            }
            Map<Long, Long> counts = repository.countByOrganization(part.name());
            tx.executeWithoutResult(s -> {
                for (Long org : counts.keySet()) {
                    List<ScopeOverride> longer = overrides(policies.of(org), TELEMETRY).stream()
                            .filter(o -> o.days() == 0 || o.days() > policies.days(org, TELEMETRY)).toList();
                    for (ScopeOverride o : longer) {
                        repository.insertIntoLong(part.name(), org, o.sql(), o.args());
                        repository.deleteMatching(part.name(), org, o.sql(), o.args());
                    }
                }
            });
            long left = repository.countRows(part.name());
            if (left > 0) {
                log.warn("원본 파티션 {}에 아직 지우면 안 되는 행 {}개가 있어 남겨 둡니다(콜드 보관 대기 등)", part.name(), left);
                continue;
            }
            drop("telemetry", part, now, report);
        }
    }

    // ---- 집계·통신 품질 ----

    private void purgeRows(Target t, long org, Instant now, Report report) {
        int days = policies.days(org, t.dataClass());
        if (days <= 0) {
            return;
        }
        List<ScopeOverride> overrides = t.overrides() ? overrides(policies.of(org), t.dataClass()) : List.of();
        for (ScopeOverride o : overrides) {
            if (o.days() > 0 && o.days() < days) {
                long n = repository.deleteBefore(t.table(), t.timeColumn(), org, now.minus(Duration.ofDays(o.days())), o.sql(),
                        o.args(), properties.retention().deleteBatch());
                purged(org, t.dataClass(), Instant.EPOCH, now.minus(Duration.ofDays(o.days())), n, false, report);
            }
        }
        Exempt exempt = exempt(overrides, days);
        Instant cutoff = now.minus(Duration.ofDays(days));
        long n = repository.deleteBefore(t.table(), t.timeColumn(), org, cutoff, exempt.notSql(), exempt.args(),
                properties.retention().deleteBatch());
        purged(org, t.dataClass(), Instant.EPOCH, cutoff, n, false, report);
    }

    private void dropPartitions(Target t, Instant now, Report report) {
        int maxDays = policies.maxDays(t.dataClass());
        for (RetentionPolicies p : policies.known()) {
            for (ScopeOverride o : overrides(p, t.dataClass())) {
                if (o.days() == 0) {
                    return;
                }
                maxDays = Math.max(maxDays, o.days());
            }
        }
        if (maxDays <= 0) {
            return;
        }
        Instant before = now.minus(Duration.ofDays(maxDays));
        for (RetentionRepository.PartitionRange part : repository.listPartitionRanges(t.table())) {
            if (part.to().isAfter(before)) {
                break;
            }
            drop(t.table(), part, now, report);
        }
    }

    private void drop(String parent, RetentionRepository.PartitionRange part, Instant now, Report report) {
        Map<Long, Long> counts = repository.countByOrganization(part.name());
        tx.executeWithoutResult(s -> {
            partitions.setLockTimeout(properties.partition().lockTimeout().toMillis());
            partitions.detachAndDrop(parent, part.name());
            repository.markDroppedOrArchived(part.name(), now);
        });
        report.droppedPartitions.add(part.name());
        String dataClass = Target.dataClassOf(parent);
        counts.forEach((org, rows) -> purged(org, dataClass, part.from(), part.to(), rows, false, report));
    }

    // ---- 원본 메시지 ----

    private void purgeRaw(long org, Instant now, Report report) {
        int days = policies.days(org, "RAW_MESSAGE");
        if (days <= 0 || days >= policies.maxDays("RAW_MESSAGE")) {
            return; // 가장 긴 기간은 파티션 관리가 일 파티션째로 지운다
        }
        Instant cutoff = now.minus(Duration.ofDays(days));
        long n = repository.deleteBefore("raw_messages", "received_at", org, cutoff, null, new Object[0],
                properties.retention().deleteBatch());
        purged(org, "RAW_MESSAGE", Instant.EPOCH, cutoff, n, false, report);
    }

    // ---- 정렬 재작성 ----

    /** 7일이 지난 원본 파티션 하나를 (device_id, time) 순서로 다시 쓴다(BR-TSD-07: 한 번에 한 파티션) */
    public String compressOne(Instant now, Report report) {
        Instant before = now.minus(Duration.ofDays(properties.retention().compressAfterDays()));
        List<String[]> candidates = repository.listUnsortedPartitions("telemetry", before);
        if (candidates.isEmpty()) {
            return null;
        }
        String[] p = candidates.getFirst();
        try {
            tx.executeWithoutResult(s -> repository.rewriteSorted("telemetry", p[0], Instant.parse(p[1]), Instant.parse(p[2]),
                    properties.partition().lockTimeout().toMillis(), now));
            report.sorted = p[0];
            return p[0];
        } catch (RuntimeException e) {
            log.warn("정렬 재작성 실패({}, 다음 밤에 다시): {}", p[0], e.getMessage());
            report.failures++;
            return null;
        }
    }

    // ---- 재정의 ----

    private List<ScopeOverride> overrides(RetentionPolicies p, String dataClass) {
        List<ScopeOverride> out = new ArrayList<>();
        for (RetentionPolicies.Item i : p.overrides(dataClass)) {
            if ("METRIC".equalsIgnoreCase(i.scope())) {
                out.add(new ScopeOverride("metric_key = ?", new Object[]{i.scopeRef()}, i.retainDays()));
            } else if ("MODEL".equalsIgnoreCase(i.scope())) {
                Long[] ids = devices.known().stream()
                        .filter(d -> d.organizationId() == p.organizationId())
                        .filter(d -> i.scopeRef().equals(d.modelCode())
                                || (d.modelId() != null && i.scopeRef().equals(Long.toString(d.modelId()))))
                        .map(DeviceInfo::deviceId).toArray(Long[]::new);
                if (ids.length > 0) {
                    out.add(new ScopeOverride("device_id = ANY(?)", new Object[]{ids}, i.retainDays()));
                }
            }
        }
        return out;
    }

    /** 조직 삭제에서 뺄 행(더 긴 재정의) */
    private static Exempt exempt(List<ScopeOverride> overrides, int orgDays) {
        List<String> parts = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        for (ScopeOverride o : overrides) {
            if (o.days() == 0 || o.days() > orgDays) {
                parts.add("(" + o.sql() + ")");
                args.addAll(List.of(o.args()));
            }
        }
        return new Exempt(parts.isEmpty() ? null : String.join(" OR ", parts), args.toArray());
    }

    private void purged(long org, String dataClass, Instant from, Instant to, long rows, boolean archived, Report report) {
        if (rows <= 0) {
            return;
        }
        report.deletedRows += rows;
        try {
            events.publish(EventType.RETENTION_PURGED, org, new RetentionPurged(dataClass, from, to, rows, archived));
        } catch (RuntimeException e) {
            log.warn("retention.purged 발행 실패: {}", e.getMessage());
        }
    }

    private static void safely(Report report, String step, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            report.failures++;
            log.warn("보관 정리 단계 실패({}): {}", step, e.getMessage());
        }
    }

    /** 데이터 종류별 대상 표(원본 텔레메트리 제외) */
    record Target(String dataClass, String table, String timeColumn, boolean partitioned, boolean overrides) {
        static final List<Target> SIMPLE = List.of(
                new Target("LINK", "link_qualities", "time", true, false),
                new Target("AGG_1M", "telemetry_1m", "bucket", true, true),
                new Target("AGG_1H", "telemetry_1h", "bucket", true, true),
                new Target("AGG_1D", "telemetry_1d", "bucket", false, true));

        static String dataClassOf(String table) {
            if ("telemetry".equals(table)) {
                return TELEMETRY;
            }
            return SIMPLE.stream().filter(t -> t.table().equals(table)).map(Target::dataClass).findFirst().orElse(table);
        }
    }

    /** 범위 재정의(METRIC: 측정 키, MODEL: 모델의 기기들) */
    record ScopeOverride(String sql, Object[] args, int days) {
    }

    /** 조직 삭제에서 뺄 조건 */
    record Exempt(String sql, Object[] args) {
        String notSql() {
            return sql == null ? null : "NOT (" + sql + ")";
        }
    }

    /** 실행 결과 */
    public static final class Report {
        public long deletedRows;
        public final List<String> archived = new ArrayList<>();
        public final List<String> droppedPartitions = new ArrayList<>();
        public String sorted;
        public int failures;
    }
}
