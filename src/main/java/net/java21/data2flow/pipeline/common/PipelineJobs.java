package net.java21.data2flow.pipeline.common;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import net.java21.data2flow.pipeline.aggregate.service.AggregationService;
import net.java21.data2flow.pipeline.connectivity.service.OfflineDetector;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.ingest.service.GatewayToucher;
import net.java21.data2flow.pipeline.ingest.service.LagMonitor;
import net.java21.data2flow.pipeline.metric.service.MetricCatalogService;
import net.java21.data2flow.pipeline.partition.service.PartitionMaintenanceService;
import net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry;
import net.java21.data2flow.pipeline.telemetry.repository.DeviceStateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;

/**
 * 스케줄 작업(architecture.md §2 scheduler: 시계열 유지와 오프라인 판정은 pipeline의 ShedLock 작업).
 *
 * <ul>
 *   <li>공유 작업(ShedLock, 작업 이름당 한 인스턴스): 파티션 관리(매일 01:00 UTC + 시작 시), 1m 집계(매분), 1h(5분), 1d(매시 10분),
 *       오프라인 판정(매분), 수신 수 갱신(매시). {@code data2flow.pipeline.jobs-enabled=false}면 끈다(로컬: 공용 DB)</li>
 *   <li>인스턴스 작업(캐시 갱신, 잠금 없음): 기기 목록(5분), 측정 항목(1분), 스크립트 번들 폴링(30초, TC-SCR-054), 게이트웨이 수신 보고(1분),
 *       처리 지연 평가(5초)</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
public class PipelineJobs {

    @Bean
    LockProvider lockProvider(DataSource dataSource) {
        return new JdbcTemplateLockProvider(JdbcTemplateLockProvider.Configuration.builder()
                .withJdbcTemplate(new org.springframework.jdbc.core.JdbcTemplate(dataSource))
                .withTableName("data2flow_pipeline.scheduler_locks")
                .usingDbTime()
                .build());
    }

    /** 잠금이 필요한 공유 작업 */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "data2flow.pipeline.jobs-enabled", havingValue = "true", matchIfMissing = true)
    static class SharedJobs {

        private static final Logger log = LoggerFactory.getLogger(SharedJobs.class);

        private final PartitionMaintenanceService partitions;
        private final AggregationService aggregation;
        private final OfflineDetector offline;
        private final DeviceStateRepository states;
        private final Clock clock;

        SharedJobs(PartitionMaintenanceService partitions, AggregationService aggregation, OfflineDetector offline,
                   DeviceStateRepository states, Clock clock) {
            this.partitions = partitions;
            this.aggregation = aggregation;
            this.offline = offline;
            this.states = states;
            this.clock = clock;
        }

        /** 시작할 때도 이번 달·오늘 파티션이 있는지 확인한다(없으면 DEFAULT로 들어가므로) */
        @EventListener(ApplicationReadyEvent.class)
        @SchedulerLock(name = "pipeline-partition-maintenance", lockAtMostFor = "30m")
        public void onReady() {
            maintainPartitions();
        }

        @Scheduled(cron = "0 0 1 * * *", zone = "UTC")
        @SchedulerLock(name = "pipeline-partition-maintenance", lockAtMostFor = "30m")
        public void maintainPartitions() {
            try {
                partitions.maintain();
            } catch (RuntimeException e) {
                log.error("파티션 관리 실패: {}", e.getMessage(), e);
            }
        }

        @Scheduled(fixedDelay = 60_000, initialDelay = 30_000)
        @SchedulerLock(name = "pipeline-aggregate-1m", lockAtMostFor = "10m")
        public void aggregateMinutes() {
            run("1m 집계", aggregation::aggregateMinutes);
        }

        @Scheduled(cron = "0 */5 * * * *", zone = "UTC")
        @SchedulerLock(name = "pipeline-aggregate-1h", lockAtMostFor = "20m")
        public void aggregateHours() {
            run("1h 집계", aggregation::aggregateHours);
        }

        @Scheduled(cron = "0 10 * * * *", zone = "UTC")
        @SchedulerLock(name = "pipeline-aggregate-1d", lockAtMostFor = "50m")
        public void aggregateDays() {
            run("1d 집계", aggregation::aggregateDays);
        }

        @Scheduled(fixedDelay = 60_000, initialDelay = 60_000)
        @SchedulerLock(name = "pipeline-offline-detect", lockAtMostFor = "5m")
        public void detectOffline() {
            run("오프라인 판정", offline::detect);
        }

        @Scheduled(cron = "0 0 * * * *", zone = "UTC")
        @SchedulerLock(name = "pipeline-message-counts", lockAtMostFor = "30m")
        public void countMessages() {
            run("24시간 수신 수", () -> states.updateMessageCounts(clock.instant().minus(Duration.ofHours(24))));
        }

        private static void run(String name, Runnable job) {
            try {
                job.run();
            } catch (RuntimeException e) {
                log.warn("{} 실패(다음 주기에 다시): {}", name, e.getMessage());
            }
        }
    }

    /** 인스턴스마다 도는 캐시 갱신 */
    @Configuration(proxyBeanMethods = false)
    static class InstanceJobs {

        private final DeviceDirectory devices;
        private final MetricCatalogService catalogs;
        private final ScriptRuntimeRegistry scripts;
        private final GatewayToucher gateways;
        private final LagMonitor lag;
        private final Clock clock;

        InstanceJobs(DeviceDirectory devices, MetricCatalogService catalogs, ScriptRuntimeRegistry scripts,
                     GatewayToucher gateways, LagMonitor lag, Clock clock) {
            this.devices = devices;
            this.catalogs = catalogs;
            this.scripts = scripts;
            this.gateways = gateways;
            this.lag = lag;
            this.clock = clock;
        }

        @Scheduled(fixedDelay = 300_000, initialDelay = 5_000)
        public void warmDevices() {
            devices.warm(clock.instant());
        }

        @Scheduled(fixedDelay = 60_000, initialDelay = 60_000)
        public void refreshCatalogs() {
            catalogs.refreshAll();
        }

        @Scheduled(fixedDelayString = "${data2flow.pipeline.core.bundle-poll-interval:30s}", initialDelay = 30_000)
        public void pollScriptBundles() {
            scripts.refreshAll();
        }

        @Scheduled(fixedDelay = 60_000, initialDelay = 60_000)
        public void flushGateways() {
            gateways.flush();
        }

        @Scheduled(fixedDelay = 5_000, initialDelay = 10_000)
        public void evaluateLag() {
            lag.evaluate();
        }
    }
}
