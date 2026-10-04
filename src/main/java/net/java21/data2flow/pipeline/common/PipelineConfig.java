package net.java21.data2flow.pipeline.common;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import net.java21.data2flow.contracts.messaging.MessageTracing;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.pipeline.aggregate.repository.AggregateRepository;
import net.java21.data2flow.pipeline.aggregate.service.AggregationService;
import net.java21.data2flow.pipeline.connectivity.service.OfflineDetector;
import net.java21.data2flow.pipeline.decoder.service.DecoderRegistry;
import net.java21.data2flow.pipeline.device.service.AutoRegisterQuota;
import net.java21.data2flow.pipeline.device.service.CoreApiClient;
import net.java21.data2flow.pipeline.device.service.CoreDirectory;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.device.service.DeviceRuntimeCache;
import net.java21.data2flow.pipeline.device.service.SourceContextCache;
import net.java21.data2flow.pipeline.ingest.repository.DlqItemRepository;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRepository;
import net.java21.data2flow.pipeline.ingest.service.DedupGuard;
import net.java21.data2flow.pipeline.ingest.service.FailureService;
import net.java21.data2flow.pipeline.ingest.service.GatewayToucher;
import net.java21.data2flow.pipeline.ingest.service.IngestMetrics;
import net.java21.data2flow.pipeline.ingest.service.IngestProcessor;
import net.java21.data2flow.pipeline.ingest.service.IngestStore;
import net.java21.data2flow.pipeline.ingest.service.LagMonitor;
import net.java21.data2flow.pipeline.ingest.service.RawStreamConsumer;
import net.java21.data2flow.pipeline.messaging.ConfigChangeListener;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import net.java21.data2flow.pipeline.messaging.StreamConnection;
import net.java21.data2flow.pipeline.messaging.TelemetryPublisher;
import net.java21.data2flow.pipeline.messaging.UnreadableRawSink;
import net.java21.data2flow.pipeline.metric.service.MetricCatalogService;
import net.java21.data2flow.pipeline.partition.repository.PartitionRepository;
import net.java21.data2flow.pipeline.partition.service.PartitionMaintenanceService;
import net.java21.data2flow.pipeline.script.service.ScriptOutputValidator;
import net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry;
import net.java21.data2flow.script.sandbox.ScriptSandbox;
import net.java21.data2flow.pipeline.script.service.ScriptStaticChecker;
import net.java21.data2flow.pipeline.telemetry.repository.DataGapRepository;
import net.java21.data2flow.pipeline.telemetry.repository.DeviceStateRepository;
import net.java21.data2flow.pipeline.telemetry.repository.TelemetryRepository;
import org.flywaydb.core.Flyway;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** pipeline 빈 구성: 시계, Flyway 실행 방식, 샌드박스, core 연동 캐시, 처리 단계, 스트림, AMQP 토폴로지, 스케줄 작업 서비스 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    /** 운영 코드는 이 시계만 쓴다(ArchUnit NO_SYSTEM_CLOCK). 테스트는 MutableClock으로 바꾼다 */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * ADR-030: staging과 prod가 DB 하나를 함께 쓰므로 migrate는 staging 배포(Flyway PreSync)와 테스트에서만 한다.
     * 그 밖(prod·local)은 validate만 해서, 배포되지 않은 마이그레이션이 공용 DB에 들어가지 않게 한다.
     */
    @Bean
    FlywayMigrationStrategy flywayMigrationStrategy(PipelineProperties properties) {
        return (Flyway flyway) -> {
            if ("migrate".equalsIgnoreCase(properties.flywayMode())) {
                flyway.migrate();
            } else {
                flyway.validate();
            }
        };
    }

    @Bean
    MessageTracing messageTracing(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        Tracer t = tracer.getIfAvailable();
        Propagator p = propagator.getIfAvailable();
        return t == null || p == null ? MessageTracing.noop() : new MessageTracing(t, p);
    }

    // ---- 스크립트(SCR) ----

    @Bean(destroyMethod = "close")
    ScriptSandbox scriptSandbox(PipelineProperties properties) {
        // ctx.runtime: 호스트가 넘기는 창 데이터·기준 시각(ScriptLinker 감싸개가 ctx.window로 바꾸고 사용자에게는 숨긴다)
        ScriptSandbox sandbox = new ScriptSandbox(properties.script().toLimits(),
                java.util.List.of(net.java21.data2flow.pipeline.script.service.ScriptRunner.RUNTIME_KEY));
        // 데워질 때까지 예열한다(빈 생성이 끝나야 readiness가 열린다). 목표에 못 미치면 시간 초과 오판 위험을 경고로 남긴다
        ScriptSandbox.WarmUpResult warm = sandbox.warmUp(properties.script().toWarmUpPolicy());
        org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ScriptSandbox.class);
        if (warm.reachedTarget()) {
            log.info("스크립트 샌드박스 예열 완료: {}회, {}ms, 대표 스크립트 CPU {}ms", warm.rounds(), warm.elapsedMs(),
                    warm.lastCpuMs());
        } else {
            log.warn("스크립트 샌드박스 예열이 목표({})에 못 미쳤습니다: {}회, {}ms, 대표 스크립트 CPU {}ms. CPU가 부족하면 정상 스크립트도 "
                    + "시간 초과로 보일 수 있습니다", properties.script().warmUpTarget(), warm.rounds(), warm.elapsedMs(),
                    warm.lastCpuMs());
        }
        return sandbox;
    }

    @Bean
    ScriptOutputValidator scriptOutputValidator(PipelineProperties properties) {
        return new ScriptOutputValidator(properties.ingest().maxMetrics());
    }

    @Bean
    ScriptStaticChecker scriptStaticChecker(ScriptSandbox sandbox) {
        return new ScriptStaticChecker(sandbox);
    }

    @Bean
    ScriptRuntimeRegistry scriptRuntimeRegistry(CoreDirectory core, Clock clock, PipelineProperties properties,
                                                ObjectProvider<net.java21.data2flow.pipeline.script.service.ScriptRunner> runner,
                                                ObjectProvider<net.java21.data2flow.pipeline.formula.service.FormulaEngine> formulas) {
        ScriptRuntimeRegistry registry = new ScriptRuntimeRegistry(core, clock, properties.instanceId());
        registry.setWarmer((old, fresh) -> {
            var r = runner.getIfAvailable();
            var f = formulas.getIfAvailable();
            if (r != null && f != null) {
                new net.java21.data2flow.pipeline.script.service.ScriptWarmer(r, f, 3, clock).warm(old, fresh);
            }
        });
        return registry;
    }

    @Bean
    DecoderRegistry decoderRegistry(ScriptSandbox sandbox, ScriptOutputValidator validator, Clock clock,
                                    net.java21.data2flow.pipeline.script.service.ScriptRunner runner) {
        return new DecoderRegistry(sandbox, validator, clock, runner);
    }

    @Bean
    net.java21.data2flow.pipeline.script.service.ScriptOps scriptOps(
            net.java21.data2flow.pipeline.script.repository.ScriptOpsRepository repository, Clock clock) {
        return new net.java21.data2flow.pipeline.script.service.ScriptOps(repository, clock);
    }

    @Bean
    net.java21.data2flow.pipeline.script.service.ScriptRunner scriptRunner(ScriptSandbox sandbox,
                                                                         net.java21.data2flow.pipeline.script.service.ScriptOps ops) {
        return new net.java21.data2flow.pipeline.script.service.ScriptRunner(sandbox, ops);
    }

    @Bean
    net.java21.data2flow.pipeline.formula.service.FormulaEngine formulaEngine(
            net.java21.data2flow.pipeline.script.service.ScriptRunner runner) {
        return new net.java21.data2flow.pipeline.formula.service.FormulaEngine(runner);
    }

    @Bean
    net.java21.data2flow.pipeline.formula.service.FormulaPreviewService formulaPreviewService(
            net.java21.data2flow.pipeline.formula.service.FormulaEngine engine, MetricCatalogService catalogs,
            TelemetryRepository telemetry, Clock clock) {
        return new net.java21.data2flow.pipeline.formula.service.FormulaPreviewService(engine, catalogs, telemetry, clock);
    }

    @Bean
    net.java21.data2flow.pipeline.telemetry.service.RecentValues recentValues(TelemetryRepository telemetry) {
        return new net.java21.data2flow.pipeline.telemetry.service.RecentValues(telemetry::loadRecent);
    }

    // ---- core-api 기준 정보(캐시) ----

    @Bean
    CoreDirectory coreDirectory(PipelineProperties properties) {
        return new CoreApiClient(properties.core());
    }

    @Bean
    DeviceDirectory deviceDirectory(CoreDirectory core, PipelineProperties properties) {
        return new DeviceDirectory(core, properties.core());
    }

    @Bean
    SourceContextCache sourceContextCache(CoreDirectory core, PipelineProperties properties) {
        return new SourceContextCache(core, properties.core());
    }

    @Bean
    DeviceRuntimeCache deviceRuntimeCache(CoreDirectory core, PipelineProperties properties) {
        return new DeviceRuntimeCache(core, properties.core());
    }

    @Bean
    AutoRegisterQuota autoRegisterQuota(Clock clock) {
        return new AutoRegisterQuota(clock);
    }

    @Bean
    MetricCatalogService metricCatalogService(CoreDirectory core) {
        return new MetricCatalogService(core);
    }

    // ---- 처리 단계(ING) ----

    @Bean
    DedupGuard dedupGuard(PipelineProperties properties) {
        return new DedupGuard(properties.ingest().dedupWindow());
    }

    @Bean
    GatewayToucher gatewayToucher(CoreDirectory core) {
        return new GatewayToucher(core);
    }

    @Bean
    IngestMetrics ingestMetrics(MeterRegistry registry) {
        return new IngestMetrics(registry);
    }

    @Bean
    IngestStore ingestStore(RawMessageRepository raws, DlqItemRepository dlq, TelemetryRepository telemetry,
                            DeviceStateRepository states, DataGapRepository gaps, PipelineProperties properties,
                            net.java21.data2flow.pipeline.retention.service.RetentionPolicyCache policies) {
        return new IngestStore(raws, dlq, telemetry, states, gaps, properties, policies::storeMode);
    }

    @Bean
    IngestProcessor ingestProcessor(SourceContextCache sources, DeviceDirectory devices, DeviceRuntimeCache runtimes,
                                    AutoRegisterQuota quota, CoreDirectory core, MetricCatalogService catalogs,
                                    ScriptRuntimeRegistry scripts, DecoderRegistry decoders, ScriptSandbox sandbox,
                                    DedupGuard dedup, IngestStore store, RawMessageRepository raws,
                                    DeviceStateRepository states, TelemetryPublisher telemetry,
                                    DomainEventPublisher events, GatewayToucher gateways, IngestMetrics metrics,
                                    net.java21.data2flow.pipeline.quality.domain.ClockSkewDetector skew,
                                    net.java21.data2flow.pipeline.quality.domain.SuspectDetector suspects,
                                    net.java21.data2flow.pipeline.script.service.ScriptRunner runner,
                                    net.java21.data2flow.pipeline.formula.service.FormulaEngine formulas,
                                    net.java21.data2flow.pipeline.telemetry.service.RecentValues recent,
                                    PipelineProperties properties, Clock clock) {
        return new IngestProcessor(new IngestProcessor.Deps(sources, devices, runtimes, quota, core, catalogs, scripts,
                decoders, sandbox, dedup, store, raws, states, telemetry, events, gateways, metrics, skew, suspects, runner,
                formulas, recent),
                properties, clock);
    }

    @Bean
    net.java21.data2flow.pipeline.quality.domain.ClockSkewDetector clockSkewDetector(PipelineProperties properties) {
        PipelineProperties.Quality q = properties.quality();
        return new net.java21.data2flow.pipeline.quality.domain.ClockSkewDetector(q.skewThreshold(), q.skewWindow(),
                q.skewSustain(), q.skewClear());
    }

    @Bean
    net.java21.data2flow.pipeline.quality.domain.SuspectDetector suspectDetector(PipelineProperties properties) {
        return new net.java21.data2flow.pipeline.quality.domain.SuspectDetector(properties.quality().stuckCount(),
                properties.quality().jumpRangeFraction());
    }

    @Bean
    net.java21.data2flow.pipeline.quality.service.DataQualityService dataQualityService(
            net.java21.data2flow.pipeline.quality.repository.DataQualityRepository repository, DeviceDirectory devices,
            PipelineProperties properties, Clock clock) {
        return new net.java21.data2flow.pipeline.quality.service.DataQualityService(repository, devices,
                properties.aggregation().defaultZone(), clock);
    }

    @Bean
    FailureService failureService(DlqItemRepository dlq, RawMessageRepository raws, IngestProcessor processor, Clock clock) {
        return new FailureService(dlq, raws, processor, clock);
    }

    @Bean(destroyMethod = "close")
    net.java21.data2flow.pipeline.ingest.service.ReprocessJobService reprocessJobService(
            net.java21.data2flow.pipeline.ingest.repository.ReprocessJobRepository jobs, RawMessageRepository raws,
            IngestProcessor processor, SourceContextCache sources, ScriptRuntimeRegistry scripts, DomainEventPublisher events,
            ObjectProvider<LagMonitor> lag, PipelineProperties properties, Clock clock) {
        return new net.java21.data2flow.pipeline.ingest.service.ReprocessJobService(jobs, raws, processor, sources, scripts,
                events, properties, () -> {
                    LagMonitor monitor = lag.getIfAvailable();
                    return monitor == null ? 0 : monitor.lagSeconds();
                }, clock);
    }

    @Bean(destroyMethod = "close")
    net.java21.data2flow.pipeline.telemetry.service.MetricRemapService metricRemapService(TelemetryRepository telemetry,
                                                                                         Clock clock) {
        return new net.java21.data2flow.pipeline.telemetry.service.MetricRemapService(telemetry, clock);
    }

    // ---- RabbitMQ Stream ----

    @Bean(destroyMethod = "close")
    StreamConnection streamConnection(RabbitProperties rabbit, PipelineProperties properties) {
        return new StreamConnection(rabbit, properties.stream());
    }

    @Bean(destroyMethod = "close")
    TelemetryPublisher telemetryPublisher(StreamConnection connection, PipelineProperties properties,
                                          MessageTracing tracing) {
        return new TelemetryPublisher(connection, properties.stream(), tracing);
    }

    @Bean
    RawStreamConsumer rawStreamConsumer(StreamConnection connection, IngestProcessor processor, DedupGuard dedup,
                                        PipelineProperties properties, MessageTracing tracing, RabbitTemplate rabbit,
                                        Clock clock) {
        return new RawStreamConsumer(connection, processor, dedup, properties, tracing,
                new UnreadableRawSink(rabbit), clock) {
            @Override
            public boolean isAutoStartup() {
                return properties.consumerEnabled();
            }
        };
    }

    /** readiness: 소비자가 열렸는지(꺼 둔 환경은 항상 UP) */
    @Bean("pipelineStream")
    HealthIndicator pipelineStreamHealth(RawStreamConsumer consumer, PipelineProperties properties) {
        return () -> {
            if (!properties.consumerEnabled() || consumer.isConsuming()) {
                return Health.up().withDetail("group", consumer.groupName()).build();
            }
            return Health.down().withDetail("error", String.valueOf(consumer.lastError())).build();
        };
    }

    // ---- RabbitMQ AMQP: 도메인 이벤트·설정 변경·DLX ----

    @Bean
    TopicExchange eventsExchange() {
        return new TopicExchange(MessagingNames.EXCHANGE_EVENTS, true, false);
    }

    @Bean
    FanoutExchange configExchange() {
        return new FanoutExchange(MessagingNames.EXCHANGE_CONFIG, true, false);
    }

    @Bean
    DirectExchange deadLetterExchange() {
        return new DirectExchange(MessagingNames.EXCHANGE_DLX, true, false);
    }

    /** 읽을 수 없는 data2flow.raw 메시지 보관 큐(reliability-and-ha.md §2 "공통") */
    @Bean
    Queue rawDeadLetterQueue() {
        return QueueBuilder.durable(MessagingNames.deadLetterQueue(UnreadableRawSink.QUEUE)).quorum().build();
    }

    @Bean
    Binding rawDeadLetterBinding(Queue rawDeadLetterQueue, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(rawDeadLetterQueue).to(deadLetterExchange).with(UnreadableRawSink.QUEUE);
    }

    @Bean
    DomainEventPublisher domainEventPublisher(RabbitTemplate rabbit, Clock clock) {
        return new DomainEventPublisher(rabbit, clock);
    }

    @Bean
    AnonymousQueue pipelineConfigQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("pipeline.config."));
    }

    @Bean
    Binding pipelineConfigBinding(AnonymousQueue pipelineConfigQueue, FanoutExchange configExchange) {
        return BindingBuilder.bind(pipelineConfigQueue).to(configExchange);
    }

    @Bean
    ConfigChangeListener configChangeListener(DeviceDirectory devices, DeviceRuntimeCache runtimes,
                                              SourceContextCache sources, AutoRegisterQuota quota,
                                              MetricCatalogService catalogs, ScriptRuntimeRegistry scripts) {
        return new ConfigChangeListener(devices, runtimes, sources, quota, catalogs, scripts);
    }

    @Bean
    SimpleMessageListenerContainer configChangeContainer(ConnectionFactory connectionFactory,
                                                         AnonymousQueue pipelineConfigQueue,
                                                         ConfigChangeListener listener) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueues(pipelineConfigQueue);
        container.setMessageListener(listener);
        container.setMissingQueuesFatal(false);
        return container;
    }

    @Bean
    net.java21.data2flow.pipeline.messaging.ConfigReconnectHandler configReconnectHandler(ConfigChangeListener listener) {
        return new net.java21.data2flow.pipeline.messaging.ConfigReconnectHandler(listener);
    }

    // ---- 스케줄 작업이 부르는 서비스 ----

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }

    @Bean
    AggregationService aggregationService(AggregateRepository repository, TransactionTemplate tx,
                                          DomainEventPublisher events, PipelineProperties properties,
                                          MetricCatalogService catalogs, Clock clock) {
        return new AggregationService(repository, tx, events, properties.aggregation(),
                () -> new String[]{"door", "motion", "occupancy", "magnet_status", "leak", "switch"}, clock);
    }

    @Bean
    PartitionMaintenanceService partitionMaintenanceService(PartitionRepository partitions, DlqItemRepository dlq,
                                                            TransactionTemplate tx, DomainEventPublisher events,
                                                            PipelineProperties properties,
                                                            net.java21.data2flow.pipeline.retention.service.RetentionPolicyCache policies,
                                                            Clock clock) {
        // 원본 메시지 일 파티션은 모든 조직 중 가장 긴 보관 기간이 지나야 지운다(짧은 조직은 보관 정리가 행 단위로)
        return new PartitionMaintenanceService(partitions, dlq, tx, events, properties,
                () -> policies.maxDays("RAW_MESSAGE"), clock);
    }

    // ---- 보관·콜드 보관(TSD-02·05) ----

    @Bean
    net.java21.data2flow.pipeline.retention.service.RetentionPolicyCache retentionPolicyCache(CoreDirectory core,
                                                                                            PipelineProperties properties,
                                                                                            Clock clock) {
        return new net.java21.data2flow.pipeline.retention.service.RetentionPolicyCache(core, properties.retention(), clock);
    }

    @Bean
    net.java21.data2flow.pipeline.retention.service.RetentionService retentionService(
            net.java21.data2flow.pipeline.retention.repository.RetentionRepository repository, PartitionRepository partitions,
            net.java21.data2flow.pipeline.retention.service.RetentionPolicyCache policies,
            ObjectProvider<net.java21.data2flow.pipeline.retention.service.ObjectStore> store, CoreDirectory core,
            DeviceDirectory devices, DomainEventPublisher events,
            net.java21.data2flow.pipeline.quality.repository.DataQualityRepository quality,
            net.java21.data2flow.pipeline.script.repository.ScriptOpsRepository scriptOps, TransactionTemplate tx,
            PipelineProperties properties, Clock clock) {
        net.java21.data2flow.pipeline.retention.service.ObjectStore objectStore = store.getIfAvailable();
        net.java21.data2flow.pipeline.retention.service.ArchiveService archive = objectStore == null ? null
                : new net.java21.data2flow.pipeline.retention.service.ArchiveService(objectStore, core, repository,
                properties.archive().prefix());
        return new net.java21.data2flow.pipeline.retention.service.RetentionService(repository, partitions, policies, archive,
                devices, events, quality, scriptOps, tx, properties, clock);
    }

    /** 콜드 보관 저장소: 주소를 정한 환경에서만(없으면 콜드 보관 없이 보관 기간대로 지움) */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnExpression("'${data2flow.pipeline.archive.endpoint:}' != ''")
    net.java21.data2flow.pipeline.retention.service.ObjectStore objectStore(PipelineProperties properties, Clock clock) {
        return new net.java21.data2flow.pipeline.retention.service.S3ObjectStore(properties.archive(), clock);
    }

    @Bean
    OfflineDetector offlineDetector(DeviceStateRepository states, DeviceDirectory devices, DomainEventPublisher events,
                                    TransactionTemplate tx, PipelineProperties properties, Clock clock) {
        return new OfflineDetector(states, devices, events, tx, properties.offline(), clock);
    }

    @Bean
    LagMonitor lagMonitor(RawStreamConsumer consumer, StreamConnection connection, DomainEventPublisher events,
                          PipelineProperties properties, Clock clock, MeterRegistry registry) {
        LagMonitor monitor = new LagMonitor(() -> partitionLags(consumer, connection), events, properties.lag(), clock);
        Gauge.builder("data2flow.ingest.lag.seconds", monitor, LagMonitor::lagSeconds)
                .description("data2flow.raw 처리 지연(가장 큰 파티션, ING-07.04)").register(registry);
        return monitor;
    }

    private static List<LagMonitor.PartitionLag> partitionLags(RawStreamConsumer consumer, StreamConnection connection) {
        List<LagMonitor.PartitionLag> lags = new ArrayList<>();
        if (!consumer.isConsuming()) {
            return lags;
        }
        for (Map.Entry<Integer, RawStreamConsumer.PartitionProgress> e : consumer.progress().entrySet()) {
            boolean backlog;
            try {
                long committed = connection.environment()
                        .queryStreamStats(MessagingNames.STREAM_RAW + "-" + e.getKey()).committedChunkId();
                backlog = committed > e.getValue().offset();
            } catch (RuntimeException ex) {
                backlog = false;
            }
            lags.add(new LagMonitor.PartitionLag(e.getKey(), backlog, e.getValue().lastReceivedAt(),
                    e.getValue().organizationId()));
        }
        return lags;
    }

}
