package net.java21.data2flow.pipeline.support;

import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.pipeline.device.service.AutoRegisterQuota;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.device.service.DeviceRuntimeCache;
import net.java21.data2flow.pipeline.device.service.SourceContextCache;
import net.java21.data2flow.pipeline.ingest.service.DedupGuard;
import net.java21.data2flow.pipeline.messaging.ConfigChangeListener;
import net.java21.data2flow.pipeline.partition.service.PartitionMaintenanceService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 통합 테스트 기반(design/testing/backend.md §3 "서비스 전체 *IT"): 실제 PostgreSQL 18·RabbitMQ 3.13 Stream + core-api 대역으로
 * 서비스 전체를 띄운다. 테스트마다 pipeline 테이블을 비우고, 시계를 T0로 되돌리고, 캐시를 지운다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "management.server.port=0")
@ActiveProfiles("test")
@Import(TestBeans.class)
public abstract class IntegrationTestSupport {

    protected static final CoreApiStub CORE = TestInfrastructure.CORE;
    protected static final MessageCodec CODEC = MessageCodec.create();
    private static TestStreams streams;
    private static boolean partitionsReady;

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", TestInfrastructure.POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", TestInfrastructure.POSTGRES::getUsername);
        registry.add("spring.datasource.password", TestInfrastructure.POSTGRES::getPassword);
        registry.add("spring.rabbitmq.host", TestInfrastructure::rabbitHost);
        registry.add("spring.rabbitmq.port", TestInfrastructure::amqpPort);
        registry.add("spring.rabbitmq.username", () -> "guest");
        registry.add("spring.rabbitmq.password", () -> "guest");
        registry.add("data2flow.pipeline.stream.port", TestInfrastructure::streamPort);
        registry.add("data2flow.pipeline.core.base-url", CORE::baseUrl);
    }

    @Autowired
    protected MutableClock clock;
    @Autowired
    protected JdbcClient jdbc;
    @Autowired
    protected RabbitTemplate rabbit;
    @Autowired
    protected RabbitAdmin rabbitAdmin;
    @Autowired
    private DeviceDirectory devices;
    @Autowired
    private DeviceRuntimeCache runtimes;
    @Autowired
    private SourceContextCache sources;
    @Autowired
    private AutoRegisterQuota quota;
    @Autowired
    private DedupGuard dedup;
    @Autowired
    private ConfigChangeListener configListener;
    @Autowired
    private PartitionMaintenanceService partitions;
    @LocalServerPort
    protected int port;

    protected TestStreams.TelemetryCollector telemetry;
    protected String eventsQueue;

    @Autowired
    private net.java21.data2flow.pipeline.ingest.service.RawStreamConsumer consumer;

    @BeforeEach
    void resetState() {
        awaitIdle();
        clock.set(MutableClock.T0);
        CORE.reset();
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(30)).ignoreExceptions().until(() -> {
            truncate();
            return true;
        });
        if (!partitionsReady) {
            partitions.maintain();
            partitionsReady = true;
        }
        configListener.invalidateAll();
        for (int p = 0; p < 12; p++) {
            dedup.clear(p);
        }
        quota.reset(3);
        if (streams == null) {
            streams = new TestStreams("/", 3);
        }
        telemetry = streams.collectTelemetry();
        org.springframework.amqp.core.Queue queue = new org.springframework.amqp.core.Queue(
                "test.events." + UUID.randomUUID(), false, false, false);
        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(new TopicExchange(MessagingNames.EXCHANGE_EVENTS, true, false))
                .with("#"));
        eventsQueue = queue.getName();
    }

    private void truncate() {
        jdbc.sql("""
                TRUNCATE data2flow_pipeline.raw_messages, data2flow_pipeline.dlq_items, data2flow_pipeline.telemetry,
                    data2flow_pipeline.telemetry_1m, data2flow_pipeline.telemetry_1h, data2flow_pipeline.telemetry_1d,
                    data2flow_pipeline.link_qualities, data2flow_pipeline.agg_watermarks, data2flow_pipeline.agg_dirty_ranges,
                    data2flow_pipeline.device_state, data2flow_pipeline.data_gaps""").update();
    }

    /** 앞 시험의 메시지 처리가 끝날 때까지(처리 중 0이 0.3초 이어짐) */
    protected void awaitIdle() {
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(60))
                .during(java.time.Duration.ofMillis(300)).until(() -> consumer.inFlight() == 0);
    }

    @AfterEach
    void closeCollectors() {
        receivedEvents.clear();
        if (telemetry == null) {
            return;
        }
        telemetry.close();
        rabbitAdmin.deleteQueue(eventsQueue);
    }

    protected static TestStreams streams() {
        return streams;
    }

    /** ingress가 만드는 것과 같은 원본 봉투 */
    protected RawEnvelope envelope(long sourceId, String sourceType, String topic, byte[] payload, Instant receivedAt) {
        return new RawEnvelope(RawEnvelope.VERSION, UUID.randomUUID(), 1, sourceId, sourceType, topic, payload, receivedAt,
                "data2flow-ingress-0", DedupKeys.detect(sourceId, topic, payload), false, null);
    }

    protected RawEnvelope envelope(long sourceId, String topic, String payload) {
        return envelope(sourceId, "MQTT_SUBSCRIBE", topic, payload.getBytes(StandardCharsets.UTF_8), clock.instant());
    }

    protected void publish(RawEnvelope envelope) {
        streams.publish(envelope);
    }

    /** 지금까지 받은 도메인 이벤트(라우팅 키, 본문) */
    protected List<JsonNode> events(String routingKey) {
        List<JsonNode> list = new ArrayList<>();
        org.springframework.amqp.core.Message m;
        while ((m = rabbit.receive(eventsQueue, 200)) != null) {
            receivedEvents.add(CODEC.mapper().readTree(m.getBody()));
        }
        for (JsonNode e : receivedEvents) {
            if (routingKey == null || routingKey.equals(e.get("type").asString())) {
                list.add(e);
            }
        }
        return list;
    }

    private final List<JsonNode> receivedEvents = new ArrayList<>();

    protected String rawStatus(UUID messageId) {
        return jdbc.sql("SELECT status FROM data2flow_pipeline.raw_messages WHERE message_id = :id")
                .param("id", messageId).query(String.class).optional().orElse(null);
    }

    protected long count(String sql) {
        return jdbc.sql(sql).query(Long.class).single();
    }

    /** 원본 처리를 기다린다 */
    protected void awaitRaw(UUID messageId) {
        org.awaitility.Awaitility.await().atMost(TestStreams.timeout()).until(() -> rawStatus(messageId) != null);
    }
}
