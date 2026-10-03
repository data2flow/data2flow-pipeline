package net.java21.data2flow.pipeline.ingest.service;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageFormatException;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.ConsumerGroups;
import net.java21.data2flow.contracts.messaging.MessageTracing;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import net.java21.data2flow.pipeline.common.Backoff;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.common.TransientFailures;
import net.java21.data2flow.pipeline.messaging.StreamConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code data2flow.raw} Super Stream 소비자(ING-01.01·01.03, NFR-02.05, reliability-and-ha.md §2 ③·§4).
 *
 * <ul>
 *   <li>그룹 이름 {@code pipeline}(로컬 {@code pipeline-<개발자>}), Single Active Consumer: 파티션마다 활성 소비자 하나가 순서대로 처리.
 *       인스턴스가 여러 대면 브로커가 파티션을 나눠 주고, 한 대가 죽으면 대기 소비자가 넘겨받는다.</li>
 *   <li>오프셋은 <b>DB 커밋과 표준 메시지 발행 확인이 끝난 뒤에만</b> 저장한다(수동 추적). 넘겨받은 소비자는 저장된 오프셋 다음부터 읽는다.</li>
 *   <li>일시 장애(DB·core·발행)는 같은 메시지를 지수 백오프로 계속 다시 시도한다(소비를 멈추고 스트림에 보관, 유실 0).
 *       일시 장애가 아닌 오류가 정해진 횟수를 넘으면 STORE_ERROR로 원본과 함께 남기고 넘어간다.</li>
 *   <li>읽을 수 없는 봉투(형식 오류·모르는 버전)는 {@code data2flow.dlx} → {@code pipeline.raw.dlq}로 보내고 넘어간다.</li>
 *   <li>종료: 새 메시지를 받지 않고 처리 중인 것을 끝낸 뒤 소비자를 닫는다(브로커가 다른 인스턴스를 활성화).</li>
 * </ul>
 */
public class RawStreamConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RawStreamConsumer.class);

    private final StreamConnection connection;
    private final IngestProcessor processor;
    private final DedupGuard dedup;
    private final PipelineProperties properties;
    private final MessageTracing tracing;
    private final UnreadableSink unreadable;
    private final Clock clock;
    private final MessageCodec codec = MessageCodec.create();
    private final Map<Integer, PartitionProgress> progress = new ConcurrentHashMap<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final ScheduledExecutorService starter =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("raw-consumer-start").factory());
    private volatile Consumer consumer;
    private volatile boolean running;
    private volatile boolean stopping;
    private volatile String lastError;

    public RawStreamConsumer(StreamConnection connection, IngestProcessor processor, DedupGuard dedup,
                             PipelineProperties properties, MessageTracing tracing, UnreadableSink unreadable, Clock clock) {
        this.connection = connection;
        this.processor = processor;
        this.dedup = dedup;
        this.properties = properties;
        this.tracing = tracing;
        this.unreadable = unreadable;
        this.clock = clock;
    }

    /** 소비자 그룹 이름(로컬은 개발자 접미사, deployment.md §8.2) */
    public String groupName() {
        return ConsumerGroups.of(ConsumerGroups.PIPELINE, properties.developer());
    }

    @Override
    public void start() {
        running = true;
        stopping = false;
        starter.execute(this::connect);
    }

    private void connect() {
        Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(30));
        while (running && consumer == null) {
            try {
                connection.ensureSuperStream(SuperStreamSpec.RAW);
                consumer = connection.environment().consumerBuilder()
                        .superStream(MessagingNames.STREAM_RAW)
                        .name(groupName())
                        .singleActiveConsumer()
                        .manualTrackingStrategy().builder()
                        .consumerUpdateListener(context -> {
                            int partition = StreamConnection.partitionIndex(context.stream());
                            dedup.clear(partition);
                            if (!context.isActive()) {
                                log.info("파티션 {} 비활성(다른 인스턴스가 넘겨받음)", context.stream());
                                return null;
                            }
                            try {
                                long stored = context.consumer().storedOffset();
                                log.info("파티션 {} 활성: 저장된 오프셋 {} 다음부터", context.stream(), stored);
                                return OffsetSpecification.offset(stored + 1);
                            } catch (NoOffsetException e) {
                                log.info("파티션 {} 활성: 저장된 오프셋 없음, 처음부터", context.stream());
                                return OffsetSpecification.first();
                            }
                        })
                        .messageHandler(this::handle)
                        .build();
                lastError = null;
                log.info("data2flow.raw 소비 시작(그룹 {})", groupName());
            } catch (RuntimeException e) {
                lastError = e.getMessage();
                log.warn("data2flow.raw 소비자를 열지 못했습니다(다시 시도): {}", e.getMessage());
                if (!backoff.pause(() -> !running)) {
                    return;
                }
            }
        }
    }

    private void handle(MessageHandler.Context context, Message message) {
        if (stopping) {
            return; // 오프셋을 저장하지 않으므로 넘겨받은 인스턴스가 다시 처리한다
        }
        inFlight.incrementAndGet();
        try {
            int partition = StreamConnection.partitionIndex(context.stream());
            RawEnvelope envelope;
            try {
                envelope = codec.read(message.getBodyAsBinary(), RawEnvelope.class);
            } catch (MessageFormatException e) {
                unreadable.send(message, context.stream(), context.offset(), e);
                context.storeOffset();
                return;
            }
            Map<String, Object> headers = message.getApplicationProperties() == null ? Map.of()
                    : new HashMap<>(message.getApplicationProperties());
            var span = tracing.startConsumerSpan(MessagingNames.STREAM_RAW, headers);
            try (var ignored = tracing.inScope(span)) {
                if (processWithRetry(envelope, partition, context.offset())) {
                    context.storeOffset();
                    progress.computeIfAbsent(partition, p -> new PartitionProgress())
                            .update(context.offset(), envelope.receivedAt(), envelope.organizationId(), clock.instant());
                }
            } finally {
                span.end();
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    /** @return 처리 결과를 기록했으면 true(오프셋 저장). 종료 중이라 그만두면 false */
    private boolean processWithRetry(RawEnvelope envelope, int partition, long offset) {
        Backoff backoff = new Backoff(properties.ingest().retryInitial(), properties.ingest().retryMax());
        int failures = 0;
        while (true) {
            try {
                processor.process(envelope, partition, offset);
                return true;
            } catch (RuntimeException e) {
                if (TransientFailures.isTransient(e)) {
                    lastError = e.getMessage();
                    log.warn("일시 장애로 원본 {}(파티션 {}, 오프셋 {})을 다시 처리합니다(대기 {}): {}", envelope.messageId(),
                            partition, offset, backoff.current(), e.getMessage());
                } else if (++failures >= properties.ingest().storeAttempts()) {
                    log.error("원본 {}을 {}번 처리하지 못해 STORE_ERROR로 남깁니다", envelope.messageId(), failures, e);
                    try {
                        processor.recordStoreError(envelope, partition, offset, e);
                        return true;
                    } catch (RuntimeException recordError) {
                        log.warn("STORE_ERROR 기록 실패(다시 시도): {}", recordError.getMessage());
                    }
                } else {
                    log.warn("원본 {} 처리 오류({}회): {}", envelope.messageId(), failures, e.toString());
                }
                if (!backoff.pause(() -> stopping || !running)) {
                    return false;
                }
            }
        }
    }

    @Override
    public void stop() {
        stopping = true;
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        Backoff wait = new Backoff(Duration.ofMillis(20), Duration.ofMillis(200));
        while (inFlight.get() > 0 && System.nanoTime() < deadline) {
            wait.pause(() -> inFlight.get() == 0);
        }
        Consumer c = consumer;
        consumer = null;
        if (c != null) {
            try {
                c.close();
            } catch (RuntimeException e) {
                log.debug("소비자 닫기 실패: {}", e.getMessage());
            }
        }
        running = false;
        starter.shutdownNow();
        try {
            starter.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** 스트림 연결 전에 DB·RabbitMQ AMQP 빈이 먼저, 웹 서버보다는 나중에 멈춘다 */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE - 100;
    }

    /** 처리 중인 메시지 수(종료·시험 대기용) */
    public int inFlight() {
        return inFlight.get();
    }

    public boolean isConsuming() {
        return consumer != null;
    }

    public String lastError() {
        return lastError;
    }

    /** 파티션별 마지막 처리(처리 지연 판정, ING-07.04) */
    public Map<Integer, PartitionProgress> progress() {
        return Map.copyOf(progress);
    }

    /** 파티션 처리 진행 */
    public static final class PartitionProgress {
        private volatile long offset = -1;
        private volatile Instant lastReceivedAt;
        private volatile Instant processedAt;
        private volatile long organizationId = 1;

        void update(long newOffset, Instant receivedAt, long organization, Instant at) {
            offset = newOffset;
            lastReceivedAt = receivedAt;
            organizationId = organization;
            processedAt = at;
        }

        public long organizationId() {
            return organizationId;
        }

        public long offset() {
            return offset;
        }

        public Instant lastReceivedAt() {
            return lastReceivedAt;
        }

        public Instant processedAt() {
            return processedAt;
        }
    }

    /** 읽을 수 없는 스트림 메시지를 보내는 곳(DLX) */
    public interface UnreadableSink {
        void send(Message message, String stream, long offset, Exception reason);
    }
}
