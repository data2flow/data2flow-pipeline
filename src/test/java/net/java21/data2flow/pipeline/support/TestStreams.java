package net.java21.data2flow.pipeline.support;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * 테스트용 스트림 도구: ingress처럼 {@code data2flow.raw}에 원본을 발행하고(라우팅 키·헤더 동일), flow-engine처럼
 * {@code data2flow.telemetry}를 읽어 모은다.
 */
public final class TestStreams implements AutoCloseable {

    private static final MessageCodec CODEC = MessageCodec.create();

    private final Environment environment;
    private final Producer rawProducer;

    public TestStreams(String vhost, int partitions) {
        String host = TestInfrastructure.rabbitHost();
        int port = TestInfrastructure.streamPort();
        this.environment = Environment.builder().host(host).port(port).username("guest").password("guest")
                .virtualHost(vhost).addressResolver(address -> new Address(host, port)).build();
        for (SuperStreamSpec spec : List.of(SuperStreamSpec.RAW, SuperStreamSpec.TELEMETRY)) {
            try {
                environment.streamCreator().name(spec.name()).maxAge(spec.maxAge()).superStream()
                        .partitions(partitions).creator().create();
            } catch (StreamException e) {
                // 이미 있음
            }
        }
        this.rawProducer = environment.producerBuilder().superStream(MessagingNames.STREAM_RAW)
                .routing(m -> m.getApplicationProperties().get("routingKey").toString()).producerBuilder().build();
    }

    /** 발행하고 확인(confirm)을 기다린다 */
    public void publish(RawEnvelope envelope) {
        CompletableFuture<Boolean> confirmed = new CompletableFuture<>();
        var builder = rawProducer.messageBuilder().properties().messageId(envelope.messageId().toString()).messageBuilder()
                .applicationProperties();
        MessageHeaders.of(envelope).forEach((k, v) -> builder.entry(k, v.toString()));
        builder.entry("routingKey", envelope.routingKey());
        rawProducer.send(builder.messageBuilder().addData(CODEC.write(envelope)).build(),
                status -> confirmed.complete(status.isConfirmed()));
        try {
            if (!confirmed.get(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("data2flow.raw 발행 확인 실패");
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 지금부터 들어오는 표준 메시지를 모은다 */
    public TelemetryCollector collectTelemetry() {
        return new TelemetryCollector(environment);
    }

    @Override
    public void close() {
        rawProducer.close();
        environment.close();
    }

    /** {@code data2flow.telemetry} 수집기(그룹 없이, 다음 오프셋부터) */
    public static final class TelemetryCollector implements AutoCloseable {
        private final List<Received> received = new CopyOnWriteArrayList<>();
        private final Consumer consumer;

        TelemetryCollector(Environment environment) {
            this.consumer = environment.consumerBuilder().superStream(MessagingNames.STREAM_TELEMETRY)
                    .offset(OffsetSpecification.next())
                    .messageHandler((ctx, message) -> received.add(new Received(ctx.stream(),
                            CODEC.read(message.getBodyAsBinary(), CanonicalTelemetry.class),
                            message.getApplicationProperties())))
                    .build();
        }

        public List<Received> received() {
            return List.copyOf(received);
        }

        public List<CanonicalTelemetry> telemetry() {
            return received.stream().map(Received::telemetry).toList();
        }

        @Override
        public void close() {
            consumer.close();
        }
    }

    public record Received(String partition, CanonicalTelemetry telemetry, java.util.Map<String, Object> headers) {
    }

    public static Duration timeout() {
        return Duration.ofSeconds(30);
    }
}
