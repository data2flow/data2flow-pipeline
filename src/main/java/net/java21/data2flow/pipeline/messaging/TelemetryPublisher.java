package net.java21.data2flow.pipeline.messaging;

import com.rabbitmq.stream.Producer;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessageTracing;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.common.TransientFailures.PublishFailedException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@code data2flow.telemetry} 발행(ING-05.01, EVT-ING-02). 라우팅 키는 deviceId(같은 기기는 같은 파티션). DB 커밋 뒤에만 부르고
 * 발행 확인(confirm)을 기다린다. 확인을 못 받으면 {@link PublishFailedException}(호출하는 쪽은 원본 오프셋을 저장하지 않고 다시 처리,
 * TC-ING-063). 공통 헤더와 traceparent를 애플리케이션 속성에 싣는다(MessageHeaders, OPS-02.03).
 */
public class TelemetryPublisher implements AutoCloseable {

    private final StreamConnection connection;
    private final PipelineProperties.Stream settings;
    private final MessageTracing tracing;
    private final MessageCodec codec = MessageCodec.create();
    private volatile Producer producer;

    public TelemetryPublisher(StreamConnection connection, PipelineProperties.Stream settings, MessageTracing tracing) {
        this.connection = connection;
        this.settings = settings;
        this.tracing = tracing;
    }

    private synchronized Producer producer() {
        if (producer == null) {
            connection.ensureSuperStream(SuperStreamSpec.TELEMETRY);
            producer = connection.environment().producerBuilder()
                    .superStream(MessagingNames.STREAM_TELEMETRY)
                    .routing(message -> message.getApplicationProperties().get("routingKey").toString())
                    .producerBuilder()
                    .build();
        }
        return producer;
    }

    /** 확인을 받을 때까지 기다린다 */
    public void publish(CanonicalTelemetry telemetry) {
        Map<String, Object> headers = MessageHeaders.of(telemetry);
        var span = tracing.startProducerSpan(MessagingNames.STREAM_TELEMETRY, headers);
        CompletableFuture<Boolean> confirmed = new CompletableFuture<>();
        try {
            Producer p = producer();
            var builder = p.messageBuilder()
                    .properties().messageId(telemetry.messageId().toString()).contentType("application/json").messageBuilder()
                    .applicationProperties();
            headers.forEach((k, v) -> builder.entry(k, v.toString()));
            builder.entry("routingKey", telemetry.routingKey());
            p.send(builder.messageBuilder().addData(codec.write(telemetry)).build(),
                    status -> confirmed.complete(status.isConfirmed()));
            if (!confirmed.get(settings.publishTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                throw new PublishFailedException("data2flow.telemetry 발행이 확인되지 않았습니다", null);
            }
            tracing.end(span, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            tracing.end(span, e);
            throw new PublishFailedException("발행 대기가 중단되었습니다", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            tracing.end(span, e);
            resetOnFailure();
            if (e instanceof PublishFailedException pfe) {
                throw pfe;
            }
            throw new PublishFailedException("data2flow.telemetry 발행 실패: " + e.getMessage(), e);
        }
    }

    private synchronized void resetOnFailure() {
        if (producer != null) {
            try {
                producer.close();
            } catch (RuntimeException ignored) {
                // 다음 발행에서 새로 만든다
            }
            producer = null;
        }
    }

    @Override
    public synchronized void close() {
        resetOnFailure();
    }
}
