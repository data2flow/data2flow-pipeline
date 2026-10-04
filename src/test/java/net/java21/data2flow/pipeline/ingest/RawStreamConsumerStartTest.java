package net.java21.data2flow.pipeline.ingest;

import com.rabbitmq.stream.Environment;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import net.java21.data2flow.pipeline.ingest.service.RawStreamConsumer;
import net.java21.data2flow.pipeline.messaging.StreamConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Clock;
import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ING-05.01: pipeline은 소비를 시작할 때 data2flow.raw와 함께 자기가 생산하는 data2flow.telemetry Super Stream도 만든다.
 * 첫 발행까지 미루면 텔레메트리 소비자(flow-engine·core)가 없는 스트림을 기다리게 된다(M3 시연에서 발견).
 */
class RawStreamConsumerStartTest {

    @Test
    @DisplayName("[ING-05.01] 소비 시작 때 raw·telemetry Super Stream을 모두 보장한 뒤 소비자를 연다")
    void ensuresTelemetryStreamOnStart() {
        StreamConnection connection = mock(StreamConnection.class);
        Environment environment = mock(Environment.class, RETURNS_DEEP_STUBS);
        when(connection.environment()).thenReturn(environment);
        RawStreamConsumer consumer = new RawStreamConsumer(connection, null, null,
                mock(net.java21.data2flow.pipeline.common.PipelineProperties.class), null, null, Clock.systemUTC());

        consumer.start();
        try {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(environment, atLeast(1)).consumerBuilder());
            InOrder order = inOrder(connection);
            order.verify(connection).ensureSuperStream(SuperStreamSpec.RAW);
            order.verify(connection).ensureSuperStream(SuperStreamSpec.TELEMETRY);
        } finally {
            consumer.stop();
        }
    }
}
