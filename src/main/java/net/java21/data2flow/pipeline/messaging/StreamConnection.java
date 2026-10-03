package net.java21.data2flow.pipeline.messaging;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.StreamException;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;

import java.time.Duration;

/**
 * RabbitMQ Stream 연결(Environment) 하나를 서비스가 함께 쓴다. 호스트·계정·vhost는 {@code spring.rabbitmq.*}, 포트는 5552.
 * 단일 노드(s4)·테스트에서는 브로커가 알려 주는 주소 대신 설정한 주소로만 접속한다({@code fixed-address}).
 * Super Stream이 없으면 계약({@link SuperStreamSpec})대로 만든다(생산자 서비스가 만든다, contracts SuperStreamSpec).
 */
public class StreamConnection implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamConnection.class);
    /** stream-client Constants.RESPONSE_CODE_STREAM_ALREADY_EXISTS */
    private static final short ALREADY_EXISTS = 5;

    private final RabbitProperties rabbit;
    private final PipelineProperties.Stream settings;
    private volatile Environment environment;

    public StreamConnection(RabbitProperties rabbit, PipelineProperties.Stream settings) {
        this.rabbit = rabbit;
        this.settings = settings;
    }

    /** 연결을 연다(실패하면 예외, 호출하는 쪽이 다시 시도) */
    public synchronized Environment environment() {
        if (environment == null) {
            String host = rabbit.getHost();
            int port = settings.port();
            var builder = Environment.builder()
                    .host(host).port(port)
                    .username(rabbit.getUsername()).password(rabbit.getPassword())
                    .virtualHost(rabbit.getVirtualHost() == null ? "/" : rabbit.getVirtualHost())
                    .recoveryBackOffDelayPolicy(com.rabbitmq.stream.BackOffDelayPolicy.fixedWithInitialDelay(
                            Duration.ofSeconds(1), Duration.ofSeconds(2)))
                    .topologyUpdateBackOffDelayPolicy(com.rabbitmq.stream.BackOffDelayPolicy.fixedWithInitialDelay(
                            Duration.ofSeconds(1), Duration.ofSeconds(2)));
            if (settings.fixedAddress()) {
                builder.addressResolver(address -> new Address(host, port));
            }
            environment = builder.build();
        }
        return environment;
    }

    /** Super Stream이 없으면 만든다. 파티션 수는 설정(운영 12, 테스트 3) */
    public void ensureSuperStream(SuperStreamSpec spec) {
        try {
            environment().streamCreator()
                    .name(spec.name())
                    .maxAge(spec.maxAge())
                    .maxLengthBytes(com.rabbitmq.stream.ByteCapacity.B(spec.maxBytesPerPartition()))
                    .superStream().partitions(settings.partitions()).creator()
                    .create();
            log.info("Super Stream {}을(를) 만들었습니다(파티션 {})", spec.name(), settings.partitions());
        } catch (StreamException e) {
            if (e.getCode() != ALREADY_EXISTS) {
                throw e;
            }
        }
    }

    /** 파티션 스트림 이름 {@code data2flow.raw-3} → 3. 모르면 -1 */
    public static int partitionIndex(String stream) {
        int dash = stream == null ? -1 : stream.lastIndexOf('-');
        if (dash < 0) {
            return -1;
        }
        try {
            return Integer.parseInt(stream.substring(dash + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    @Override
    public synchronized void close() {
        if (environment != null) {
            try {
                environment.close();
            } catch (RuntimeException e) {
                log.debug("Stream 연결 닫기 실패: {}", e.getMessage());
            }
            environment = null;
        }
    }
}
