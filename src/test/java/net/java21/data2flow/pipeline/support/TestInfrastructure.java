package net.java21.data2flow.pipeline.support;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.IOException;
import java.time.Duration;

/**
 * 통합 테스트 인프라(ING test-plan "공통 테스트 환경"): Testcontainers PostgreSQL 18 + RabbitMQ 3.13(stream 플러그인) + core-api 대역.
 * JVM에 하나씩만 띄워 모든 IT가 함께 쓴다. 실제 s3·s4 인프라에는 붙지 않는다.
 */
public final class TestInfrastructure {

    public static final int STREAM_PORT = 5552;

    public static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("data2flow");
    @SuppressWarnings("resource")
    public static final GenericContainer<?> RABBIT = new GenericContainer<>("rabbitmq:3.13-management")
            .withExposedPorts(STREAM_PORT, 5672, 15672)
            .withCopyToContainer(Transferable.of("[rabbitmq_management,rabbitmq_stream]."), "/etc/rabbitmq/enabled_plugins")
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1).withStartupTimeout(Duration.ofMinutes(3)));
    public static final CoreApiStub CORE = new CoreApiStub();

    static {
        POSTGRES.start();
        RABBIT.start();
    }

    private TestInfrastructure() {
    }

    public static String rabbitHost() {
        return RABBIT.getHost();
    }

    public static int amqpPort() {
        return RABBIT.getMappedPort(5672);
    }

    public static int streamPort() {
        return RABBIT.getMappedPort(STREAM_PORT);
    }

    /** 테스트끼리 섞이지 않도록 별도 vhost를 만든다(NFR 시험: 별도 JVM 인스턴스) */
    public static void createVhost(String vhost) {
        try {
            exec("rabbitmqctl", "add_vhost", vhost);
            exec("rabbitmqctl", "set_permissions", "-p", vhost, "guest", ".*", ".*", ".*");
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("vhost를 만들지 못했습니다: " + vhost, e);
        }
    }

    private static void exec(String... command) throws IOException, InterruptedException {
        var result = RABBIT.execInContainer(command);
        if (result.getExitCode() != 0 && !result.getStderr().contains("already exists")) {
            throw new IllegalStateException(String.join(" ", command) + ": " + result.getStderr());
        }
    }
}
