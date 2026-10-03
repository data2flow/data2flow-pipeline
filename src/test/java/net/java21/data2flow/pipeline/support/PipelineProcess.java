package net.java21.data2flow.pipeline.support;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.awaitility.Awaitility.await;

/**
 * pipeline을 <b>별도 JVM</b>으로 띄운다(ING test-plan "장애 도구": ProcessHandle.destroyForcibly()로 kill -9). 테스트 클래스패스와
 * test 프로필을 그대로 쓰고, 인프라 주소는 Testcontainers 값으로 넘긴다.
 */
public final class PipelineProcess implements AutoCloseable {

    private final String name;
    private final Process process;
    private final int managementPort;
    private final Path log;

    private PipelineProcess(String name, Process process, int managementPort, Path log) {
        this.name = name;
        this.process = process;
        this.managementPort = managementPort;
        this.log = log;
    }

    public static PipelineProcess start(String name, String vhost) {
        try {
            int management = freePort();
            Path log = Files.createTempFile("pipeline-" + name, ".log");
            String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            Path argFile = Files.createTempFile("pipeline-" + name, ".args");
            Files.writeString(argFile, "-cp \"" + classpath.replace("\\", "\\\\") + "\"");
            List<String> command = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx512m", "-Dpolyglotimpl.AttachLibraryFailureAction=ignore", "@" + argFile,
                    "net.java21.data2flow.pipeline.PipelineApplication",
                    "--spring.profiles.active=test",
                    "--spring.datasource.url=" + TestInfrastructure.POSTGRES.getJdbcUrl(),
                    "--spring.datasource.username=" + TestInfrastructure.POSTGRES.getUsername(),
                    "--spring.datasource.password=" + TestInfrastructure.POSTGRES.getPassword(),
                    "--spring.rabbitmq.host=" + TestInfrastructure.rabbitHost(),
                    "--spring.rabbitmq.port=" + TestInfrastructure.amqpPort(),
                    "--spring.rabbitmq.username=guest", "--spring.rabbitmq.password=guest",
                    "--spring.rabbitmq.virtual-host=" + vhost,
                    "--data2flow.pipeline.stream.port=" + TestInfrastructure.streamPort(),
                    "--data2flow.pipeline.core.base-url=" + TestInfrastructure.CORE.baseUrl(),
                    "--data2flow.pipeline.instance-id=" + name,
                    "--data2flow.pipeline.flyway-mode=validate",
                    "--server.port=0",
                    "--management.server.port=" + management));
            Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
            return new PipelineProcess(name, process, management, log);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** readiness(스트림 소비자 포함)가 UP이 될 때까지 */
    public PipelineProcess awaitReady(Duration timeout) {
        HttpClient http = HttpClient.newHttpClient();
        await().atMost(timeout).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().until(() -> {
            if (!process.isAlive()) {
                throw new IllegalStateException(name + " 프로세스가 끝났습니다:\n" + tail());
            }
            HttpResponse<String> res = http.send(HttpRequest.newBuilder(URI.create(
                    "http://localhost:" + managementPort + "/actuator/health/readiness")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return res.statusCode() == 200;
        });
        return this;
    }

    /** kill -9(정리 없이 즉시 종료) */
    public void kill() {
        process.toHandle().destroyForcibly();
        await().atMost(Duration.ofSeconds(30)).until(() -> !process.isAlive());
    }

    public boolean alive() {
        return process.isAlive();
    }

    public String tail() {
        try {
            List<String> lines = Files.readAllLines(log);
            return String.join("\n", lines.subList(Math.max(0, lines.size() - 40), lines.size()));
        } catch (IOException e) {
            return "";
        }
    }

    @Override
    public void close() {
        if (process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @SuppressWarnings("unused")
    private static String path(File f) {
        return f.getAbsolutePath();
    }
}
