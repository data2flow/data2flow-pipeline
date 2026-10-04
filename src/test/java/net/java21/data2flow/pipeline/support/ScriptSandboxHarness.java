package net.java21.data2flow.pipeline.support;

import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import net.java21.data2flow.script.sandbox.ScriptSandbox;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;

/**
 * SCR test-plan "샌드박스 하네스": 운영과 같은 {@link ScriptSandbox}(공용 모듈 data2flow-script-sandbox, ADR-046)를 운영 설정 파일
 * (application.yml)의 한도로 만든다. 공격 코퍼스 42종과 샌드박스 자체 시험은 공용 모듈로 옮겼고, 여기서는 pipeline 쪽 연결
 * (진입 함수 decode·transform, 설정 바인딩)만 쓴다.
 */
public final class ScriptSandboxHarness {

    public static final String NORMAL_INPUT = "{\"v\":1,\"deviceId\":17,\"measuredAt\":\"2026-10-03T00:00:00Z\","
            + "\"metrics\":[{\"key\":\"temperature\",\"value\":22.04,\"unit\":\"℃\",\"quality\":0}]}";

    private static final ScriptSandbox SANDBOX = new ScriptSandbox(properties().script().toLimits(),
            java.util.List.of(net.java21.data2flow.pipeline.script.service.ScriptRunner.RUNTIME_KEY));

    static {
        SANDBOX.warmUp(properties().script().toWarmUpPolicy());
    }

    private ScriptSandboxHarness() {
    }

    public static ScriptSandbox sandbox() {
        return SANDBOX;
    }

    /** application.yml의 data2flow.pipeline.* 를 그대로 읽는다 */
    public static PipelineProperties properties() {
        try {
            List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                    .load("application", new ClassPathResource("application.yml"));
            Binder binder = new Binder(ConfigurationPropertySources.from(sources), new PropertySourcesPlaceholdersResolver(sources));
            return binder.bindOrCreate("data2flow.pipeline", PipelineProperties.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ScriptOutcome transform(String code) {
        return transform(code, NORMAL_INPUT, "{}");
    }

    public static ScriptOutcome transform(String code, String input, String ctx) {
        return SANDBOX.run(ScriptKind.TRANSFORM.functionName(), code, "script.js", input, ctx, Instant.parse("2026-10-03T00:00:00Z"));
    }

    public static ScriptOutcome decode(String code, String input, String ctx) {
        return SANDBOX.run(ScriptKind.DECODE.functionName(), code, "script.js", input, ctx, Instant.parse("2026-10-03T00:00:00Z"));
    }
}
