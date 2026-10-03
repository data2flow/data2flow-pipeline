package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.pipeline.device.domain.SourceContext;
import net.java21.data2flow.pipeline.device.domain.UnknownDevicePolicy;
import net.java21.data2flow.pipeline.script.domain.FailurePolicy;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.service.ScriptOutputValidator;
import net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Clock;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ING-02.01 TC-ING-032 · AT-ING-02.9 · DSC-01.06: 소스 설정으로 디코더를 고른다 */
class DecoderRegistryTest {

    private static final String DECODE = """
            function decode(input, ctx) {
              const b = ctx.util.bytes.fromBase64(input.payloadBase64);
              return {externalId: 'dev-' + ctx.config.suffix, metrics: [{key: 'temperature', value: ctx.util.bytes.readInt16LE(b, 2) / 10}]};
            }
            """;

    private final DecoderRegistry registry = new DecoderRegistry(ScriptSandboxHarness.sandbox(),
            new ScriptOutputValidator(100), Clock.systemUTC());

    private static SourceContext source(String key, Long scriptId) {
        return new SourceContext(3, 1, key, scriptId, DecoderTestSupport.MAPPER.createObjectNode(),
                UnknownDevicePolicy.AUTO_REGISTER, 100, null, null, 1);
    }

    private static ScriptRuntimeRegistry.Plan plan() {
        return new ScriptRuntimeRegistry.Plan(new RuntimeBundle(1, List.of(new RuntimeBundle.Script(42, ScriptKind.DECODE,
                420, 1, DECODE, DecoderTestSupport.MAPPER.readTree("{\"suffix\":\"x\"}"), FailurePolicy.FAIL_OPEN, true,
                List.of(new RuntimeBundle.Binding("SOURCE", 3, true, null))))));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({"chirpstack-v4", "generic-json", "single-value"})
    @DisplayName("[ING-02.01][AT-ING-02.9] TC-ING-032 기본 제공 디코더 키로 고른다")
    void builtin(String key) throws Exception {
        assertThat(registry.select(source(key, null), plan()).key()).isEqualTo(key);
    }

    @Test
    @DisplayName("[ING-02.01][AT-ING-02.9] TC-ING-032 script는 script:{id}@v{n} 디코더, 바이트 payload를 스크립트가 해석(TC-ING-034)")
    void script() throws Exception {
        var decoder = registry.select(source("script", 42L), plan());
        DecodedUplink uplink = decoder.decode(DecoderTestSupport.raw("t", new byte[]{1, 0x75, 0x68, 0x01}), null);

        assertThat(decoder.key()).isEqualTo("script:42@v1");
        assertThat(decoder.version()).isEqualTo("1");
        assertThat(uplink.externalId()).isEqualTo("dev-x");
        assertThat(uplink.values().getFirst().asDouble()).isEqualTo(36.0);
        assertThat(registry.select(source("script", null), plan()).key()).isEqualTo("script:42@v1");
        assertThat(registry.select(source("script:42@v1", null), plan()).key()).isEqualTo("script:42@v1");
    }

    @Test
    @DisplayName("[ING-02.01][AT-ING-02.9] TC-ING-032 모르는 키·없는 스크립트는 ING_DECODE_FAILED, 스크립트 예외는 DECODE_ERROR(fail-open 없음)")
    void unknown() throws Exception {
        assertThatThrownBy(() -> registry.select(source("nope", null), plan()))
                .isInstanceOfSatisfying(IngestDecodeException.class, e -> assertThat(e.errorCode()).isEqualTo("ING_DECODE_FAILED"));
        assertThatThrownBy(() -> registry.select(source("script", 7L), plan())).isInstanceOf(IngestDecodeException.class);
        var broken = new ScriptPayloadDecoder(new RuntimeBundle.Script(5, ScriptKind.DECODE, 50, 2,
                "function decode(input, ctx) {\n  throw new Error('boom');\n}", DecoderTestSupport.MAPPER.createObjectNode(),
                FailurePolicy.FAIL_OPEN, true, List.of()), ScriptSandboxHarness.sandbox(), new ScriptOutputValidator(100),
                Clock.systemUTC());
        assertThatThrownBy(() -> broken.decode(DecoderTestSupport.raw("t", "x"), null))
                .isInstanceOfSatisfying(IngestDecodeException.class, e -> {
                    assertThat(e.errorCode()).isEqualTo("ING_DECODE_FAILED");
                    assertThat(e.getMessage()).contains("SCRIPT_RUNTIME_ERROR");
                });
        var invalid = new ScriptPayloadDecoder(new RuntimeBundle.Script(6, ScriptKind.DECODE, 60, 1,
                "function decode(input, ctx) { return {metrics: []}; }", DecoderTestSupport.MAPPER.createObjectNode(),
                FailurePolicy.FAIL_OPEN, true, List.of()), ScriptSandboxHarness.sandbox(), new ScriptOutputValidator(100),
                Clock.systemUTC());
        assertThatThrownBy(() -> invalid.decode(DecoderTestSupport.raw("t", "x"), null))
                .hasMessageContaining("SCRIPT_OUTPUT_INVALID");
    }
}
