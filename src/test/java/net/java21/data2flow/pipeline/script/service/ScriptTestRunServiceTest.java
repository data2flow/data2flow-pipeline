package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.dto.ScriptTestRunRequest;
import net.java21.data2flow.pipeline.script.dto.ScriptTestRunResponse;
import net.java21.data2flow.pipeline.support.MutableClock;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** SCR-03.02 TC-SCR-042 · SCR-01.01 TC-SCR-001 · SCR-01.02 TC-SCR-006: 테스트 실행 결과(출력·차이·시간·로그)와 반환값 계약 */
class ScriptTestRunServiceTest {

    private static final JsonMapper MAPPER = MessageCodec.newMapper();

    @SuppressWarnings("unchecked")
    private final ObjectProvider<RawInputLoader> raws = mock(ObjectProvider.class);
    @SuppressWarnings("unchecked")
    private final ObjectProvider<ScriptConfigLookup> configs = mock(ObjectProvider.class);
    private final ScriptTestRunService service = new ScriptTestRunService(ScriptSandboxHarness.sandbox(),
            new ScriptOutputValidator(100), raws, configs, MutableClock.atUtc("2026-10-03T00:00:00Z"));

    private static JsonNode json(String s) {
        return MAPPER.readTree(s);
    }

    @Test
    @DisplayName("[SCR-03.02][AT-SCR-02.1] TC-SCR-042 결과에 출력·차이(추가 dew_point, 바뀐 temperature)·시간·console 로그")
    void transformDiff() {
        ScriptTestRunResponse r = service.run(new ScriptTestRunRequest(ScriptKind.TRANSFORM, """
                function transform(msg, ctx) {
                  console.log('offset', ctx.config.offset);
                  const t = ctx.util.metric(msg, 'temperature');
                  t.value = ctx.util.round(t.value + ctx.config.offset, 1);
                  ctx.util.setMetric(msg, 'dew_point', ctx.util.dewPoint(t.value, 60));
                  return msg;
                }
                """, json("{\"metrics\":[{\"key\":\"temperature\",\"value\":25.0},{\"key\":\"humidity\",\"value\":60}]}"),
                json("{\"config\":{\"offset\":0.5},\"last\":{\"temperature\":24.9}}"), null, 1L, null));

        assertThat(r.ok()).as(String.valueOf(r.error())).isTrue();
        assertThat(r.diff().added()).containsExactly("dew_point");
        assertThat(r.diff().changed()).singleElement().satisfies(c -> {
            assertThat(c.key()).isEqualTo("temperature");
            assertThat(c.to()).isEqualTo(25.5);
        });
        assertThat(r.logs()).extracting(ScriptTestRunResponse.Log::message).containsExactly("offset 0.5");
        assertThat(r.durationMs()).isPositive();
        assertThat(r.outputBytes()).isPositive();
    }

    @Test
    @DisplayName("[SCR-03.02][AT-SCR-02.3] TC-SCR-042 metrics[0].value = \"abc\" → SCRIPT_OUTPUT_INVALID \"metrics[0].value는 숫자여야 합니다\"")
    void outputContract() {
        ScriptTestRunResponse r = service.run(new ScriptTestRunRequest(ScriptKind.TRANSFORM,
                "function transform(msg, ctx) { msg.metrics[0].value = 'abc'; return msg; }",
                json("{\"metrics\":[{\"key\":\"temperature\",\"value\":25.0}]}"), null, null, 1L, null));

        assertThat(r.ok()).isFalse();
        assertThat(r.error().code()).isEqualTo("SCRIPT_OUTPUT_INVALID");
        assertThat(r.error().message()).isEqualTo("metrics[0].value는 숫자여야 합니다");
    }

    @Test
    @DisplayName("[SCR-01.01][AT-SCR-05.1] TC-SCR-001 decode(input): topic·payloadBase64·receivedAt·source.config 전달, externalId 없으면 SCRIPT_OUTPUT_INVALID")
    void decodeContract() {
        String code = """
                function decode(input, ctx) {
                  const b = ctx.util.bytes.fromBase64(input.payloadBase64);
                  return {externalId: input.topic.split('/')[1], measuredAt: input.receivedAt,
                          metrics: [{key: 'temperature', value: ctx.util.bytes.readInt16LE(b, 2) / 10},
                                    {key: 'humidity', value: b[6] / 2}],
                          meta: {tags: {mode: input.source.config.mode}}};
                }
                """;
        ScriptTestRunResponse r = service.run(new ScriptTestRunRequest(ScriptKind.DECODE, code,
                json("{\"topic\":\"milesight/dev-1/up\",\"payload\":\"AXVoAQJnBgE=\",\"receivedAt\":\"2026-10-03T01:00:00Z\","
                        + "\"source\":{\"code\":\"src\",\"config\":{\"mode\":\"tlv\"}}}"), null, null, 1L, null));
        ScriptTestRunResponse missing = service.run(new ScriptTestRunRequest(ScriptKind.DECODE,
                "function decode(input, ctx) { return {metrics: []}; }", json("{\"payload\":{\"a\":1}}"), null, null, 1L,
                null));

        assertThat(r.ok()).as(String.valueOf(r.error())).isTrue();
        assertThat(r.output().get("externalId").asString()).isEqualTo("dev-1");
        assertThat(r.output().get("measuredAt").asString()).isEqualTo("2026-10-03T01:00:00Z");
        assertThat(r.diff().added()).containsExactly("temperature", "humidity");
        assertThat(missing.error().code()).isEqualTo("SCRIPT_OUTPUT_INVALID");
        assertThat(missing.error().message()).isEqualTo("externalId가 필요합니다(1~128자 문자열)");
    }

    @Test
    @DisplayName("[SCR-01.02][AT-SCR-04.1] TC-SCR-006 ctx.device.attributes·ctx.config는 읽기 전용(쓰면 TypeError), null 반환은 버리기")
    void readOnlyContext() {
        ScriptTestRunResponse write = service.run(new ScriptTestRunRequest(ScriptKind.TRANSFORM,
                "function transform(msg, ctx) { ctx.config.offset = 9; return msg; }",
                json("{\"metrics\":[]}"), json("{\"config\":{\"offset\":1},\"device\":{\"id\":1,\"attributes\":{\"a\":1}}}"),
                null, 1L, null));
        ScriptTestRunResponse attr = service.run(new ScriptTestRunRequest(ScriptKind.TRANSFORM,
                "function transform(msg, ctx) { ctx.device.attributes.a = 2; return msg; }",
                json("{\"metrics\":[]}"), json("{\"device\":{\"id\":1,\"attributes\":{\"a\":1}}}"), null, 1L, null));
        ScriptTestRunResponse drop = service.run(new ScriptTestRunRequest(ScriptKind.TRANSFORM,
                "function transform(msg, ctx) { return null; }",
                json("{\"metrics\":[{\"key\":\"t\",\"value\":1}]}"), null, null, 1L, null));

        assertThat(write.error().message()).contains("TypeError");
        assertThat(attr.error().message()).contains("TypeError");
        assertThat(drop.ok()).isTrue();
        assertThat(drop.diff().removed()).containsExactly("t");
    }

    @Test
    @DisplayName("[SCR-03.02] 원본 메시지 ID로 입력을 만들고(없으면 404), scriptId가 있으면 번들 설정값을 ctx.config로 쓴다")
    void rawMessageAndConfig() {
        RawInputLoader loader = (org, id) -> id == 5 ? Optional.of(new RawInputLoader.RawInput("t/dev-5/x",
                "22.8".getBytes(), Instant.parse("2026-10-03T00:00:00Z"), 3, "WEBHOOK")) : Optional.empty();
        when(raws.getIfAvailable()).thenReturn(loader);
        when(configs.getIfAvailable()).thenReturn((org, id) -> Optional.of(json("{\"k\":\"v\"}")));
        ScriptTestRunResponse r = service.run(new ScriptTestRunRequest(ScriptKind.DECODE, """
                function decode(input, ctx) {
                  return {externalId: input.topic.split('/')[1], metrics: [{key: 'x', value: Number(input.payloadText)}],
                          meta: {tags: {k: ctx.config.k, enc: input.payloadEncoding}}};
                }
                """, null, null, 9L, 1L, 5L));

        assertThat(r.ok()).as(String.valueOf(r.error())).isTrue();
        assertThat(r.output().get("meta").get("tags").get("k").asString()).isEqualTo("v");
        assertThat(r.output().get("meta").get("tags").get("enc").asString()).isEqualTo("TEXT");
        assertThatThrownBy(() -> service.run(new ScriptTestRunRequest(ScriptKind.DECODE, "x", null, null, null, 1L, 6L)))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> service.run(new ScriptTestRunRequest(ScriptKind.TRANSFORM, "x", null, null, null, 1L, null)))
                .isInstanceOf(BusinessException.class);
    }
}
