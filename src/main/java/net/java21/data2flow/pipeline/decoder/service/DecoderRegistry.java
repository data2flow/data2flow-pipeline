package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.decoder.DecoderKeys;
import net.java21.data2flow.contracts.message.decoder.PayloadDecoder;
import net.java21.data2flow.pipeline.device.domain.SourceContext;
import net.java21.data2flow.pipeline.script.service.ScriptOutputValidator;
import net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry;
import net.java21.data2flow.script.sandbox.ScriptSandbox;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;

/**
 * 디코더 레지스트리(ING-02.01 TC-ING-032, DSC-01.06): 소스 설정 {@code decoderKey}로 고른다. 기본 제공 {@code chirpstack-v4}·
 * {@code generic-json}·{@code single-value}, 사용자 DECODE 스크립트 {@code script}({@code decodeScriptId} 또는 소스 연결) 또는
 * {@code script:{id}@v{n}}. 모르는 키는 {@code ING_DECODE_FAILED}.
 */
public class DecoderRegistry {

    private final Map<String, PayloadDecoder> builtin;
    private final ScriptSandbox sandbox;
    private final ScriptOutputValidator validator;
    private final Clock clock;

    public DecoderRegistry(ScriptSandbox sandbox, ScriptOutputValidator validator, Clock clock) {
        this.builtin = Map.of(
                DecoderKeys.CHIRPSTACK_V4, new ChirpStackV4Decoder(),
                DecoderKeys.GENERIC_JSON, new GenericJsonDecoder(),
                DecoderKeys.SINGLE_VALUE, new SingleValueDecoder());
        this.sandbox = sandbox;
        this.validator = validator;
        this.clock = clock;
    }

    /**
     * @param source 소스 처리 정보
     * @param plan   이 메시지가 쓰는 스크립트 계획(DECODE 스크립트 소스). 스크립트가 아니면 쓰지 않는다
     */
    public PayloadDecoder select(SourceContext source, ScriptRuntimeRegistry.Plan plan) throws IngestDecodeException {
        String key = source.decoderKey() == null ? DecoderKeys.CHIRPSTACK_V4 : source.decoderKey();
        PayloadDecoder decoder = builtin.get(key);
        if (decoder != null) {
            return decoder;
        }
        Optional<DecoderKeys.ScriptVersion> pinned = DecoderKeys.parseScript(key);
        if ("script".equalsIgnoreCase(key) || pinned.isPresent()) {
            Long scriptId = pinned.map(DecoderKeys.ScriptVersion::scriptId).orElse(source.decodeScriptId());
            var script = scriptId != null ? plan.decodeScript(scriptId) : plan.decodeScriptForSource(source.sourceId());
            if (script.isEmpty()) {
                throw IngestDecodeException.failed(key, "활성 DECODE 스크립트가 없습니다(script=" + scriptId + ")");
            }
            return new ScriptPayloadDecoder(script.get(), sandbox, validator, clock);
        }
        throw IngestDecodeException.failed(key, "모르는 디코더입니다: " + key);
    }
}
