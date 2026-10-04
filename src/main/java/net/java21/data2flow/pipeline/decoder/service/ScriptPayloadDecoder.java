package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.decoder.DecodeException;
import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecoderKeys;
import net.java21.data2flow.contracts.message.decoder.PayloadDecoder;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import net.java21.data2flow.pipeline.script.service.ScriptInputs;
import net.java21.data2flow.pipeline.script.service.ScriptOutputValidator;
import net.java21.data2flow.script.sandbox.ScriptSandbox;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DECODE 스크립트 디코더 {@code script:{id}@v{n}}(SCR-01.01, ING-02.01 TC-ING-034). 스크립트 예외·시간 초과·계약 위반은 모두
 * {@code DECODE_ERROR}다(원본 해석이 불가능하므로 fail-open이 없음). 오류 코드·줄·열은 detail에 남는다.
 */
public class ScriptPayloadDecoder implements PayloadDecoder {

    private final RuntimeBundle.Script script;
    private final ScriptSandbox sandbox;
    private final ScriptOutputValidator validator;
    private final Clock clock;
    private final JsonMapper mapper = MessageCodec.newMapper();

    private final net.java21.data2flow.pipeline.script.service.ScriptRunner runner;
    private final RuntimeBundle bundle;

    public ScriptPayloadDecoder(RuntimeBundle.Script script, ScriptSandbox sandbox, ScriptOutputValidator validator,
                                Clock clock) {
        this(script, sandbox, validator, clock, null, RuntimeBundle.EMPTY);
    }

    /** 운영: 공유 모듈 연결·운영 지표·오류 스냅샷을 남기는 실행 창구({@code runner})로 실행한다(SCR-03.05·04.01·05.01) */
    public ScriptPayloadDecoder(RuntimeBundle.Script script, ScriptSandbox sandbox, ScriptOutputValidator validator,
                                Clock clock, net.java21.data2flow.pipeline.script.service.ScriptRunner runner,
                                RuntimeBundle bundle) {
        this.script = script;
        this.sandbox = sandbox;
        this.validator = validator;
        this.clock = clock;
        this.runner = runner;
        this.bundle = bundle;
    }

    @Override
    public String key() {
        return DecoderKeys.script(script.scriptId(), script.versionNo());
    }

    @Override
    public String version() {
        return Integer.toString(script.versionNo());
    }

    public RuntimeBundle.Script script() {
        return script;
    }

    @Override
    public DecodedUplink decode(RawEnvelope raw, JsonNode config) throws DecodeException {
        ObjectNode input = ScriptInputs.decodeInput(mapper, raw.topic(), raw.payload(), raw.receivedAt(), raw.sourceId(),
                raw.sourceType(), config);
        ObjectNode ctx = mapper.createObjectNode();
        ctx.set("config", script.config());
        ctx.set("source", input.get("source"));
        ScriptOutcome outcome = runner != null
                ? runner.run(bundle, script, ScriptKind.DECODE, mapper.writeValueAsString(input), ctx,
                new net.java21.data2flow.pipeline.script.service.ScriptRunner.Execution(raw.organizationId(), null, null,
                        clock.instant(), null, null, true))
                : sandbox.run(ScriptKind.DECODE.functionName(), script.code(),
                "script-" + script.scriptId() + "-v" + script.versionNo() + ".js",
                mapper.writeValueAsString(input), mapper.writeValueAsString(ctx), clock.instant());
        if (!outcome.ok()) {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("scriptErrorCode", outcome.failure().code().name());
            detail.put("line", outcome.failure().line());
            detail.put("col", outcome.failure().col());
            throw new IngestDecodeException(key(), IngestDecodeException.RESULT_CODE,
                    outcome.failure().code().name() + ": " + outcome.failure().message(), detail, null);
        }
        try {
            return validator.toDecodedUplink(outcome.output());
        } catch (ScriptOutputValidator.OutputContractException e) {
            throw new IngestDecodeException(key(), IngestDecodeException.RESULT_CODE, "SCRIPT_OUTPUT_INVALID: " + e.getMessage(),
                    Map.of("scriptErrorCode", "SCRIPT_OUTPUT_INVALID"), null);
        }
    }
}
