package net.java21.data2flow.pipeline.script.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.script.sandbox.ScriptErrorCode;
import net.java21.data2flow.script.sandbox.ScriptFailure;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import net.java21.data2flow.script.sandbox.ScriptSandbox;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 운영 스크립트 실행 창구(DECODE·TRANSFORM·수식). 공유 모듈 연결과 {@code ctx.window}({@link ScriptLinker}), 운영 지표·오류 스냅샷·
 * 로그 수집({@link ScriptOps})을 한곳에서 한다. 테스트 실행(API-SCR-31)은 {@code record=false}로 불러 지표에 넣지 않는다(BR-SCR-08).
 */
public class ScriptRunner {

    /** ctx에 호스트가 넣는 내부 필드(사용자에게는 감싸개가 숨긴다) */
    public static final String RUNTIME_KEY = "runtime";

    private final ScriptSandbox sandbox;
    private final ScriptOps ops;
    private final JsonMapper mapper = MessageCodec.newMapper();
    private final Cache<String, String> linked = Caffeine.newBuilder().maximumSize(2_000).build();

    public ScriptRunner(ScriptSandbox sandbox, ScriptOps ops) {
        this.sandbox = sandbox;
        this.ops = ops;
    }

    /**
     * @param bundle   모듈을 찾을 번들(재처리는 고정한 번들)
     * @param script   실행할 스크립트
     * @param ctx      ctx(config·device·last 등). {@code window}가 있으면 {@code runtime}으로 옮긴다
     * @param exec     실행 정보(기록 여부, 기기·원본, 창 데이터)
     */
    public ScriptOutcome run(RuntimeBundle bundle, RuntimeBundle.Script script, ScriptKind kind, String inputJson,
                             ObjectNode ctx, Execution exec) {
        String code;
        try {
            code = linkedCode(bundle, script.code(), kind, script.moduleRefs(), "v" + script.versionId());
        } catch (ScriptLinker.LinkException e) {
            ScriptOutcome failed = new ScriptOutcome(null, new ScriptFailure(ScriptErrorCode.SCRIPT_RUNTIME_ERROR,
                    ScriptLinker.MODULE_NOT_FOUND + ": " + e.getMessage(), e.line(), e.col()), List.of(), 0, 0);
            record(script, failed, inputJson, exec);
            return failed;
        }
        putRuntime(ctx, exec);
        ScriptOutcome outcome = sandbox.run(kind.functionName(), code,
                "script-" + script.scriptId() + "-v" + script.versionNo() + ".js", inputJson, mapper.writeValueAsString(ctx),
                exec.now());
        record(script, outcome, inputJson, exec);
        return outcome;
    }

    /** 저장하지 않은 코드(테스트 실행·수식 미리 보기) */
    public ScriptOutcome runUnsaved(RuntimeBundle bundle, String code, ScriptKind kind, List<String> moduleRefs,
                                    String inputJson, ObjectNode ctx, Execution exec) {
        String linkedCode;
        try {
            linkedCode = ScriptLinker.needsLink(code, moduleRefs)
                    ? ScriptLinker.link(code, kind.functionName(), moduleRefs, bundle::module) : code;
        } catch (ScriptLinker.LinkException e) {
            return new ScriptOutcome(null, new ScriptFailure(ScriptErrorCode.SCRIPT_RUNTIME_ERROR,
                    ScriptLinker.MODULE_NOT_FOUND + ": " + e.getMessage(), e.line(), e.col()), List.of(), 0, 0);
        }
        putRuntime(ctx, exec);
        return sandbox.run(kind.functionName(), linkedCode, "script.js", inputJson, mapper.writeValueAsString(ctx), exec.now());
    }

    private static final java.util.regex.Pattern WINDOW_CALL = java.util.regex.Pattern.compile(
            "window\\(\\s*(['\"])([A-Za-z][A-Za-z0-9_]{0,63})\\1\\s*(?:,\\s*(['\"])(\\d+)([smh])\\3)?");

    /**
     * 코드가 {@code ctx.window('key', '10m')}로 읽는 측정 키와 창 길이(최대 24시간). 창 데이터는 이 키만 넘긴다(메시지마다 24시간치를
     * 모두 넘기지 않게). 키를 글자로 쓰지 않으면 빈 배열이 돌아간다.
     */
    public static Map<String, java.time.Duration> windowKeys(String code) {
        Map<String, java.time.Duration> out = new java.util.LinkedHashMap<>();
        if (code == null || !code.contains("window")) {
            return out;
        }
        java.util.regex.Matcher m = WINDOW_CALL.matcher(code);
        while (m.find()) {
            java.time.Duration d = java.time.Duration.ofHours(24);
            if (m.group(4) != null) {
                long n = Long.parseLong(m.group(4));
                d = switch (m.group(5)) {
                    case "s" -> java.time.Duration.ofSeconds(n);
                    case "m" -> java.time.Duration.ofMinutes(n);
                    default -> java.time.Duration.ofHours(n);
                };
            }
            out.merge(m.group(2), d, (a, b) -> a.compareTo(b) >= 0 ? a : b);
        }
        return out;
    }

    /** pipeline이 만든 프로그램(수식 묶음) 실행: 모듈·감싸개 없이, 창 데이터는 {@code ctx.runtime.series}로 직접 읽는다 */
    public ScriptOutcome runProgram(String code, String inputJson, ObjectNode ctx, Execution exec) {
        putRuntime(ctx, exec);
        return sandbox.run(ScriptKind.TRANSFORM.functionName(), code, "formula.js", inputJson, mapper.writeValueAsString(ctx),
                exec.now());
    }

    private String linkedCode(RuntimeBundle bundle, String code, ScriptKind kind, List<String> moduleRefs, String cacheKey) {
        if (!ScriptLinker.needsLink(code, moduleRefs)) {
            return code;
        }
        String key = cacheKey + ":" + code.hashCode() + ":" + code.length() + ":" + moduleRefs + ":"
                + bundle.modules().stream().map(RuntimeBundle.Module::ref).sorted().toList();
        String cached = linked.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        String result = ScriptLinker.link(code, kind.functionName(), moduleRefs, bundle::module);
        linked.put(key, result);
        return result;
    }

    private void putRuntime(ObjectNode ctx, Execution exec) {
        ObjectNode runtime = ctx.putObject(RUNTIME_KEY);
        runtime.put("now", (exec.windowAt() != null ? exec.windowAt() : exec.now()).toString());
        if (exec.window() != null && !exec.window().isEmpty()) {
            ObjectNode win = runtime.putObject("series");
            exec.window().forEach((key, points) -> {
                ArrayNode list = win.putArray(key);
                for (double[] p : points) {
                    list.addArray().add((long) p[0]).add(p[1]);
                }
            });
        }
    }

    private void record(RuntimeBundle.Script script, ScriptOutcome outcome, String inputJson, Execution exec) {
        if (exec.record() && ops != null) {
            ops.record(exec.organizationId(), script, outcome, inputJson.getBytes(StandardCharsets.UTF_8).length,
                    inputJson, exec.deviceId(), exec.rawMessageId(), exec.now());
        }
    }

    /**
     * @param organizationId 조직
     * @param deviceId       기기(없으면 null)
     * @param rawMessageId   원본(없으면 null)
     * @param now            처리 시각(ctx.util.now)·창 기준 시각
     * @param windowAt       {@code ctx.window} 기준 시각(측정 시각). 없으면 now
     * @param window         측정 키 → [[epochMillis, value]…](측정 시각 순, 창 기준 시각 이전 값만)
     * @param record         운영 지표·오류·로그에 남길지(테스트 실행은 false)
     */
    public record Execution(long organizationId, Long deviceId, Long rawMessageId, Instant now, Instant windowAt,
                            Map<String, List<double[]>> window, boolean record) {

        public static Execution test(long organizationId, Instant now) {
            return new Execution(organizationId, null, null, now, null, null, false);
        }
    }
}
