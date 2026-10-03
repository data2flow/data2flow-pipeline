package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.script.domain.ScriptErrorCode;
import net.java21.data2flow.pipeline.script.domain.ScriptFailure;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.domain.ScriptLimits;
import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.EnvironmentAccess;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.ResourceLimits;
import org.graalvm.polyglot.SandboxPolicy;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.SourceSection;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.IOAccess;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 사용자 JavaScript 샌드박스(SCR-02.01·02.02, ADR-008, design/flow-engine-and-live-reload.md §5).
 *
 * <ul>
 *   <li>엔진: GraalJS 커뮤니티판(polyglot + js-community, UPL). 인스턴스당 {@link Engine} 하나를 공유해 파싱 결과를 캐시한다.</li>
 *   <li>격리: <b>실행마다 새 {@link Context}</b>. 호스트 객체 매핑·클래스 조회·클래스 로딩·입출력·스레드·프로세스·네이티브·환경 변수·
 *       다른 언어 접근을 모두 끈다. {@code SandboxPolicy.CONSTRAINED}가 요구하는 제한을 전부 명시적으로 걸고, CONSTRAINED가 허용하지
 *       않는 길이 제한 옵션(문자열·타입 배열·apply 인자)을 더하기 위해 정책 이름은 TRUSTED로 둔다(문서 보강: flow-engine-and-live-reload.md §5).</li>
 *   <li>전역: {@code eval}·{@code Function}(동적 코드 생성) 금지, {@code load}·{@code print}·{@code Graal}·{@code Polyglot}·
 *       {@code Java}·{@code Packages} 등은 머리말(prelude.js)이 막는다.</li>
 *   <li>제한: CPU 50ms 워치독({@link ScriptWatchdog}, {@code context.close(true)}), 문장 수 한도, 출력 64KB, 로그 1KB×N,
 *       문자열·배열 길이(메모리 간접 제한).</li>
 *   <li>입출력: 입력은 JSON 문자열을 안에서 {@code JSON.parse}, 출력은 호스트가 값 트리를 직접 옮긴다(호스트 객체 노출 없음).</li>
 * </ul>
 * 스레드 안전하다. 여러 스레드가 동시에 실행해도 각자 새 Context를 쓴다.
 */
public class ScriptSandbox implements AutoCloseable {

    /** 사용자 코드 앞에 붙이는 엄격 모드 선언. 같은 줄에 붙여 줄 번호는 그대로 두고 1번 줄의 열만 보정한다 */
    private static final String STRICT_PREFIX = "'use strict';";
    private static final String FORBIDDEN_MARK = "__D2F_FORBIDDEN__:";
    private static final String ENTRY_MISSING_MARK = "__D2F_ENTRY_MISSING__:";
    /** 모듈 적재 구문(import, 동적 import()). 문법이라 머리말로 막을 수 없으므로 실행 전에 거부한다(BR-SCR-05와 같은 규칙) */
    private static final java.util.regex.Pattern MODULE_SYNTAX =
            java.util.regex.Pattern.compile("(?m)(\\bimport\\s*\\(|^\\s*import\\s|^\\s*export\\s)");

    private final Engine engine;
    private final Source prelude;
    private final ScriptLimits limits;
    private final ScriptWatchdog watchdog;
    private final ResourceLimits resourceLimits;
    private final JsonMapper mapper = MessageCodec.newMapper();
    private final RunHistory history = new RunHistory(4096);

    public ScriptSandbox(ScriptLimits limits) {
        if (System.getProperty("polyglotimpl.AttachLibraryFailureAction") == null) {
            System.setProperty("polyglotimpl.AttachLibraryFailureAction", "ignore");
        }
        this.limits = limits;
        this.engine = Engine.newBuilder("js")
                .out(OutputStream.nullOutputStream())
                .err(OutputStream.nullOutputStream())
                .option("engine.WarnInterpreterOnly", "false")
                .build();
        this.prelude = Source.newBuilder("js", loadResource("/script/prelude.js"), "d2f-prelude.js")
                .cached(true).buildLiteral();
        this.resourceLimits = ResourceLimits.newBuilder().statementLimit(limits.statementLimit(), null).build();
        this.watchdog = new ScriptWatchdog();
    }

    public ScriptLimits limits() {
        return limits;
    }

    /**
     * 스크립트 한 번 실행. 예외를 던지지 않고 결과에 실패를 담는다.
     *
     * @param kind       DECODE(진입 함수 {@code decode}) 또는 TRANSFORM({@code transform})
     * @param code       사용자 코드
     * @param sourceName 오류 위치 표시용 이름(예: {@code script-42-v3.js})
     * @param inputJson  첫 번째 인자(JSON)
     * @param ctxJson    두 번째 인자 ctx의 데이터 부분(JSON): config, device, last, source
     * @param now        {@code ctx.util.now()}가 돌려줄 처리 시각
     */
    public ScriptOutcome run(ScriptKind kind, String code, String sourceName, String inputJson, String ctxJson, Instant now) {
        ScriptOutcome first = runOnce(kind, code, sourceName, inputJson, ctxJson, now);
        String key = code == null ? "" : code.length() + ":" + code.hashCode();
        if (isTimeout(first) && history.shouldRetry(key)) {
            // 해석 실행(JIT 없음)에서 처음 쓰는 언어 기능의 초기화 비용이나 GC·페이지 할당 지연으로 정상 스크립트가 한도를 넘을 수 있다.
            // 처음 시간 초과이거나 전에 성공한 코드면 한 번만 다시 실행한다(스크립트는 부수 효과가 없어 다시 실행해도 안전).
            ScriptOutcome second = runOnce(kind, code, sourceName, inputJson, ctxJson, now);
            second = new ScriptOutcome(second.output(), second.failure(), second.logs(),
                    first.durationMs() + second.durationMs(), second.outputBytes());
            history.record(key, !isTimeout(second));
            return second;
        }
        history.record(key, !isTimeout(first));
        return first;
    }

    private static boolean isTimeout(ScriptOutcome outcome) {
        return !outcome.ok() && outcome.failure().code() == ScriptErrorCode.SCRIPT_TIMEOUT
                && outcome.failure().message().contains("[cpu");
    }

    private ScriptOutcome runOnce(ScriptKind kind, String code, String sourceName, String inputJson, String ctxJson,
                                  Instant now) {
        long started = System.nanoTime();
        if (code == null || code.getBytes(StandardCharsets.UTF_8).length > limits.maxCodeBytes()) {
            return failed(ScriptFailure.of(ScriptErrorCode.SCRIPT_RUNTIME_ERROR,
                    "코드가 너무 큽니다(최대 " + limits.maxCodeBytes() + "바이트)"), List.of(), started);
        }
        java.util.regex.Matcher module = MODULE_SYNTAX.matcher(code);
        if (module.find()) {
            int line = (int) code.substring(0, module.start()).chars().filter(c -> c == '\n').count() + 1;
            return failed(new ScriptFailure(ScriptErrorCode.SCRIPT_FORBIDDEN_API, "사용할 수 없는 기능입니다: import(모듈 적재)",
                    line, null), List.of(), started);
        }
        Context context = newContext();
        ScriptWatchdog.Watch watch = null;
        Value logsFn = null;
        try {
            Value api = context.eval(prelude)
                    .execute(limits.maxLogBytes(), limits.maxLogEntries(), limits.maxArrayLength());
            Value invoke = api.getMember("invoke");
            logsFn = api.getMember("logs");
            Source user = Source.newBuilder("js", STRICT_PREFIX + code, sourceName).cached(true).buildLiteral();
            Value program = context.parse(user);
            watch = watchdog.start(context, limits.cpuTime(), limits.wallTime());
            program.execute();
            Value result = invoke.execute(kind.functionName(), inputJson, ctxJson, now.toString());
            JsonNode output = new GuestValueConverter(limits.maxOutputDepth(), limits.maxOutputBytes()).convert(result);
            watch.close();
            int bytes = mapper.writeValueAsBytes(output).length;
            List<String> logs = readLogs(logsFn);
            if (bytes > limits.maxOutputBytes()) {
                return failed(ScriptFailure.of(ScriptErrorCode.SCRIPT_OUTPUT_INVALID,
                        "반환값이 " + limits.maxOutputBytes() + "바이트를 넘습니다(" + bytes + "바이트)"), logs, started);
            }
            return new ScriptOutcome(output, null, logs, elapsedMs(started), bytes);
        } catch (GuestValueConverter.PromiseOutputException e) {
            return failed(ScriptFailure.of(ScriptErrorCode.SCRIPT_FORBIDDEN_API, e.getMessage()), safeLogs(logsFn, watch), started);
        } catch (GuestValueConverter.OutputInvalidException e) {
            return failed(ScriptFailure.of(ScriptErrorCode.SCRIPT_OUTPUT_INVALID, e.getMessage()), safeLogs(logsFn, watch), started);
        } catch (PolyglotException e) {
            return failed(classify(e, watch, sourceName), safeLogs(logsFn, watch), started);
        } catch (IllegalStateException e) {
            // 워치독이 Context를 닫은 직후의 호출
            boolean timedOut = watch != null && watch.timedOut();
            return failed(timedOut ? timeout(watch) : ScriptFailure.of(ScriptErrorCode.SCRIPT_RUNTIME_ERROR, e.getMessage()),
                    List.of(), started);
        } catch (StackOverflowError | OutOfMemoryError e) {
            return failed(ScriptFailure.of(ScriptErrorCode.SCRIPT_RUNTIME_ERROR, "실행 자원이 부족합니다: " + e), List.of(),
                    started);
        } finally {
            if (watch != null) {
                watch.close();
            }
            try {
                context.close(true);
            } catch (RuntimeException ignored) {
                // 워치독이 이미 닫음
            }
        }
    }

    /** 문법만 검사한다(실행하지 않음). 문법 오류면 실패 정보, 아니면 null */
    public ScriptFailure syntaxCheck(String code, String sourceName) {
        Context context = newContext();
        try {
            context.parse(Source.newBuilder("js", STRICT_PREFIX + code, sourceName).buildLiteral());
            return null;
        } catch (PolyglotException e) {
            return classify(e, null, sourceName);
        } finally {
            context.close(true);
        }
    }

    /**
     * 처음 실행이 느린 문제(클래스 적재·내장 함수 초기화)를 시작할 때 미리 치른다(readiness 예열, reliability-and-ha.md §4.2).
     * 해석 실행(JIT 없음)에서는 처음 쓰는 내장 함수 경로가 수십 ms 걸려 정상 스크립트가 시간 초과로 보일 수 있으므로, 자주 쓰는
     * 내장 함수와 오류 경로를 모두 한 번씩 지난다.
     */
    public void warmUp(int rounds) {
        String decode = loadResource("/script/warmup-decode.js");
        String transform = loadResource("/script/warmup-transform.js");
        String[] errors = {
                "function transform(msg, ctx) { throw new Error('warm'); }",
                "function transform(msg, ctx) { return fetch('x'); }",
                "function transform(msg, ctx) { return eval('1'); }",
                "function transform(msg, ctx) { return import('x'); }",
                "function transform(msg, ctx) { return function () {}; }",
                "function transform(msg, ctx) { const a = {}; a.self = a; return a; }",
                "function transform(msg, ctx) { null.x; }",
                "function transform(msg, ctx) { return (function f() { return f(); })(); }",
                "function transform(msg, ctx) { return 'x'.repeat(2 ** 28); }",
                "function transform(msg, ctx) { return new Float64Array(2 ** 28); }",
                "function transform(msg, ctx) { return Array.from({length: 1e9}); }",
                "function transform(msg, ctx) {",
                "function decode(input, ctx) { return {}; }"};
        String input = "{\"v\":1,\"deviceId\":1,\"measuredAt\":\"2026-01-01T00:00:00Z\","
                + "\"metrics\":[{\"key\":\"temperature\",\"value\":22.0,\"unit\":\"C\",\"quality\":0},"
                + "{\"key\":\"humidity\",\"value\":40,\"quality\":0}]}";
        String ctx = "{\"config\":{\"offset\":0.5},\"device\":{\"id\":1,\"attributes\":{\"tempOffset\":0.1}},"
                + "\"last\":{\"temperature\":{\"value\":21.5,\"measuredAt\":\"2026-01-01T00:00:00Z\"}}}";
        for (int i = 0; i < rounds; i++) {
            run(ScriptKind.DECODE, decode, "warm-decode.js",
                    "{\"topic\":\"t\",\"payload\":{\"a\":1},\"payloadBase64\":\"AXVkA2cQAQRoeAV9GgQ=\","
                            + "\"payloadEncoding\":\"JSON\",\"receivedAt\":\"2026-01-01T00:00:00Z\"}", ctx, Instant.EPOCH);
            run(ScriptKind.TRANSFORM, transform, "warm-transform.js", input, ctx, Instant.EPOCH);
            for (String error : errors) {
                run(ScriptKind.TRANSFORM, error, "warm-error.js", input, ctx, Instant.EPOCH);
            }
        }
    }

    private Context newContext() {
        return Context.newBuilder("js")
                .engine(engine)
                .sandbox(SandboxPolicy.TRUSTED)
                .out(OutputStream.nullOutputStream())
                .err(OutputStream.nullOutputStream())
                .in(InputStream.nullInputStream())
                .allowHostAccess(HostAccess.newBuilder().allowMutableTargetMappings().build())
                .allowHostClassLookup(className -> false)
                .allowHostClassLoading(false)
                .allowIO(IOAccess.NONE)
                .allowCreateThread(false)
                .allowCreateProcess(false)
                .allowNativeAccess(false)
                .allowEnvironmentAccess(EnvironmentAccess.NONE)
                .allowPolyglotAccess(PolyglotAccess.NONE)
                .allowValueSharing(false)
                .allowInnerContextOptions(false)
                .allowExperimentalOptions(true)
                .option("js.allow-eval", "false")
                .option("js.console", "false")
                .option("js.load", "false")
                .option("js.print", "false")
                .option("js.graal-builtin", "false")
                .option("js.polyglot-builtin", "false")
                .option("js.java-package-globals", "false")
                .option("js.string-length-limit", Integer.toString(limits.maxStringLength()))
                .option("js.max-typed-array-length", Integer.toString(limits.maxArrayLength()))
                .option("js.max-apply-argument-length", "65536")
                .resourceLimits(resourceLimits)
                .build();
    }

    private ScriptFailure classify(PolyglotException e, ScriptWatchdog.Watch watch, String sourceName) {
        if ((watch != null && watch.timedOut()) || e.isCancelled()) {
            return timeout(watch);
        }
        String message = e.getMessage() == null ? "" : e.getMessage();
        if (e.isResourceExhausted()) {
            if (message.contains("Statement count limit")) {
                return ScriptFailure.of(ScriptErrorCode.SCRIPT_TIMEOUT,
                        "실행 문장 수 한도(" + limits.statementLimit() + ")를 넘었습니다");
            }
            return ScriptFailure.of(ScriptErrorCode.SCRIPT_RUNTIME_ERROR, "실행 자원(메모리)이 부족합니다: " + message);
        }
        Integer[] location = location(e, sourceName);
        int forbidden = message.indexOf(FORBIDDEN_MARK);
        if (forbidden >= 0) {
            String name = message.substring(forbidden + FORBIDDEN_MARK.length()).trim();
            return new ScriptFailure(ScriptErrorCode.SCRIPT_FORBIDDEN_API, "사용할 수 없는 기능입니다: " + name,
                    location[0], location[1]);
        }
        if (message.startsWith("EvalError") || message.contains("Access to host") || message.contains("is not allowed")) {
            return new ScriptFailure(ScriptErrorCode.SCRIPT_FORBIDDEN_API,
                    "사용할 수 없는 기능입니다(동적 코드 생성·호스트 접근): " + ScriptFailure.truncate(message), location[0],
                    location[1]);
        }
        int missing = message.indexOf(ENTRY_MISSING_MARK);
        if (missing >= 0) {
            String fn = message.substring(missing + ENTRY_MISSING_MARK.length()).trim();
            return ScriptFailure.of(ScriptErrorCode.SCRIPT_RUNTIME_ERROR, "진입 함수 " + fn + "(input, ctx)가 없습니다");
        }
        if (e.isHostException()) {
            return ScriptFailure.of(ScriptErrorCode.SCRIPT_RUNTIME_ERROR, "실행 환경 오류: " + e.asHostException());
        }
        return new ScriptFailure(ScriptErrorCode.SCRIPT_RUNTIME_ERROR, ScriptFailure.truncate(message), location[0],
                location[1]);
    }

    private static Integer[] location(PolyglotException e, String sourceName) {
        SourceSection found = null;
        for (PolyglotException.StackFrame frame : e.getPolyglotStackTrace()) {
            SourceSection section = frame.getSourceLocation();
            if (section != null && section.isAvailable() && sourceName.equals(section.getSource().getName())) {
                found = section;
                break;
            }
        }
        if (found == null) {
            SourceSection section = e.getSourceLocation();
            if (section != null && section.isAvailable() && sourceName.equals(section.getSource().getName())) {
                found = section;
            }
        }
        if (found == null) {
            return new Integer[]{null, null};
        }
        int line = found.getStartLine();
        int col = found.getStartColumn();
        if (line == 1) {
            col = Math.max(1, col - STRICT_PREFIX.length());
        }
        return new Integer[]{line, col};
    }

    private ScriptFailure timeout(ScriptWatchdog.Watch watch) {
        String reason = watch == null || watch.reason() == null ? "" : " [" + watch.reason() + "]";
        return ScriptFailure.of(ScriptErrorCode.SCRIPT_TIMEOUT,
                "실행 시간 제한(" + limits.cpuTime().toMillis() + "ms)을 넘었습니다" + reason);
    }

    private List<String> safeLogs(Value logsFn, ScriptWatchdog.Watch watch) {
        if (logsFn == null || (watch != null && watch.timedOut())) {
            return List.of();
        }
        try {
            return readLogs(logsFn);
        } catch (RuntimeException e) {
            return List.of();
        }
    }

    private static List<String> readLogs(Value logsFn) {
        Value array = logsFn.execute();
        List<String> logs = new ArrayList<>((int) array.getArraySize());
        for (long i = 0; i < array.getArraySize(); i++) {
            logs.add(array.getArrayElement(i).asString());
        }
        return logs;
    }

    private static ScriptOutcome failed(ScriptFailure failure, List<String> logs, long started) {
        return new ScriptOutcome(null, failure, logs, elapsedMs(started), 0);
    }

    private static double elapsedMs(long started) {
        return Math.round((System.nanoTime() - started) / 1_000.0) / 1_000.0;
    }

    private static String loadResource(String path) {
        try (InputStream in = ScriptSandbox.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("리소스가 없습니다: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 코드별 최근 결과(성공 또는 시간 초과). 시간 초과 재시도 판단에만 쓴다 */
    private static final class RunHistory {
        private final java.util.Map<String, Boolean> results;

        RunHistory(int capacity) {
            this.results = java.util.Collections.synchronizedMap(new java.util.LinkedHashMap<>(256, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(java.util.Map.Entry<String, Boolean> eldest) {
                    return size() > capacity;
                }
            });
        }

        /** 처음 시간 초과(기록 없음)이거나 전에 성공한 코드면 재시도 */
        boolean shouldRetry(String key) {
            Boolean last = results.get(key);
            return last == null || last;
        }

        void record(String key, boolean ok) {
            results.put(key, ok);
        }
    }

    @Override
    public void close() {
        watchdog.close();
        engine.close(true);
    }
}
