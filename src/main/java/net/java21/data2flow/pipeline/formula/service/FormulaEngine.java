package net.java21.data2flow.pipeline.formula.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.formula.domain.FormulaCompiler;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.service.ScriptRunner;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 수식 파생 항목 실행(SCR-01.06, BR-SCR-03·19). TRANSFORM(모델 → 기기) 다음, 검증 앞에서 기기에 연결된(모델·기기·공간) 수식을
 * 한 번의 샌드박스 실행으로 계산한다. 입력 측정 값이 없는 수식은 건너뛰고(그 메시지에 결과 없음), 계산 오류는 그 수식만 빠진다.
 * {@code rolling_*}은 기기별 최근 값(늦은 값 제외, 호스트가 {@code ctx.runtime.series}로 넘김)에 현재 값을 더해 계산하고, 창이 비면
 * 현재 값이다. 프로그램은 pipeline이 만들므로 모듈 연결 없이 바로 실행하고, 번들을 바꿀 때 미리 데워 둔다({@link #warm}).
 */
public class FormulaEngine {

    private static final Logger log = LoggerFactory.getLogger(FormulaEngine.class);

    private final ScriptRunner runner;
    private final JsonMapper mapper = MessageCodec.newMapper();
    private final Cache<String, Optional<FormulaCompiler.Compiled>> compiled = Caffeine.newBuilder().maximumSize(5_000).build();
    private final Cache<String, String> programs = Caffeine.newBuilder().maximumSize(2_000).build();

    public FormulaEngine(ScriptRunner runner) {
        this.runner = runner;
    }

    /** 이 기기에 적용할 수식(모델 → 기기 → 공간 순) */
    public static List<RuntimeBundle.Formula> applicable(RuntimeBundle bundle, Long modelId, long deviceId, Long spaceId) {
        List<RuntimeBundle.Formula> out = new ArrayList<>();
        for (String type : List.of("MODEL", "DEVICE", "SPACE")) {
            for (RuntimeBundle.Formula f : bundle.formulas()) {
                if (!type.equalsIgnoreCase(f.targetType()) || f.resultKey() == null) {
                    continue;
                }
                boolean match = switch (type) {
                    case "MODEL" -> modelId != null && f.targetId() == modelId;
                    case "DEVICE" -> f.targetId() == deviceId;
                    default -> spaceId != null && f.targetId() == spaceId;
                };
                if (match) {
                    out.add(f);
                }
            }
        }
        return out;
    }

    /** 창이 필요한 측정 키와 가장 긴 창 */
    public Map<String, Duration> windows(List<RuntimeBundle.Formula> formulas) {
        Map<String, Duration> out = new LinkedHashMap<>();
        for (RuntimeBundle.Formula f : formulas) {
            compiledOf(f).ifPresent(c -> c.windows().forEach((k, d) -> out.merge(k, d, (a, b) -> a.compareTo(b) >= 0 ? a : b)));
        }
        return out;
    }

    /**
     * @param metrics 지금까지의 측정 값(키 → 값)
     * @param exec    실행 정보(창 데이터 포함)
     * @return 계산한 파생 값(수식 순서)
     */
    public Result evaluate(RuntimeBundle bundle, List<RuntimeBundle.Formula> formulas, Map<String, Double> metrics,
                           ScriptRunner.Execution exec) {
        // 대상(모델·기기·공간)마다 프로그램 하나: 같은 묶음은 같은 코드라 예열한 그대로 쓴다
        Map<String, List<Item>> groups = new LinkedHashMap<>();
        for (RuntimeBundle.Formula f : formulas) {
            compiledOf(f).ifPresent(c -> groups.computeIfAbsent(f.targetType() + ":" + f.targetId(), k -> new ArrayList<>())
                    .add(new Item(f, c)));
        }
        List<Derived> derived = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        double duration = 0;
        for (List<Item> items : groups.values()) {
            Result r = evaluateGroup(items, metrics, exec);
            derived.addAll(r.derived());
            errors.addAll(r.errors());
            duration += r.durationMs();
        }
        return new Result(derived, errors, duration);
    }

    private Result evaluateGroup(List<Item> items, Map<String, Double> metrics, ScriptRunner.Execution exec) {
        List<RuntimeBundle.Formula> formulas = items.stream().map(Item::formula).toList();
        String program = program(items);
        ObjectNode msg = mapper.createObjectNode();
        ArrayNode list = msg.putArray("metrics");
        metrics.forEach((k, v) -> list.addObject().put("key", k).put("value", v));
        ScriptOutcome outcome = runner.runProgram(program, mapper.writeValueAsString(msg), mapper.createObjectNode(), exec);
        List<Derived> derived = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        if (!outcome.ok()) {
            errors.add(outcome.failure().code().name() + ": " + outcome.failure().message());
            return new Result(derived, errors, outcome.durationMs());
        }
        Map<Long, RuntimeBundle.Formula> byId = new LinkedHashMap<>();
        formulas.forEach(f -> byId.put(f.id(), f));
        for (JsonNode r : outcome.output()) {
            RuntimeBundle.Formula f = byId.get(r.path("id").asLong());
            if (f == null) {
                continue;
            }
            if (r.has("error")) {
                errors.add(f.resultKey() + ": " + r.get("error").asString());
            } else if (r.path("value").isNumber()) {
                derived.add(new Derived(f.id(), f.resultKey(), r.get("value").asDouble(), f.unit()));
            }
        }
        return new Result(derived, errors, outcome.durationMs());
    }

    /**
     * 예열(SCR-03.04 "컴파일·예열 뒤 원자 교체"): 대상(모델·기기·공간)마다 그 수식 묶음 프로그램을 시험 값으로 몇 번 돌려 둔다.
     * 처음 보는 코드는 해석 실행이 덜 데워져 첫 메시지가 50ms를 넘을 수 있기 때문이다.
     */
    public void warm(RuntimeBundle bundle, int rounds, java.time.Instant now) {
        Map<String, List<RuntimeBundle.Formula>> groups = new LinkedHashMap<>();
        for (RuntimeBundle.Formula f : bundle.formulas()) {
            groups.computeIfAbsent(f.targetType() + ":" + f.targetId(), k -> new ArrayList<>()).add(f);
        }
        for (List<RuntimeBundle.Formula> group : groups.values()) {
            Map<String, Double> sample = new LinkedHashMap<>();
            for (RuntimeBundle.Formula f : group) {
                compiledOf(f).ifPresent(c -> c.inputs().forEach(k -> sample.put(k, 1.0)));
            }
            for (int i = 0; i < rounds; i++) {
                evaluate(bundle, group, sample, ScriptRunner.Execution.test(0, now));
            }
        }
    }

    Optional<FormulaCompiler.Compiled> compiledOf(RuntimeBundle.Formula f) {
        String key = f.id() + "|" + f.expression() + "|" + f.compiledJs();
        return compiled.get(key, k -> {
            if (f.expression() != null && !f.expression().isBlank()) {
                try {
                    return Optional.of(FormulaCompiler.compile(f.expression(), m -> true));
                } catch (FormulaCompiler.FormulaException e) {
                    log.warn("수식 {}({})을(를) 읽지 못해 건너뜁니다: {}", f.id(), f.resultKey(), e.getMessage());
                    return Optional.empty();
                }
            }
            if (f.compiledJs() != null && !f.compiledJs().isBlank()) {
                return Optional.of(new FormulaCompiler.Compiled(null, f.compiledJs(), List.of(), Map.of()));
            }
            return Optional.empty();
        });
    }

    private String program(List<Item> items) {
        String key = items.stream().map(i -> i.formula().id() + ":" + i.compiled().js()).reduce("", (a, b) -> a + "\n" + b);
        return programs.get(key, k -> build(items));
    }

    /** 수식 묶음 → TRANSFORM 함수 하나 */
    static String build(List<Item> items) {
        StringBuilder js = new StringBuilder("""
                function transform(msg, ctx) {
                  const U = ctx.util;
                  const __v = {};
                  const ms = msg.metrics || [];
                  for (let i = 0; i < ms.length; i++) { __v[ms[i].key] = ms[i].value; }
                  const __missing = {missing: true};
                  const __m = function (k) { const x = __v[k]; if (typeof x !== 'number') { throw __missing; } return x; };
                  const __rt = ctx.runtime || {};
                  const __series = __rt.series || {};
                  const __now = __rt.now ? Date.parse(__rt.now) : 0;
                  const __roll = function (k, ms, fn) {
                    const pts = __series[k] || [];
                    const w = [];
                    for (let i = 0; i < pts.length; i++) {
                      if (pts[i][0] >= __now - ms && pts[i][0] < __now) { w.push(pts[i][1]); }
                    }
                    const cur = __v[k];
                    if (typeof cur === 'number') { w.push(cur); }
                    if (w.length === 0) { throw __missing; }
                    if (fn === 'count') { return w.length; }
                    if (fn === 'min') { return Math.min.apply(null, w); }
                    if (fn === 'max') { return Math.max.apply(null, w); }
                    let s = 0;
                    for (let i = 0; i < w.length; i++) { s += w[i]; }
                    return fn === 'sum' ? s : s / w.length;
                  };
                  const __out = [];
                  const __f = [
                """);
        for (Item i : items) {
            js.append("    [").append(i.formula().id()).append(", function () { return (").append(i.compiled().js())
                    .append("); }],\n");
        }
        js.append("""
                  ];
                  for (let i = 0; i < __f.length; i++) {
                    try {
                      const r = __f[i][1]();
                      if (typeof r === 'number' && isFinite(r)) { __out.push({id: __f[i][0], value: r}); }
                      else { __out.push({id: __f[i][0], error: '결과가 유한한 숫자가 아닙니다'}); }
                    } catch (e) {
                      if (e !== __missing) { __out.push({id: __f[i][0], error: String(e && e.message ? e.message : e)}); }
                    }
                  }
                  return __out;
                }
                """);
        return js.toString();
    }

    /** 창 데이터 키 목록 */
    public static Set<String> keys(Map<String, Duration> windows) {
        return new LinkedHashSet<>(windows.keySet());
    }

    /** 결과 정렬용 */
    static final Comparator<Derived> BY_KEY = Comparator.comparing(Derived::key);

    record Item(RuntimeBundle.Formula formula, FormulaCompiler.Compiled compiled) {
    }

    /** 파생 값 하나 */
    public record Derived(long formulaId, String key, double value, String unit) {
    }

    /**
     * @param derived    계산한 값
     * @param errors     빠진 수식과 이유
     * @param durationMs 실행 시간
     */
    public record Result(List<Derived> derived, List<String> errors, double durationMs) {
    }
}
