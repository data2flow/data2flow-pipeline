package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 공유 모듈 연결과 ctx 확장(SCR-04.01, BR-SCR-13·19). 샌드박스(공용 모듈)는 {@code import}를 막고 {@code ctx.modules}·
 * {@code ctx.window}를 비워 두므로, pipeline이 사용자 코드를 실행하기 전에 다음처럼 고친다.
 *
 * <ol>
 *   <li>{@code import { parseChannels } from 'module:milesight@1';}를 <b>같은 줄에서</b>
 *       {@code const { parseChannels } = __d2f_mod("milesight@1");}로 바꾼다(줄 번호가 그대로라 오류 위치가 맞다).
 *       버전을 지정해야 하고({@code name@version}), {@code module:}이 아닌 가져오기(외부 패키지)는 거부한다.</li>
 *   <li>사용자 코드 <b>뒤</b>에 모듈 공장 함수(함수 선언이라 끌어올려짐)와 진입 함수 감싸개를 붙인다. 감싸개는 ctx를 복사해
 *       {@code modules}(가져온 모듈 + 스크립트의 moduleRefs)와 {@code window(key, duration)}(호스트가 {@code ctx.runtime.series}로
 *       넘긴 최근 값)을 채운 뒤 사용자 함수를 부른다. {@code ctx.runtime}은 사용자에게 보이지 않는다.</li>
 * </ol>
 * 모듈 코드는 같은 샌드박스 규칙을 따르고({@code export function}·{@code export const}·{@code exports.x}·{@code module.exports}),
 * 모듈끼리의 가져오기도 같은 방식으로 연결하며 순환은 거부한다. 모듈 버전은 불변이므로 연결 결과는 (코드, 모듈 버전 집합)으로 캐시한다.
 */
public final class ScriptLinker {

    /** 모듈을 찾지 못함·외부 패키지·순환(검사 오류 코드) */
    public static final String MODULE_NOT_FOUND = "SCRIPT_MODULE_NOT_FOUND";

    private static final Pattern IMPORT = Pattern.compile(
            "(?m)^([ \\t]*)import\\s+(?:(\\{[^}]*\\})|\\*\\s+as\\s+([A-Za-z_$][\\w$]*)|([A-Za-z_$][\\w$]*))\\s+from\\s+(['\"])([^'\"]+)\\5\\s*;?");
    private static final Pattern MODULE_SPEC = Pattern.compile("module:([a-z0-9-]{3,40})@(\\d+)");
    private static final Pattern EXPORT_DECL = Pattern.compile(
            "(?m)^([ \\t]*)export\\s+(function\\s*\\*?\\s*|const\\s+|let\\s+|var\\s+|class\\s+)([A-Za-z_$][\\w$]*)");
    private static final Pattern EXPORT_DEFAULT = Pattern.compile("(?m)^([ \\t]*)export\\s+default\\s+");
    private static final Pattern EXPORT_LIST = Pattern.compile("(?m)^([ \\t]*)export\\s*\\{([^}]*)\\}\\s*;?");

    private ScriptLinker() {
    }

    /** 고칠 것이 있는 코드인가(없으면 원래 코드를 그대로 실행해 비용을 아낀다) */
    public static boolean needsLink(String code, List<String> moduleRefs) {
        return code != null && (!moduleRefs.isEmpty() || code.contains("import") || code.contains("window")
                || code.contains("modules"));
    }

    /**
     * @param code        사용자 코드
     * @param entry       진입 함수 이름(decode·transform)
     * @param moduleRefs  스크립트 버전에 기록된 모듈 {@code name@version}(ctx.modules로도 노출)
     * @param modules     {@code name@version} → 모듈(번들)
     * @return 실행할 코드
     * @throws LinkException 모듈 없음·외부 패키지·순환
     */
    public static String link(String code, String entry, List<String> moduleRefs,
                              Function<String, Optional<RuntimeBundle.Module>> modules) {
        Map<String, Integer> factories = new LinkedHashMap<>();
        List<String> factoryCode = new ArrayList<>();
        String body = rewriteImports(code, modules, factories, factoryCode, new LinkedHashSet<>(), null);
        for (String ref : moduleRefs) {
            resolve(ref, modules, factories, factoryCode, new LinkedHashSet<>(), 1, 1);
        }
        StringBuilder out = new StringBuilder(body);
        out.append("\n;function __d2f_mod(ref) {\n  switch (ref) {\n");
        factories.forEach((ref, i) -> out.append("    case ").append(quote(ref)).append(": return __d2f_m").append(i)
                .append("();\n"));
        out.append("    default: throw new Error('").append(MODULE_NOT_FOUND).append(": ' + ref);\n  }\n}\n");
        for (String f : factoryCode) {
            out.append(f);
        }
        List<String> exposed = new ArrayList<>(new LinkedHashSet<>(factories.keySet()));
        out.append(wrapper(entry, exposed));
        return out.toString();
    }

    private static String rewriteImports(String code, Function<String, Optional<RuntimeBundle.Module>> modules,
                                         Map<String, Integer> factories, List<String> factoryCode, Set<String> stack,
                                         String inModule) {
        Matcher m = IMPORT.matcher(code);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String spec = m.group(6);
            int line = lineOf(code, m.start());
            Matcher ms = MODULE_SPEC.matcher(spec);
            if (!ms.matches()) {
                throw new LinkException(inModule == null
                        ? "외부 패키지·버전 없는 모듈은 쓸 수 없습니다(module:이름@버전): " + spec
                        : "모듈 " + inModule + "이(가) 쓸 수 없는 가져오기를 합니다: " + spec, line, m.start(6) - lineStart(code, m.start()));
            }
            String ref = ms.group(1) + "@" + ms.group(2);
            resolve(ref, modules, factories, factoryCode, stack, line, m.start(6) - lineStart(code, m.start()) + 1);
            String target;
            if (m.group(2) != null) {
                target = m.group(2).replaceAll("\\s+as\\s+", ": ");
            } else if (m.group(3) != null) {
                target = m.group(3);
            } else {
                target = "{ default: " + m.group(4) + " }";
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(m.group(1) + "const " + target + " = __d2f_mod(" + quote(ref) + ");"));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static void resolve(String ref, Function<String, Optional<RuntimeBundle.Module>> modules,
                                Map<String, Integer> factories, List<String> factoryCode, Set<String> stack, int line, int col) {
        if (!MODULE_SPEC.matcher("module:" + ref).matches()) {
            throw new LinkException("모듈은 이름@버전으로 지정해야 합니다: " + ref, line, col);
        }
        if (stack.contains(ref)) {
            throw new LinkException("모듈 가져오기가 순환합니다: " + String.join(" → ", stack) + " → " + ref, line, col);
        }
        if (factories.containsKey(ref)) {
            return;
        }
        RuntimeBundle.Module module = modules.apply(ref)
                .orElseThrow(() -> new LinkException("모듈을 찾을 수 없습니다: " + ref, line, col));
        stack.add(ref);
        String moduleCode = rewriteImports(module.code() == null ? "" : module.code(), modules, factories, factoryCode, stack, ref);
        stack.remove(ref);
        int index = factories.size();
        factories.put(ref, index);
        factoryCode.add("function __d2f_m" + index + "() {\n  if (__d2f_m" + index + ".v === undefined) {\n    __d2f_m" + index
                + ".v = (function () {\n      const exports = {};\n      const module = {exports: exports};\n"
                + exportsOf(moduleCode) + "\n      return Object.freeze(module.exports);\n    })();\n  }\n  return __d2f_m"
                + index + ".v;\n}\n");
    }

    /** {@code export} 문법을 {@code exports.x = x}로 바꾼다 */
    static String exportsOf(String code) {
        List<String> names = new ArrayList<>();
        Matcher decl = EXPORT_DECL.matcher(code);
        StringBuilder sb = new StringBuilder();
        while (decl.find()) {
            names.add(decl.group(3));
            decl.appendReplacement(sb, Matcher.quoteReplacement(decl.group(1) + decl.group(2) + decl.group(3)));
        }
        decl.appendTail(sb);
        String out = EXPORT_DEFAULT.matcher(sb.toString()).replaceAll("$1exports.default = ");
        Matcher list = EXPORT_LIST.matcher(out);
        StringBuilder sb2 = new StringBuilder();
        while (list.find()) {
            StringBuilder assigns = new StringBuilder(list.group(1));
            for (String part : list.group(2).split(",")) {
                String p = part.trim();
                if (p.isEmpty()) {
                    continue;
                }
                String[] as = p.split("\\s+as\\s+");
                assigns.append("exports.").append(as.length > 1 ? as[1].trim() : as[0]).append(" = ").append(as[0].trim())
                        .append("; ");
            }
            list.appendReplacement(sb2, Matcher.quoteReplacement(assigns.toString()));
        }
        list.appendTail(sb2);
        StringBuilder tail = new StringBuilder(sb2);
        for (String n : names) {
            tail.append("\nexports.").append(n).append(" = ").append(n).append(";");
        }
        return tail.toString();
    }

    /** 진입 함수 감싸개: ctx 복사 + modules + window(BR-SCR-19: 1분~24시간, 최대 1440개) */
    private static String wrapper(String entry, List<String> refs) {
        StringBuilder names = new StringBuilder("[");
        for (int i = 0; i < refs.size(); i++) {
            names.append(i > 0 ? ", " : "").append(quote(refs.get(i)));
        }
        names.append(']');
        return """
                ;(function () {
                  const __user = typeof %1$s === 'function' ? %1$s : null;
                  if (__user === null) { return; }
                  const __refs = %2$s;
                  const __dur = function (d) {
                    const m = /^(\\d+)(s|m|h)$/.exec(String(d));
                    if (!m) { throw new RangeError('창 길이 형식이 아닙니다(예: 10m): ' + d); }
                    const ms = Number(m[1]) * (m[2] === 's' ? 1000 : m[2] === 'm' ? 60000 : 3600000);
                    if (ms <= 0 || ms > 86400000) { throw new RangeError('창 길이는 24시간 이하입니다: ' + d); }
                    return ms;
                  };
                  globalThis.%1$s = function (input, ctx) {
                    const rt = ctx.runtime || {};
                    const c = {};
                    const ks = Object.keys(ctx);
                    for (let i = 0; i < ks.length; i++) { if (ks[i] !== 'runtime') { c[ks[i]] = ctx[ks[i]]; } }
                    const mods = {};
                    for (let i = 0; i < __refs.length; i++) { mods[__refs[i]] = __d2f_mod(__refs[i]); }
                    c.modules = Object.freeze(mods);
                    const win = rt.series || {};
                    const nowMs = rt.now ? Date.parse(rt.now) : 0;
                    c.window = function (key, duration) {
                      const from = nowMs - __dur(duration);
                      const list = win[key] || [];
                      const out = [];
                      for (let i = 0; i < list.length && out.length < 1440; i++) {
                        if (list[i][0] >= from && list[i][0] < nowMs) { out.push(list[i][1]); }
                      }
                      return out;
                    };
                    return __user(input, Object.freeze(c));
                  };
                })();
                """.formatted(entry, names);
    }

    private static int lineOf(String code, int index) {
        int line = 1;
        for (int i = 0; i < index; i++) {
            if (code.charAt(i) == '\n') {
                line++;
            }
        }
        return line;
    }

    private static int lineStart(String code, int index) {
        int i = index;
        while (i > 0 && code.charAt(i - 1) != '\n') {
            i--;
        }
        return i;
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /** 연결 오류(정적 검사 오류 {@code SCRIPT_MODULE_NOT_FOUND}) */
    public static class LinkException extends RuntimeException {
        private final int line;
        private final int col;

        public LinkException(String message, int line, int col) {
            super(message);
            this.line = line;
            this.col = Math.max(1, col);
        }

        public int line() {
            return line;
        }

        public int col() {
            return col;
        }
    }
}
