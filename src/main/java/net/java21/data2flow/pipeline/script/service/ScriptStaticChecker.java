package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.script.sandbox.ScriptSandbox;
import net.java21.data2flow.script.sandbox.ScriptFailure;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.domain.ScriptProblem;
import net.java21.data2flow.pipeline.script.domain.ScriptProblem.Severity;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 정적 검사(API-SCR-30, SCR-04.05, BR-SCR-05): 코드 크기, 문법 오류(GraalJS 파서), 금지 식별자, globalThis 수정, 진입 함수 누락,
 * 반환 누락(경고). 문자열·주석 안의 단어와 속성 이름({@code msg.process})·객체 키({@code {fetch: 1}})는 금지 식별자로 보지 않는다.
 * 런타임 차단은 {@link ScriptSandbox}가 따로 하므로 이 검사는 사용자에게 빨리 알려 주는 용도다.
 */
public class ScriptStaticChecker {

    /** BR-SCR-05 금지 식별자 + 샌드박스가 막는 전역 */
    static final Set<String> FORBIDDEN = Set.of("require", "import", "fetch", "XMLHttpRequest", "WebSocket", "eval",
            "Function", "setTimeout", "setInterval", "setImmediate", "process", "Java", "Packages", "Polyglot", "load",
            "Worker", "WebAssembly", "importScripts");

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*");
    private static final Pattern GLOBAL_WRITE =
            Pattern.compile("globalThis\\s*(\\.\\s*[A-Za-z_$][A-Za-z0-9_$]*|\\[[^\\]]*])\\s*=(?!=)");

    private final ScriptSandbox sandbox;

    public ScriptStaticChecker(ScriptSandbox sandbox) {
        this.sandbox = sandbox;
    }

    public List<ScriptProblem> check(ScriptKind kind, String code) {
        return check(kind, code, List.of(), ref -> java.util.Optional.empty());
    }

    /**
     * 공유 모듈을 쓰는 코드의 검사(SCR-04.01, TC-SCR-067): {@code import … from 'module:이름@버전'}은 금지 식별자로 보지 않고
     * 연결해 본다. 없는 모듈·버전 없는 모듈·외부 패키지·순환은 {@code SCRIPT_MODULE_NOT_FOUND}, 가져온 모듈 안의 금지 API는
     * {@code SCRIPT_FORBIDDEN_API}(위치는 가져오는 줄)다.
     */
    public List<ScriptProblem> check(ScriptKind kind, String code, List<String> moduleRefs,
                                     java.util.function.Function<String, java.util.Optional<RuntimeBundle.Module>> modules) {
        List<ScriptProblem> problems = new ArrayList<>();
        if (code != null && !code.isBlank() && (code.contains("import") || !moduleRefs.isEmpty())) {
            String userCode = code;
            java.util.Set<String> used = new java.util.LinkedHashSet<>(moduleRefs);
            Matcher imports = MODULE_IMPORT.matcher(code);
            StringBuilder blanked = new StringBuilder();
            while (imports.find()) {
                used.add(imports.group(2) + "@" + imports.group(3));
                imports.appendReplacement(blanked, Matcher.quoteReplacement(
                        imports.group(0).replaceAll("[^\\n]", " ")));
            }
            imports.appendTail(blanked);
            try {
                ScriptLinker.link(code, kind.functionName(), moduleRefs, modules);
            } catch (ScriptLinker.LinkException e) {
                problems.add(new ScriptProblem(e.line(), e.col(), Severity.ERROR, ScriptLinker.MODULE_NOT_FOUND, e.getMessage()));
            }
            for (String ref : used) {
                modules.apply(ref).ifPresent(module -> {
                    for (String word : forbiddenIn(module.code())) {
                        int[] pos = importPosition(userCode, ref);
                        problems.add(new ScriptProblem(pos[0], pos[1], Severity.ERROR, "SCRIPT_FORBIDDEN_API",
                                "모듈 " + ref + "에서 금지된 API: " + word));
                    }
                });
            }
            List<ScriptProblem> rest = checkPlain(kind, blanked.toString());
            problems.addAll(rest);
            return problems;
        }
        return checkPlain(kind, code);
    }

    private static final Pattern MODULE_IMPORT = Pattern.compile(
            "(?m)^[ \\t]*import\\s+[^;\\n]*?from\\s+(['\"])module:([a-z0-9-]{3,40})@(\\d+)\\1\\s*;?");

    private List<String> forbiddenIn(String moduleCode) {
        List<String> found = new ArrayList<>();
        if (moduleCode == null) {
            return found;
        }
        String masked = maskStringsAndComments(MODULE_IMPORT.matcher(moduleCode).replaceAll(""));
        Matcher m = IDENTIFIER.matcher(masked);
        while (m.find()) {
            String word = m.group();
            if (FORBIDDEN.contains(word) && !isPropertyName(masked, m.start(), m.end()) && !found.contains(word)) {
                found.add(word);
            }
        }
        return found;
    }

    private static int[] importPosition(String code, String ref) {
        int at = code.indexOf("module:" + ref);
        return at < 0 ? new int[]{1, 1} : position(code, at);
    }

    private List<ScriptProblem> checkPlain(ScriptKind kind, String code) {
        List<ScriptProblem> problems = new ArrayList<>();
        if (code == null || code.isBlank()) {
            problems.add(new ScriptProblem(1, 1, Severity.ERROR, "ENTRY_MISSING", kind.functionName() + "(…) 함수가 필요합니다"));
            return problems;
        }
        int bytes = code.getBytes(StandardCharsets.UTF_8).length;
        int max = sandbox.limits().maxCodeBytes();
        if (bytes > max) {
            problems.add(new ScriptProblem(1, 1, Severity.ERROR, "SCRIPT_CODE_TOO_LARGE",
                    "코드가 너무 큽니다(" + bytes + "/" + max + "바이트)"));
            return problems;
        }
        ScriptFailure syntax = sandbox.syntaxCheck(code, "script.js");
        if (syntax != null) {
            problems.add(new ScriptProblem(syntax.line() == null ? 1 : syntax.line(), syntax.col() == null ? 1 : syntax.col(),
                    Severity.ERROR, "SYNTAX_ERROR", "문법 오류: " + syntax.message()));
        }
        String masked = maskStringsAndComments(code);
        Matcher m = IDENTIFIER.matcher(masked);
        while (m.find()) {
            String word = m.group();
            if (FORBIDDEN.contains(word) && !isPropertyName(masked, m.start(), m.end())) {
                int[] pos = position(code, m.start());
                problems.add(new ScriptProblem(pos[0], pos[1], Severity.ERROR, "SCRIPT_FORBIDDEN_API",
                        "금지된 API: " + word + " (" + pos[0] + ":" + pos[1] + ")"));
            }
        }
        Matcher g = GLOBAL_WRITE.matcher(masked);
        while (g.find()) {
            int[] pos = position(code, g.start());
            problems.add(new ScriptProblem(pos[0], pos[1], Severity.ERROR, "SCRIPT_FORBIDDEN_API",
                    "globalThis는 바꿀 수 없습니다 (" + pos[0] + ":" + pos[1] + ")"));
        }
        String fn = kind.functionName();
        Matcher entry = Pattern.compile("function\\s+" + fn + "\\s*\\(|(const|let|var)\\s+" + fn + "\\s*=").matcher(masked);
        if (!entry.find()) {
            problems.add(new ScriptProblem(1, 1, Severity.ERROR, "ENTRY_MISSING",
                    fn + "(" + (kind == ScriptKind.DECODE ? "input" : "msg") + ", ctx) 함수가 필요합니다"));
        } else if (!bodyHasReturn(masked, entry.end())) {
            int[] pos = position(code, entry.start());
            problems.add(new ScriptProblem(pos[0], pos[1], Severity.WARNING, "RETURN_MISSING",
                    kind == ScriptKind.TRANSFORM ? "모든 경로에서 msg 또는 null을 반환해야 합니다"
                            : "모든 경로에서 표준 메시지를 반환해야 합니다"));
        }
        return problems;
    }

    public static boolean hasErrors(List<ScriptProblem> problems) {
        return problems.stream().anyMatch(p -> p.severity() == Severity.ERROR);
    }

    /** 앞이 '.'(속성 접근)이거나 뒤가 ':'이고 앞이 '{'·','(객체 키)이면 속성 이름 */
    private static boolean isPropertyName(String s, int start, int end) {
        int before = start - 1;
        while (before >= 0 && Character.isWhitespace(s.charAt(before))) {
            before--;
        }
        if (before >= 0 && s.charAt(before) == '.' && !(before > 1 && s.startsWith("...", before - 2))) {
            return true;
        }
        int after = end;
        while (after < s.length() && Character.isWhitespace(s.charAt(after))) {
            after++;
        }
        boolean colon = after < s.length() && s.charAt(after) == ':';
        return colon && before >= 0 && (s.charAt(before) == '{' || s.charAt(before) == ',');
    }

    private static boolean bodyHasReturn(String masked, int from) {
        int open = masked.indexOf('{', from);
        if (open < 0) {
            return masked.indexOf("=>", from) >= 0; // 화살표 함수 식 본문
        }
        int depth = 0;
        for (int i = open; i < masked.length(); i++) {
            char c = masked.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return Pattern.compile("\\breturn\\b").matcher(masked.substring(open, i)).find();
                }
            }
        }
        return false;
    }

    /** 문자열·템플릿·주석 내용을 공백으로 바꾼다(위치는 그대로) */
    static String maskStringsAndComments(String code) {
        StringBuilder out = new StringBuilder(code);
        int i = 0;
        int n = code.length();
        while (i < n) {
            char c = code.charAt(i);
            if (c == '/' && i + 1 < n && code.charAt(i + 1) == '/') {
                while (i < n && code.charAt(i) != '\n') {
                    out.setCharAt(i++, ' ');
                }
            } else if (c == '/' && i + 1 < n && code.charAt(i + 1) == '*') {
                int endComment = code.indexOf("*/", i + 2);
                int stop = endComment < 0 ? n : endComment + 2;
                for (; i < stop; i++) {
                    if (code.charAt(i) != '\n') {
                        out.setCharAt(i, ' ');
                    }
                }
            } else if (c == '\'' || c == '"' || c == '`') {
                int j = i + 1;
                while (j < n && code.charAt(j) != c) {
                    if (code.charAt(j) == '\\') {
                        j++;
                    }
                    j++;
                }
                for (int k = i + 1; k < Math.min(j, n); k++) {
                    if (code.charAt(k) != '\n') {
                        out.setCharAt(k, ' ');
                    }
                }
                i = j + 1;
            } else {
                i++;
            }
        }
        return out.toString();
    }

    private static int[] position(String code, int index) {
        int line = 1;
        int col = 1;
        for (int i = 0; i < index && i < code.length(); i++) {
            if (code.charAt(i) == '\n') {
                line++;
                col = 1;
            } else {
                col++;
            }
        }
        return new int[]{line, col};
    }
}
