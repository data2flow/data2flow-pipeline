package net.java21.data2flow.pipeline.formula.domain;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * 수식형 파생 항목(SCR-01.06, BR-SCR-19)의 수식을 읽어 샌드박스에서 실행할 JavaScript 식으로 바꾼다. 수식은 같은 샌드박스에서 실행한다
 * (코드 생성 결과에는 금지 API가 없고, 측정 값은 {@code __m("key")}, 창은 {@code __roll("key", 600000, "mean")}(밀리초)로만 읽는다).
 *
 * <pre>
 * 식     := 항 (('+' | '-') 항)*
 * 항     := 인수 (('*' | '/' | '%') 인수)*
 * 인수   := ('-' | '+') 인수 | 기본 ('^' 인수)?
 * 기본   := 숫자 | 측정키 | 함수 '(' 인자 (',' 인자)* ')' | '(' 식 ')'
 * 인자   := 식 | 기간(rolling_* 둘째 인자: 30s, 10m, 1h — 1분~24시간)
 * </pre>
 * 함수: {@code thi, dew_point(dewPoint), abs_humidity, round, clamp, c2f, f2c, min, max, abs, sqrt, pow, log, exp,
 * rolling_mean, rolling_min, rolling_max, rolling_sum, rolling_count}. 오류는 {@code SCRIPT_FORMULA_INVALID}와 열 위치.
 */
public final class FormulaCompiler {

    public static final String ERROR_CODE = "SCRIPT_FORMULA_INVALID";
    private static final int MAX_LENGTH = 500;

    /** 함수 이름 → (JS 식 접두, 인자 수 최소, 최대) */
    private static final Map<String, Fn> FUNCTIONS = Map.ofEntries(
            Map.entry("thi", new Fn("U.thi", 2, 2)),
            Map.entry("dew_point", new Fn("U.dewPoint", 2, 2)),
            Map.entry("dewpoint", new Fn("U.dewPoint", 2, 2)),
            Map.entry("abs_humidity", new Fn("U.absHumidity", 2, 2)),
            Map.entry("round", new Fn("U.round", 1, 2)),
            Map.entry("clamp", new Fn("U.clamp", 3, 3)),
            Map.entry("c2f", new Fn("U.c2f", 1, 1)),
            Map.entry("f2c", new Fn("U.f2c", 1, 1)),
            Map.entry("min", new Fn("Math.min", 1, 10)),
            Map.entry("max", new Fn("Math.max", 1, 10)),
            Map.entry("abs", new Fn("Math.abs", 1, 1)),
            Map.entry("sqrt", new Fn("Math.sqrt", 1, 1)),
            Map.entry("pow", new Fn("Math.pow", 2, 2)),
            Map.entry("log", new Fn("Math.log", 1, 1)),
            Map.entry("exp", new Fn("Math.exp", 1, 1)));
    private static final Set<String> ROLLING = Set.of("rolling_mean", "rolling_min", "rolling_max", "rolling_sum",
            "rolling_count");

    private final String src;
    private final Predicate<String> knownMetric;
    private final Set<String> inputs = new LinkedHashSet<>();
    private final Map<String, Duration> windows = new java.util.LinkedHashMap<>();
    private int pos;

    private FormulaCompiler(String src, Predicate<String> knownMetric) {
        this.src = src;
        this.knownMetric = knownMetric;
    }

    /**
     * @param expression  수식
     * @param knownMetric 측정 키가 있는지(오타 검사). 모르면 {@code k -> true}
     * @throws FormulaException 문법·측정 키·함수·창 오류
     */
    public static Compiled compile(String expression, Predicate<String> knownMetric) {
        if (expression == null || expression.isBlank()) {
            throw new FormulaException("수식이 비어 있습니다", 1);
        }
        if (expression.length() > MAX_LENGTH) {
            throw new FormulaException("수식은 " + MAX_LENGTH + "자 이하입니다", MAX_LENGTH);
        }
        FormulaCompiler c = new FormulaCompiler(expression, knownMetric);
        String js = c.expr();
        c.skipSpaces();
        if (c.pos < c.src.length()) {
            if (c.src.charAt(c.pos) == ')') {
                throw new FormulaException("괄호가 맞지 않습니다", c.pos + 1);
            }
            throw new FormulaException("알 수 없는 글자입니다: " + c.src.charAt(c.pos), c.pos + 1);
        }
        return new Compiled(expression, js, List.copyOf(c.inputs), Map.copyOf(c.windows));
    }

    private String expr() {
        StringBuilder out = new StringBuilder(term());
        while (true) {
            skipSpaces();
            if (peek('+') || peek('-')) {
                char op = src.charAt(pos++);
                out.append(' ').append(op).append(' ').append(term());
            } else {
                return out.toString();
            }
        }
    }

    private String term() {
        StringBuilder out = new StringBuilder(factor());
        while (true) {
            skipSpaces();
            if (peek('*') || peek('/') || peek('%')) {
                char op = src.charAt(pos++);
                out.append(' ').append(op).append(' ').append(factor());
            } else {
                return out.toString();
            }
        }
    }

    private String factor() {
        skipSpaces();
        if (peek('-') || peek('+')) {
            char op = src.charAt(pos++);
            return "(" + op + factor() + ")";
        }
        String base = primary();
        skipSpaces();
        if (peek('^')) {
            pos++;
            return "Math.pow(" + base + ", " + factor() + ")";
        }
        return base;
    }

    private String primary() {
        skipSpaces();
        if (pos >= src.length()) {
            throw new FormulaException("수식이 끝나지 않았습니다", pos + 1);
        }
        char ch = src.charAt(pos);
        if (ch == '(') {
            int open = pos++;
            String inner = expr();
            skipSpaces();
            if (!peek(')')) {
                throw new FormulaException("괄호가 맞지 않습니다", open + 1);
            }
            pos++;
            return "(" + inner + ")";
        }
        if (Character.isDigit(ch) || ch == '.') {
            int start = pos;
            while (pos < src.length() && (Character.isDigit(src.charAt(pos)) || src.charAt(pos) == '.')) {
                pos++;
            }
            String number = src.substring(start, pos);
            try {
                Double.parseDouble(number);
            } catch (NumberFormatException e) {
                throw new FormulaException("숫자 형식이 아닙니다: " + number, start + 1);
            }
            return number;
        }
        if (Character.isLetter(ch) || ch == '_') {
            int start = pos;
            String name = identifier();
            skipSpaces();
            if (peek('(')) {
                pos++;
                return call(name, start);
            }
            if (!knownMetric.test(name)) {
                throw new FormulaException("알 수 없는 측정 항목: " + name, start + 1);
            }
            inputs.add(name);
            return "__m(" + quote(name) + ")";
        }
        throw new FormulaException("알 수 없는 글자입니다: " + ch, pos + 1);
    }

    private String call(String name, int start) {
        String lower = name.toLowerCase();
        if (ROLLING.contains(lower)) {
            skipSpaces();
            int keyAt = pos;
            String key = identifier();
            if (key.isEmpty() || !knownMetric.test(key)) {
                throw new FormulaException("알 수 없는 측정 항목: " + key, keyAt + 1);
            }
            skipSpaces();
            expect(',', "rolling 함수는 (측정 항목, 기간) 두 인자입니다");
            skipSpaces();
            int durAt = pos;
            String dur = identifierOrDuration();
            Duration window = parseWindow(dur, durAt);
            skipSpaces();
            expect(')', "괄호가 맞지 않습니다");
            inputs.add(key);
            windows.merge(key, window, (a, b) -> a.compareTo(b) >= 0 ? a : b);
            return "__roll(" + quote(key) + ", " + window.toMillis() + ", " + quote(lower.substring("rolling_".length())) + ")";
        }
        Fn fn = FUNCTIONS.get(lower);
        if (fn == null) {
            throw new FormulaException("알 수 없는 함수: " + name, start + 1);
        }
        List<String> args = new ArrayList<>();
        skipSpaces();
        if (!peek(')')) {
            args.add(expr());
            skipSpaces();
            while (peek(',')) {
                pos++;
                args.add(expr());
                skipSpaces();
            }
        }
        if (!peek(')')) {
            throw new FormulaException("괄호가 맞지 않습니다", start + 1);
        }
        pos++;
        if (args.size() < fn.min() || args.size() > fn.max()) {
            throw new FormulaException(name + " 함수의 인자 수가 맞지 않습니다(" + fn.min()
                    + (fn.max() == fn.min() ? "" : "~" + fn.max()) + "개)", start + 1);
        }
        return fn.js() + "(" + String.join(", ", args) + ")";
    }

    static Duration parseWindow(String text, int at) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(\\d+)(s|m|h)").matcher(text);
        if (!m.matches()) {
            throw new FormulaException("기간 형식이 아닙니다(예: 10m): " + text, at + 1);
        }
        long n = Long.parseLong(m.group(1));
        Duration d = switch (m.group(2)) {
            case "s" -> Duration.ofSeconds(n);
            case "m" -> Duration.ofMinutes(n);
            default -> Duration.ofHours(n);
        };
        if (d.compareTo(Duration.ofMinutes(1)) < 0 || d.compareTo(Duration.ofHours(24)) > 0) {
            throw new FormulaException("창 길이는 1분~24시간입니다: " + text, at + 1);
        }
        return d;
    }

    private String identifier() {
        int start = pos;
        while (pos < src.length() && (Character.isLetterOrDigit(src.charAt(pos)) || src.charAt(pos) == '_')) {
            pos++;
        }
        return src.substring(start, pos);
    }

    private String identifierOrDuration() {
        int start = pos;
        while (pos < src.length() && Character.isLetterOrDigit(src.charAt(pos))) {
            pos++;
        }
        return src.substring(start, pos);
    }

    private void expect(char ch, String message) {
        if (!peek(ch)) {
            throw new FormulaException(message, pos + 1);
        }
        pos++;
    }

    private boolean peek(char ch) {
        return pos < src.length() && src.charAt(pos) == ch;
    }

    private void skipSpaces() {
        while (pos < src.length() && Character.isWhitespace(src.charAt(pos))) {
            pos++;
        }
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private record Fn(String js, int min, int max) {
    }

    /**
     * @param expression 원래 수식
     * @param js         JavaScript 식(헬퍼 {@code __m}, {@code __roll}, {@code U}를 씀)
     * @param inputs     읽는 측정 키
     * @param windows    창이 필요한 측정 키 → 가장 긴 창
     */
    public record Compiled(String expression, String js, List<String> inputs, Map<String, Duration> windows) {
    }

    /** 수식 오류({@code SCRIPT_FORMULA_INVALID}, 줄은 항상 1) */
    public static class FormulaException extends RuntimeException {
        private final int col;

        public FormulaException(String message, int col) {
            super(message);
            this.col = col;
        }

        public int line() {
            return 1;
        }

        public int col() {
            return col;
        }
    }
}
