package net.java21.data2flow.pipeline.script.service;

import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 테스트 케이스 기대 출력 비교(SCR-03.03, API-SCR-10 {@code compareMode}). EXACT는 JSON이 같아야 하고(키 순서 무관),
 * FIELDS는 지정한 경로({@code $.metrics[0].value}, {@code metrics.temperature} — 배열에서 {@code key}가 같은 항목)만,
 * TOLERANCE는 숫자를 허용 오차 안에서 같다고 본다. 다른 곳은 {@code {path, expected, actual}}로 돌려준다.
 */
public final class TestCaseComparator {

    private TestCaseComparator() {
    }

    public static List<Difference> compare(String mode, JsonNode expected, JsonNode actual, List<String> fields,
                                           Double tolerance) {
        List<Difference> diffs = new ArrayList<>();
        String m = mode == null ? "EXACT" : mode.toUpperCase();
        double tol = "TOLERANCE".equals(m) && tolerance != null ? Math.abs(tolerance) : 0;
        if ("FIELDS".equals(m) && fields != null && !fields.isEmpty()) {
            for (String path : fields) {
                JsonNode e = at(expected, path);
                JsonNode a = at(actual, path);
                walk(path, e, a, tol, diffs);
            }
            return diffs;
        }
        walk("$", expected, actual, tol, diffs);
        return diffs;
    }

    private static void walk(String path, JsonNode e, JsonNode a, double tol, List<Difference> diffs) {
        if (missing(e) && missing(a)) {
            return;
        }
        if (missing(e) || missing(a)) {
            diffs.add(new Difference(path, missing(e) ? null : e, missing(a) ? null : a));
            return;
        }
        if (e.isNumber() && a.isNumber()) {
            if (Math.abs(e.asDouble() - a.asDouble()) > tol + 1e-12) {
                diffs.add(new Difference(path, e, a));
            }
            return;
        }
        if (e.isObject() && a.isObject()) {
            java.util.Set<String> keys = new java.util.LinkedHashSet<>();
            e.properties().forEach(x -> keys.add(x.getKey()));
            a.properties().forEach(x -> keys.add(x.getKey()));
            for (String k : keys) {
                walk(path + "." + k, e.get(k), a.get(k), tol, diffs);
            }
            return;
        }
        if (e.isArray() && a.isArray()) {
            int n = Math.max(e.size(), a.size());
            for (int i = 0; i < n; i++) {
                walk(path + "[" + i + "]", e.get(i), a.get(i), tol, diffs);
            }
            return;
        }
        if (!e.equals(a)) {
            diffs.add(new Difference(path, e, a));
        }
    }

    private static boolean missing(JsonNode n) {
        return n == null || n.isMissingNode();
    }

    /** 간단한 경로: {@code $.a.b[0].c}, 배열 항목 이름은 {@code key} 필드로({@code metrics.temperature.value}) */
    static JsonNode at(JsonNode root, String path) {
        JsonNode cur = root;
        String p = path.startsWith("$") ? path.substring(1) : path;
        for (String part : p.split("\\.")) {
            if (part.isEmpty() || cur == null) {
                continue;
            }
            String name = part;
            List<Integer> indexes = new ArrayList<>();
            int b = part.indexOf('[');
            if (b >= 0) {
                name = part.substring(0, b);
                for (String ix : part.substring(b).split("[\\[\\]]+")) {
                    if (!ix.isEmpty()) {
                        indexes.add(Integer.parseInt(ix));
                    }
                }
            }
            if (!name.isEmpty()) {
                cur = byName(cur, name);
            }
            for (int ix : indexes) {
                cur = cur == null ? null : cur.get(ix);
            }
        }
        return cur;
    }

    private static JsonNode byName(JsonNode node, String name) {
        if (node == null) {
            return null;
        }
        if (node.isArray()) {
            for (Iterator<JsonNode> it = node.iterator(); it.hasNext(); ) {
                JsonNode item = it.next();
                if (name.equals(item.path("key").asString(null))) {
                    return item;
                }
            }
            return null;
        }
        return node.get(name);
    }

    /** 다른 곳 */
    public record Difference(String path, JsonNode expected, JsonNode actual) {

        public Map<String, Object> toMap() {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("path", path);
            m.put("expected", expected);
            m.put("actual", actual);
            return m;
        }
    }
}
