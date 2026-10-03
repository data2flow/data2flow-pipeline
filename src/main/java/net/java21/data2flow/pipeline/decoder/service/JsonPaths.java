package net.java21.data2flow.pipeline.decoder.service;

import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * generic-json 매핑의 작은 JSON 경로(ING-02.03): {@code $}, {@code .key}, {@code ['key']}, {@code [n]}, {@code [*]}.
 * 외부 라이브러리 없이 Jackson 트리에서 바로 찾는다. 경로가 없으면 빈 목록(그 항목은 건너뜀).
 */
public final class JsonPaths {

    private static final Pattern STEP = Pattern.compile("\\.([A-Za-z_$][A-Za-z0-9_$-]*)|\\['([^']+)']|\\[\"([^\"]+)\"]|\\[(\\d+)]|\\[\\*]");

    private JsonPaths() {
    }

    /** 경로 문법이 맞는지 */
    public static boolean isValid(String path) {
        if (path == null || !path.startsWith("$")) {
            return false;
        }
        String rest = path.substring(1);
        Matcher m = STEP.matcher(rest);
        int pos = 0;
        while (m.find()) {
            if (m.start() != pos) {
                return false;
            }
            pos = m.end();
        }
        return pos == rest.length();
    }

    /** 경로가 가리키는 모든 노드({@code [*]}면 여러 개) */
    public static List<JsonNode> select(JsonNode root, String path) {
        if (!isValid(path)) {
            throw new IllegalArgumentException("잘못된 JSON 경로입니다: " + path);
        }
        List<JsonNode> current = new ArrayList<>(List.of(root));
        Matcher m = STEP.matcher(path.substring(1));
        while (m.find()) {
            List<JsonNode> next = new ArrayList<>();
            for (JsonNode node : current) {
                if (m.group(1) != null || m.group(2) != null || m.group(3) != null) {
                    String field = m.group(1) != null ? m.group(1) : m.group(2) != null ? m.group(2) : m.group(3);
                    JsonNode child = node.get(field);
                    if (child != null && !child.isNull()) {
                        next.add(child);
                    }
                } else if (m.group(4) != null) {
                    JsonNode child = node.get(Integer.parseInt(m.group(4)));
                    if (child != null && !child.isNull()) {
                        next.add(child);
                    }
                } else if (node.isArray()) {
                    node.forEach(next::add);
                }
            }
            current = next;
        }
        return current;
    }

    /** 첫 번째 값. 없으면 null */
    public static JsonNode first(JsonNode root, String path) {
        List<JsonNode> all = select(root, path);
        return all.isEmpty() ? null : all.getFirst();
    }
}
