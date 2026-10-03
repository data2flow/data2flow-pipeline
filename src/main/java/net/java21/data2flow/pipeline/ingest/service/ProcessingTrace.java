package net.java21.data2flow.pipeline.ingest.service;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * 처리 기록({@code raw_messages.processing_trace}, API-ING-06 {@code trace[{stage, ok, ms, info}]}): 단계별 결과·소요 ms,
 * 디코더·스크립트 이름과 버전, 표준 메시지(재발행·원본 상세), 재처리 시도 {@code attempts[]}.
 */
final class ProcessingTrace {

    private final ObjectNode root;
    private final ArrayNode stages;

    ProcessingTrace(JsonMapper mapper) {
        this.root = mapper.createObjectNode();
        this.stages = root.putArray("stages");
    }

    ObjectNode stage(String name, boolean ok, double ms) {
        ObjectNode s = stages.addObject();
        s.put("stage", name);
        s.put("ok", ok);
        s.put("ms", Math.round(ms * 1000) / 1000.0);
        return s.putObject("info");
    }

    void decoder(String key, String version) {
        ObjectNode d = root.putObject("decoder");
        d.put("key", key);
        d.put("version", version);
    }

    void script(long id, int version) {
        ArrayNode scripts = root.has("scripts") ? (ArrayNode) root.get("scripts") : root.putArray("scripts");
        ObjectNode s = scripts.addObject();
        s.put("id", id);
        s.put("version", version);
    }

    void skipped(String key, String reason) {
        ArrayNode skipped = root.has("skipped") ? (ArrayNode) root.get("skipped") : root.putArray("skipped");
        ObjectNode s = skipped.addObject();
        s.put("key", key);
        s.put("reason", reason);
    }

    void put(String field, JsonNode value) {
        root.set(field, value);
    }

    void put(String field, String value) {
        root.put(field, value);
    }

    ObjectNode root() {
        return root;
    }
}
