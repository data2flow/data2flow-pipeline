package net.java21.data2flow.pipeline.script.service;

import tools.jackson.databind.JsonNode;

import java.util.Optional;

/** 스크립트 설정값(ctx.config, SCR-04.02)을 실행 번들에서 찾는다 */
public interface ScriptConfigLookup {

    Optional<JsonNode> config(long organizationId, long scriptId);
}
