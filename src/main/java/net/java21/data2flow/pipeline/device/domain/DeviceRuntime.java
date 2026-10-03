package net.java21.data2flow.pipeline.device.domain;

import tools.jackson.databind.JsonNode;

/**
 * 처리용 기기 실행 정보(API-DEV-122 {@code GET /internal/core/devices/{device-id}/runtime}). TRANSFORM 스크립트의
 * {@code ctx.device.attributes}(서버·공유 속성, 읽기 전용)에 쓴다.
 */
public record DeviceRuntime(long deviceId, JsonNode attributes) {
}
