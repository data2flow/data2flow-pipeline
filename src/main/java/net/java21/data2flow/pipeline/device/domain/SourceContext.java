package net.java21.data2flow.pipeline.device.domain;

import tools.jackson.databind.JsonNode;

/**
 * 소스 처리 정보(API-ING-21 {@code GET /internal/core/ingest-context?sourceId=}). 소스 설정 변경(EVT-DSC-01 SOURCE)이 오면 다시 읽는다.
 *
 * @param sourceId                데이터 소스 ID
 * @param organizationId          조직 ID
 * @param decoderKey              디코더 키(chirpstack-v4, generic-json, single-value, script, milesight-*)
 * @param decodeScriptId          DECODE 스크립트(decoderKey=script)
 * @param decoderConfig           디코더 설정(generic-json 매핑 등). 없으면 빈 객체
 * @param unknownDevicePolicy     미등록 기기 정책
 * @param autoRegisterHourlyLimit 시간당 자동 등록 한도(BR-ING-09)
 * @param defaultModelId          기본 기기 모델(자동 등록 시 core가 적용, DSC-01.07)
 * @param defaultSpaceId          기본 공간
 * @param contextVersion          설정 버전
 */
public record SourceContext(long sourceId, long organizationId, String decoderKey, Long decodeScriptId,
                            JsonNode decoderConfig, UnknownDevicePolicy unknownDevicePolicy, int autoRegisterHourlyLimit,
                            Long defaultModelId, Long defaultSpaceId, long contextVersion) {
}
