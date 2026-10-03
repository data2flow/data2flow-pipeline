package net.java21.data2flow.pipeline.device.domain;

/**
 * 자동 등록 결과(API-DEV-121).
 *
 * @param deviceId 등록되었거나 이미 있던 기기. 거부면 null
 * @param created  새로 만들었는지
 * @param outcome  REGISTERED, QUOTA_EXCEEDED(DEVICE_AUTOREG_LIMIT 429 또는 ING_AUTO_REGISTER_QUOTA), REJECTED(DEVICE_REJECTED 409)
 */
public record AutoRegisterResult(Long deviceId, boolean created, Outcome outcome) {

    public enum Outcome {
        REGISTERED, QUOTA_EXCEEDED, REJECTED
    }
}
