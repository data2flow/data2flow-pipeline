package net.java21.data2flow.pipeline.script.service;

import java.time.Instant;
import java.util.Optional;

/** 테스트 실행에서 원본 메시지 ID로 DECODE 입력을 만들 때 원본을 찾는다(ingest가 구현) */
public interface RawInputLoader {

    Optional<RawInput> load(long organizationId, long rawMessageId);

    record RawInput(String topic, byte[] payload, Instant receivedAt, long sourceId, String sourceType) {
    }
}
