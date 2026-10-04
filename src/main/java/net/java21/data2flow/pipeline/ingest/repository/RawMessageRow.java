package net.java21.data2flow.pipeline.ingest.repository;

import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * {@code raw_messages} 한 행(design/erd/pipeline.md §2.1). JSON 열은 문자열.
 *
 * @param signatureStatus 플랫폼 브로커 서명 판정(VERIFIED·UNSIGNED·INVALID, M5부터 보관). 그 밖의 소스·M5 이전 행은 null
 */
public record RawMessageRow(long id, long organizationId, long sourceId, Long deviceId, UUID messageId, String sourceType,
                            String topic, byte[] payload, String payloadEncoding, String ingressInstance, String dedupKey,
                            int streamPartition, long streamOffset, String externalId, RawMessageStatus status,
                            String errorCode, String errorDetail, String processingTrace, Integer metricCount,
                            boolean dropped, boolean virtual, Instant receivedAt, Instant processedAt,
                            String signatureStatus) {
}
