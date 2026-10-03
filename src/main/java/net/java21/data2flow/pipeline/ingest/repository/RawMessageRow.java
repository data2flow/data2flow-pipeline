package net.java21.data2flow.pipeline.ingest.repository;

import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;

import java.time.Instant;
import java.util.UUID;

/** {@code raw_messages} 한 행(design/erd/pipeline.md §2.1). JSON 열은 문자열 */
public record RawMessageRow(long id, long organizationId, long sourceId, Long deviceId, UUID messageId, String sourceType,
                            String topic, byte[] payload, String payloadEncoding, String ingressInstance, String dedupKey,
                            int streamPartition, long streamOffset, String externalId, RawMessageStatus status,
                            String errorCode, String errorDetail, String processingTrace, Integer metricCount,
                            boolean dropped, boolean virtual, Instant receivedAt, Instant processedAt) {
}
