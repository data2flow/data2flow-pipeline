package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import net.java21.data2flow.pipeline.device.domain.SourceContext;
import net.java21.data2flow.pipeline.ingest.domain.DlqStage;
import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRow;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** 메시지 하나를 처리하는 동안의 중간 결과. 처리가 끝나면 한 트랜잭션으로 기록된다({@link IngestStore}) */
final class MessageDraft {

    final RawEnvelope envelope;
    final int partition;
    final long offset;
    final RawMessageRow existing;
    final ProcessingTrace trace;

    RawMessageStatus status = RawMessageStatus.OK;
    String errorCode;
    ObjectNode errorDetail;
    String dedupKey;
    SourceContext source;
    DeviceInfo device;
    String externalId;
    Instant measuredAt;
    boolean timeCorrected;
    boolean late;
    boolean dropped;
    final List<MetricOut> metrics = new ArrayList<>();
    CanonicalTelemetry.Link link;
    Map<String, String> tags = Map.of();
    CanonicalTelemetry.DecoderRef decoder;
    final List<CanonicalTelemetry.ScriptRef> scripts = new ArrayList<>();
    byte[] payloadToStore;

    MessageDraft(RawEnvelope envelope, int partition, long offset, RawMessageRow existing, JsonMapper mapper) {
        this.envelope = envelope;
        this.partition = partition;
        this.offset = offset;
        this.existing = existing;
        this.trace = new ProcessingTrace(mapper);
        this.dedupKey = existing != null ? existing.dedupKey() : envelope.dedupKey();
        this.payloadToStore = envelope.payload();
    }

    boolean failed() {
        return status != RawMessageStatus.OK;
    }

    /** 실패로 끝낸다(메시지는 원본과 함께 기록, DLQ 단계는 상태로 정해진다) */
    void fail(RawMessageStatus failStatus, String code, String message, JsonMapper mapper) {
        this.status = failStatus;
        this.errorCode = code;
        this.errorDetail = mapper.createObjectNode();
        this.errorDetail.put("message", message == null ? "" : message);
        DlqStage stage = failStatus.dlqStage();
        if (stage != null) {
            this.errorDetail.put("stage", stage.name());
        }
    }

    boolean reprocessing() {
        return existing != null;
    }

    UUID telemetryMessageId() {
        // 같은 원본을 다시 처리해도 같은 messageId → 하위가 중복을 거른다(reliability-and-ha.md §2 ⑤)
        return UUID.nameUUIDFromBytes(("telemetry:" + envelope.organizationId() + ":" + envelope.messageId())
                .getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 표준 메시지의 측정값 하나
     *
     * @param originalKey 원본 키(별칭 변환 전)
     */
    record MetricOut(String key, double value, String unit, int quality, boolean derived, String originalKey) {

        MetricOut withQuality(int q) {
            return new MetricOut(key, value, unit, q, derived, originalKey);
        }
    }
}
