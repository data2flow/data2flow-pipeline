package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.DeviceConnectivityChanged;
import net.java21.data2flow.contracts.message.event.IngestGapDetected;
import net.java21.data2flow.pipeline.common.PayloadEncoding;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.ingest.domain.DlqStage;
import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;
import net.java21.data2flow.pipeline.ingest.repository.DlqItemRepository;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRepository;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRow;
import net.java21.data2flow.pipeline.telemetry.repository.DataGapRepository;
import net.java21.data2flow.pipeline.telemetry.repository.DeviceStateRepository;
import net.java21.data2flow.pipeline.telemetry.repository.LinkQualityRow;
import net.java21.data2flow.pipeline.telemetry.repository.TelemetryRepository;
import net.java21.data2flow.pipeline.telemetry.repository.TelemetryRow;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 처리 결과를 <b>한 트랜잭션</b>으로 기록한다(ING-01.01·05.02, TSD-01.01·01.02): 원본(raw_messages, 실패도 보관) + 측정값
 * (telemetry, ON CONFLICT) + 통신 품질(link_qualities) + 기기 상태(device_state, 더 최신일 때만) + 수신 공백(data_gaps) +
 * 늦은 데이터의 집계 재계산 구간(agg_dirty_ranges) + 실패 보관함(dlq_items). 커밋이 끝나야 표준 메시지를 발행하고 스트림 오프셋을
 * 저장한다(reliability-and-ha.md §2 ③).
 */
public class IngestStore {

    private static final int STORED_PREFIX_BYTES = 4096;

    private final RawMessageRepository raws;
    private final DlqItemRepository dlq;
    private final TelemetryRepository telemetry;
    private final DeviceStateRepository states;
    private final DataGapRepository gaps;
    private final PipelineProperties properties;
    private final JsonMapper mapper = MessageCodec.newMapper();

    public IngestStore(RawMessageRepository raws, DlqItemRepository dlq, TelemetryRepository telemetry,
                       DeviceStateRepository states, DataGapRepository gaps, PipelineProperties properties) {
        this.raws = raws;
        this.dlq = dlq;
        this.telemetry = telemetry;
        this.states = states;
        this.gaps = gaps;
        this.properties = properties;
    }

    @Transactional
    public Stored persist(MessageDraft d, Instant now) {
        long org = d.envelope.organizationId();
        long rawId = d.existing != null ? d.existing.id() : raws.allocateId();
        CanonicalTelemetry canonical = null;
        DeviceConnectivityChanged connectivity = null;
        IngestGapDetected gap = null;
        int stored = 0;
        if (d.status == RawMessageStatus.OK && !d.dropped && d.device != null) {
            canonical = canonical(d, rawId);
            stored = writeTelemetry(d, rawId);
            var prior = states.lockState(org, d.device.deviceId());
            int interval = d.device.effectiveIntervalSec(properties.offline().defaultIntervalSec());
            if (prior.isEmpty() || !"ONLINE".equals(prior.get().connectivity())) {
                DeviceConnectivityChanged.Connectivity from = prior.isEmpty() || "UNKNOWN".equals(prior.get().connectivity())
                        ? null : DeviceConnectivityChanged.Connectivity.valueOf(prior.get().connectivity());
                connectivity = new DeviceConnectivityChanged(d.device.deviceId(), from,
                        DeviceConnectivityChanged.Connectivity.ONLINE, d.envelope.receivedAt(), interval,
                        d.device.effectiveMultiplier(properties.offline().defaultMultiplier()));
            }
            if (prior.isPresent() && prior.get().lastSeenAt() != null) {
                Instant from = prior.get().lastSeenAt();
                Duration silence = Duration.between(from, d.envelope.receivedAt());
                if (silence.toMillis() >= (long) (properties.offline().gapFactor() * interval * 1000)) {
                    int missing = (int) (silence.toSeconds() / interval);
                    if (gaps.insert(org, d.device.deviceId(), from, d.envelope.receivedAt(), missing, now)) {
                        gap = new IngestGapDetected(d.device.deviceId(), from, d.envelope.receivedAt(), missing);
                    }
                }
            }
            updateState(d, connectivity != null);
            d.trace.put("canonical", mapper.valueToTree(canonical));
        }
        d.trace.stage("store", true, 0).put("rows", stored);
        RawMessageRow row = row(d, rawId, now);
        if (d.existing != null) {
            raws.updateResult(org, row);
        } else {
            raws.insert(row);
        }
        DlqStage stage = d.status.dlqStage();
        if (stage != null) {
            dlq.insert(org, rawId, d.envelope.receivedAt(), d.envelope.sourceId(),
                    d.device == null ? null : d.device.deviceId(), stage, d.errorCode,
                    d.errorDetail == null ? "" : d.errorDetail.path("message").asString(""), now);
        } else if (d.existing != null && d.status == RawMessageStatus.OK) {
            dlq.markResolved(org, rawId, now);
        }
        return new Stored(rawId, canonical, connectivity, gap, stored);
    }

    private int writeTelemetry(MessageDraft d, long rawId) {
        long org = d.envelope.organizationId();
        long deviceId = d.device.deviceId();
        int flags = (d.late ? TelemetryRow.FLAG_LATE : 0) | (d.reprocessing() ? TelemetryRow.FLAG_REPROCESSED : 0);
        List<TelemetryRow> rows = new ArrayList<>();
        for (MessageDraft.MetricOut m : d.metrics) {
            rows.add(new TelemetryRow(deviceId, m.key(), d.measuredAt, org, m.value(), m.quality(), flags,
                    d.envelope.virtual(), d.envelope.receivedAt(), rawId));
        }
        int inserted = telemetry.insertAll(rows, d.reprocessing());
        List<LinkQualityRow> links = new ArrayList<>();
        if (d.link != null) {
            if (d.link.gateways() != null && !d.link.gateways().isEmpty()) {
                for (CanonicalTelemetry.GatewayReception g : d.link.gateways()) {
                    links.add(new LinkQualityRow(deviceId, g.eui(), d.measuredAt, org, g.rssi(), g.snr(),
                            d.link.frameCounter()));
                }
            } else if (d.link.rssi() != null || d.link.snr() != null) {
                links.add(new LinkQualityRow(deviceId, "", d.measuredAt, org, d.link.rssi(), d.link.snr(),
                        d.link.frameCounter()));
            }
        }
        telemetry.insertLinks(links);
        markDirtyIfAggregated(d);
        return inserted;
    }

    /** 이미 집계한 1분 구간에 들어온 값이면 다시 계산할 구간을 남긴다(BR-TSD-06, ING-06.03) */
    private void markDirtyIfAggregated(MessageDraft d) {
        Instant watermark = telemetry.currentWatermark("1m");
        if (watermark == null || !d.measuredAt.isBefore(watermark)) {
            return;
        }
        Instant from = d.measuredAt.truncatedTo(ChronoUnit.MINUTES);
        String reason = d.reprocessing() ? "REPROCESS" : "LATE";
        for (MessageDraft.MetricOut m : d.metrics) {
            telemetry.insertDirtyRange(d.envelope.organizationId(), "1m", d.device.deviceId(), m.key(), from,
                    from.plus(Duration.ofMinutes(1)), reason);
        }
    }

    private void updateState(MessageDraft d, boolean connectivityChanged) {
        ObjectNode latest = mapper.createObjectNode();
        Double battery = null;
        for (MessageDraft.MetricOut m : d.metrics) {
            ObjectNode v = latest.putObject(m.key());
            v.put("v", m.value());
            v.put("t", d.measuredAt.toString());
            v.put("q", m.quality());
            if (m.unit() != null) {
                v.put("unit", m.unit());
            }
            if ("battery".equals(m.key())) {
                battery = Math.max(0, Math.min(999.99, m.value()));
            }
        }
        String bestGateway = null;
        if (d.link != null && d.link.gateways() != null) {
            bestGateway = d.link.gateways().stream()
                    .filter(g -> g.rssi() != null)
                    .max((a, b) -> Double.compare(a.rssi(), b.rssi()))
                    .map(CanonicalTelemetry.GatewayReception::eui).orElse(null);
        }
        states.upsertReceived(d.envelope.organizationId(), d.device.deviceId(), d.envelope.receivedAt(), d.measuredAt,
                mapper.writeValueAsString(latest), battery, d.link == null ? null : d.link.rssi(),
                d.link == null ? null : d.link.snr(), bestGateway, connectivityChanged);
    }

    CanonicalTelemetry canonical(MessageDraft d, long rawId) {
        List<CanonicalTelemetry.Metric> metrics = d.metrics.stream()
                .map(m -> new CanonicalTelemetry.Metric(m.key(), m.value(), m.unit(), m.quality(), m.derived() ? true : null))
                .toList();
        return CanonicalTelemetry.builder()
                .messageId(d.telemetryMessageId())
                .organizationId(d.envelope.organizationId())
                .sourceId(d.envelope.sourceId())
                .externalId(d.externalId)
                .deviceId(d.device.deviceId())
                .deviceStatus(d.device.telemetryStatus())
                .modelId(d.device.modelId() == null ? null : Long.toString(d.device.modelId()))
                .spaceId(d.device.spaceId())
                .measuredAt(d.measuredAt)
                .receivedAt(d.envelope.receivedAt())
                .late(d.late)
                .virtual(d.envelope.virtual())
                .metrics(metrics)
                .link(d.link)
                .meta(new CanonicalTelemetry.Meta(d.tags.isEmpty() ? null : d.tags, d.decoder,
                        d.scripts.isEmpty() ? null : d.scripts))
                .rawMessageId(rawId)
                .build();
    }

    private RawMessageRow row(MessageDraft d, long rawId, Instant now) {
        byte[] payload = d.payloadToStore;
        if (payload.length > properties.ingest().maxPayloadBytes()) {
            payload = Arrays.copyOf(payload, STORED_PREFIX_BYTES); // 256KB 초과는 앞 4KB만(TC-ING-003)
        }
        d.trace.put("instance", properties.instanceId());
        if (d.existing != null) {
            // 재처리 시도 이력(ING domain-model: processing_trace.attempts[])
            tools.jackson.databind.node.ArrayNode attempts = mapper.createArrayNode();
            if (d.existing.processingTrace() != null) {
                tools.jackson.databind.JsonNode previous = mapper.readTree(d.existing.processingTrace()).get("attempts");
                if (previous != null && previous.isArray()) {
                    previous.forEach(attempts::add);
                }
            }
            attempts.addObject().put("at", now.toString()).put("status", d.status.name()).put("errorCode", d.errorCode);
            d.trace.put("attempts", attempts);
        }
        return new RawMessageRow(rawId, d.envelope.organizationId(), d.envelope.sourceId(),
                d.device == null ? null : d.device.deviceId(), d.envelope.messageId(), storedSourceType(d.envelope.sourceType()),
                d.envelope.topic(), payload, PayloadEncoding.detect(payload).name(), d.envelope.ingressInstance(),
                d.dedupKey, d.partition, d.offset, d.externalId, d.status, d.errorCode,
                d.errorDetail == null ? null : mapper.writeValueAsString(d.errorDetail),
                mapper.writeValueAsString(d.trace.root()), d.status == RawMessageStatus.OK ? d.metrics.size() : null,
                d.dropped, d.envelope.virtual(), d.envelope.receivedAt(), now);
    }

    /** raw_messages.source_type은 계약 SourceTypes 값만 받는다. 모르는 값(더 새 생산자)은 CONNECTOR로 두고 원래 값은 error_detail에 */
    static String storedSourceType(String sourceType) {
        return net.java21.data2flow.contracts.message.SourceTypes.KNOWN.contains(sourceType) ? sourceType
                : net.java21.data2flow.contracts.message.SourceTypes.CONNECTOR;
    }

    /**
     * @param rawId        원본 ID
     * @param canonical    발행할 표준 메시지(OK·저장한 경우만, 아니면 null)
     * @param connectivity 수신으로 ONLINE이 된 경우의 이벤트 페이로드. 아니면 null
     * @param gap          수신 공백이 끝난 경우의 이벤트 페이로드. 아니면 null
     * @param storedRows   새로 저장한 telemetry 행 수
     */
    public record Stored(long rawId, CanonicalTelemetry canonical, DeviceConnectivityChanged connectivity,
                         IngestGapDetected gap, int storedRows) {
    }
}
