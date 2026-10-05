package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.ClockSkewSuspected;
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

    private final java.util.function.BiFunction<Long, String, String> storeModes;

    public IngestStore(RawMessageRepository raws, DlqItemRepository dlq, TelemetryRepository telemetry,
                       DeviceStateRepository states, DataGapRepository gaps, PipelineProperties properties) {
        this(raws, dlq, telemetry, states, gaps, properties, (org, key) -> "ALL");
    }

    /** @param storeModes (조직, 측정 키) → 저장 방식 ALL·ON_CHANGE(TSD-05.03, 보관 정책 METRIC 범위) */
    public IngestStore(RawMessageRepository raws, DlqItemRepository dlq, TelemetryRepository telemetry,
                       DeviceStateRepository states, DataGapRepository gaps, PipelineProperties properties,
                       java.util.function.BiFunction<Long, String, String> storeModes) {
        this.storeModes = storeModes;
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
        ClockSkewSuspected clockSkew = null;
        int stored = 0;
        if (d.status == RawMessageStatus.OK && !d.dropped && d.device != null) {
            canonical = canonical(d, rawId);
            var prior = states.lockState(org, d.device.deviceId());
            tools.jackson.databind.JsonNode priorLatest = prior.isEmpty() ? mapper.createObjectNode()
                    : states.findLatest(org, d.device.deviceId()).map(mapper::readTree).orElse(mapper.createObjectNode());
            java.util.Map<String, Instant> storedAt = new java.util.HashMap<>();
            stored = writeTelemetry(d, rawId, priorLatest, storedAt);
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
            updateState(d, connectivity != null, priorLatest, storedAt);
            if (d.clockSkew != null) {
                states.updateClockSkew(org, d.device.deviceId(), d.clockSkew.avgSkewSec(), d.clockSkew.since(),
                        d.clockSkew.suspected());
                clockSkew = d.clockSkew.event();
            }
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
        return new Stored(rawId, canonical, connectivity, gap, stored, clockSkew);
    }

    private int writeTelemetry(MessageDraft d, long rawId, tools.jackson.databind.JsonNode priorLatest,
                               java.util.Map<String, Instant> storedAt) {
        long org = d.envelope.organizationId();
        long deviceId = d.device.deviceId();
        int flags = (d.late ? TelemetryRow.FLAG_LATE : 0) | (d.reprocessing() ? TelemetryRow.FLAG_REPROCESSED : 0);
        List<TelemetryRow> rows = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        for (MessageDraft.MetricOut m : d.metrics) {
            int rowFlags = flags;
            if (!d.reprocessing() && "ON_CHANGE".equalsIgnoreCase(storeModes.apply(org, m.key()))) {
                OnChange decision = onChange(priorLatest.get(m.key()), m.value(), d.measuredAt);
                if (decision == OnChange.SKIP) {
                    skipped.add(m.key());
                    continue;
                }
                if (decision == OnChange.CHANGED) {
                    rowFlags |= TelemetryRow.FLAG_STATE_CHANGE;
                }
            }
            storedAt.put(m.key(), d.measuredAt);
            rows.add(new TelemetryRow(deviceId, m.key(), d.measuredAt, org, m.value(), m.quality(), rowFlags,
                    d.envelope.virtual(), d.envelope.receivedAt(), rawId));
        }
        if (!skipped.isEmpty()) {
            d.trace.stage("store-mode", true, 0).put("unchangedNotStored", String.join(",", skipped));
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

    /** 상태형 저장 판단(TSD-05.03, BR-TSD-17) */
    enum OnChange { SKIP, CHANGED, HEARTBEAT }

    /** 상태형 측정의 하트비트 간격(BR-TSD-17: 1시간마다 한 번은 현재 값 저장) */
    static final java.time.Duration ON_CHANGE_HEARTBEAT = java.time.Duration.ofHours(1);

    /**
     * 값이 직전 값과 같고 마지막 저장 뒤 1시간이 안 됐으면 저장하지 않는다. 직전 값이 없거나 다르면 변화(state_change 표시),
     * 같지만 1시간이 지났으면 하트비트로 저장한다. 순서가 바뀐(더 오래된) 값은 판단하지 않고 저장한다.
     *
     * @param prev {@code latest[key]} = {v, t, q, unit, s(마지막 저장 측정 시각)}
     */
    static OnChange onChange(tools.jackson.databind.JsonNode prev, double value, Instant measuredAt) {
        if (prev == null || !prev.path("v").isNumber()) {
            return OnChange.CHANGED;
        }
        Instant last = prev.hasNonNull("t") ? Instant.parse(prev.get("t").asString()) : null;
        if (last != null && measuredAt.isBefore(last)) {
            return OnChange.HEARTBEAT;
        }
        if (Double.compare(prev.get("v").asDouble(), value) != 0) {
            return OnChange.CHANGED;
        }
        Instant storedLast = prev.hasNonNull("s") ? Instant.parse(prev.get("s").asString()) : null;
        if (storedLast == null || !measuredAt.isBefore(storedLast.plus(ON_CHANGE_HEARTBEAT))) {
            return OnChange.HEARTBEAT;
        }
        return OnChange.SKIP;
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

    private void updateState(MessageDraft d, boolean connectivityChanged, tools.jackson.databind.JsonNode priorLatest,
                             java.util.Map<String, Instant> storedAt) {
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
            // 마지막으로 telemetry에 저장한 측정 시각(상태형 ON_CHANGE 하트비트 기준, TSD-05.03)
            Instant s = storedAt.get(m.key());
            if (s != null) {
                v.put("s", s.toString());
            } else if (priorLatest.path(m.key()).hasNonNull("s")) {
                v.set("s", priorLatest.get(m.key()).get("s"));
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
                d.link == null ? null : d.link.snr(), bestGateway, connectivityChanged, validZone(d.device.timezone()));
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
                        d.scripts.isEmpty() ? null : d.scripts,
                        d.heartbeat == null ? null : java.util.Map.of("heartbeat", d.heartbeat)))
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
                d.dropped, d.envelope.virtual(), d.envelope.receivedAt(), now, storedSignature(d.envelope.signatureStatus()),
                storedFormat(d.envelope.payloadFormat()), storedOriginal(d.envelope.originalPayload()),
                d.envelope.topicAttributes() == null ? null : mapper.writeValueAsString(d.envelope.topicAttributes()));
    }

    /** 사이트 시간대: IANA 이름만 보관한다(잘못된 값이 1d 집계 SQL을 깨지 않게) */
    static String validZone(String zone) {
        if (zone == null || zone.isBlank()) {
            return null;
        }
        // 지역 이름(Asia/Seoul 등)만: '+09:00' 같은 오프셋은 PostgreSQL이 POSIX 규칙으로 부호를 거꾸로 읽는다
        return java.time.ZoneId.getAvailableZoneIds().contains(zone) ? zone : null;
    }

    /** ingress 변환 형식은 계약 값(PayloadFormat 이름)만 보관한다(모르는 값은 null, DSC-09.07) */
    static String storedFormat(String format) {
        return format != null && java.util.Set.of("JSON", "CBOR", "MSGPACK", "PROTOBUF", "AVRO", "CSV", "TEXT", "BINARY",
                "SPARKPLUG_B").contains(format) ? format : null;
    }

    /** 변환 전 원본: payload와 같은 크기 규칙(256KB 초과는 앞 4KB만, TC-ING-003) */
    private byte[] storedOriginal(byte[] original) {
        if (original == null) {
            return null;
        }
        return original.length > properties.ingest().maxPayloadBytes() ? Arrays.copyOf(original, STORED_PREFIX_BYTES) : original;
    }

    /** 서명 판정은 계약 값(VERIFIED·UNSIGNED·INVALID)만 보관한다(모르는 값은 null) */
    static String storedSignature(String status) {
        return status != null && java.util.Set.of("VERIFIED", "UNSIGNED", "INVALID").contains(status) ? status : null;
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
     * @param clockSkew    이번에 시계 오차 의심이 된 경우의 이벤트 페이로드(ING-06.04). 아니면 null
     */
    public record Stored(long rawId, CanonicalTelemetry canonical, DeviceConnectivityChanged connectivity,
                         IngestGapDetected gap, int storedRows, ClockSkewSuspected clockSkew) {
    }
}
