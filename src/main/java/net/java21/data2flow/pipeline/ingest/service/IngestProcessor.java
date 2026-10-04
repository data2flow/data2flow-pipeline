package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.SignatureStatus;
import net.java21.data2flow.contracts.message.SourceTypes;
import net.java21.data2flow.contracts.message.decoder.DecodeException;
import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import net.java21.data2flow.contracts.message.decoder.DecoderKeys;
import net.java21.data2flow.contracts.message.decoder.PayloadDecoder;
import net.java21.data2flow.contracts.message.event.IngestAlert;
import net.java21.data2flow.contracts.messaging.DedupKeys;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.common.TransientFailures;
import net.java21.data2flow.pipeline.decoder.service.ChirpStackV4Decoder;
import net.java21.data2flow.pipeline.decoder.service.DecoderRegistry;
import net.java21.data2flow.pipeline.decoder.service.IngestDecodeException;
import net.java21.data2flow.pipeline.decoder.service.ScriptPayloadDecoder;
import net.java21.data2flow.pipeline.device.domain.AutoRegisterResult;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import net.java21.data2flow.pipeline.device.domain.SourceContext;
import net.java21.data2flow.pipeline.device.domain.UnknownDevicePolicy;
import net.java21.data2flow.pipeline.device.service.AutoRegisterQuota;
import net.java21.data2flow.pipeline.device.service.CoreApiClient;
import net.java21.data2flow.pipeline.device.service.CoreDirectory;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.device.service.DeviceRuntimeCache;
import net.java21.data2flow.pipeline.device.service.SourceContextCache;
import net.java21.data2flow.pipeline.ingest.domain.LateArrivalClassifier;
import net.java21.data2flow.pipeline.ingest.domain.MeasuredAtNormalizer;
import net.java21.data2flow.pipeline.ingest.domain.MessageLimitValidator;
import net.java21.data2flow.pipeline.ingest.domain.QualityAssigner;
import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRepository;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRow;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import net.java21.data2flow.pipeline.messaging.TelemetryPublisher;
import net.java21.data2flow.pipeline.metric.domain.MetricCatalog;
import net.java21.data2flow.pipeline.metric.domain.MetricDefinition;
import net.java21.data2flow.pipeline.metric.service.MetricCatalogService;
import net.java21.data2flow.pipeline.metric.service.MetricValueMapper;
import net.java21.data2flow.pipeline.formula.service.FormulaEngine;
import net.java21.data2flow.pipeline.quality.domain.ClockSkewDetector;
import net.java21.data2flow.pipeline.script.service.ScriptRunner;
import net.java21.data2flow.pipeline.telemetry.service.RecentValues;
import net.java21.data2flow.pipeline.quality.domain.SuspectDetector;
import net.java21.data2flow.pipeline.script.domain.FailurePolicy;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import net.java21.data2flow.pipeline.script.service.ScriptOutputValidator;
import net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry;
import net.java21.data2flow.script.sandbox.ScriptSandbox;
import net.java21.data2flow.pipeline.telemetry.repository.DeviceStateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * 수집 처리 단계(ING "처리 단계"): ① 원본 보관 → ② 디코딩 → ③ 기기 식별·자동 등록 → ④ 별칭 → ⑤ TRANSFORM(모델 → 기기) →
 * ⑥ 검증·품질 → ⑦ 중복 제거 → ⑧ 저장 → ⑨ 발행. 결과는 언제나 원본과 함께 한 트랜잭션으로 기록하고({@link IngestStore}),
 * 커밋 뒤에 {@code data2flow.telemetry}와 도메인 이벤트를 발행한다. 일시 장애(DB·core·발행)는 예외로 올려 보내 호출하는 쪽이
 * 오프셋을 넘기지 않고 다시 처리하게 한다(유실 0). 같은 스트림 메시지(messageId)를 다시 읽으면 기록은 건너뛰고 저장해 둔
 * 표준 메시지만 다시 발행한다(reliability-and-ha.md §2 ⑤).
 */
public class IngestProcessor {

    private static final Logger log = LoggerFactory.getLogger(IngestProcessor.class);

    private final Deps deps;
    private final PipelineProperties properties;
    private final Clock clock;
    private final JsonMapper mapper = MessageCodec.newMapper();
    private final MessageLimitValidator limits;
    private final MeasuredAtNormalizer normalizer;
    private final LateArrivalClassifier lateClassifier;
    private final ScriptOutputValidator outputValidator;
    /** 재처리용 의심 판정(실시간 상태를 과거 값으로 흐리지 않게 따로 둔다) */
    private final SuspectDetector reprocessSuspects;

    public IngestProcessor(Deps deps, PipelineProperties properties, Clock clock) {
        this.deps = deps;
        this.properties = properties;
        this.clock = clock;
        PipelineProperties.Ingest ingest = properties.ingest();
        this.limits = new MessageLimitValidator(ingest.maxMetrics(), CanonicalTelemetry.MAX_KEY_LENGTH, ingest.maxStringBytes());
        this.normalizer = new MeasuredAtNormalizer(ingest.futureTolerance(), ingest.pastLimit());
        this.lateClassifier = new LateArrivalClassifier(ingest.lateThreshold());
        this.outputValidator = new ScriptOutputValidator(ingest.maxMetrics());
        this.reprocessSuspects = new SuspectDetector(properties.quality().stuckCount(),
                properties.quality().jumpRangeFraction());
    }

    /** 협력 객체 묶음 */
    public record Deps(SourceContextCache sources, DeviceDirectory devices, DeviceRuntimeCache runtimes,
                       AutoRegisterQuota quota, CoreDirectory core, MetricCatalogService catalogs,
                       ScriptRuntimeRegistry scripts, DecoderRegistry decoders, ScriptSandbox sandbox, DedupGuard dedup,
                       IngestStore store, RawMessageRepository raws, DeviceStateRepository states,
                       TelemetryPublisher telemetry, DomainEventPublisher events, GatewayToucher gateways,
                       IngestMetrics metrics, ClockSkewDetector skew, SuspectDetector suspects, ScriptRunner runner,
                       FormulaEngine formulas, RecentValues recent) {
    }

    /** 실시간 처리 결과 */
    public record Outcome(RawMessageStatus status, long rawMessageId, boolean published, String errorCode,
                          boolean replayed) {
    }

    /** {@code data2flow.raw}에서 읽은 메시지 하나 */
    public Outcome process(RawEnvelope envelope, int partition, long offset) {
        Optional<RawMessageRow> prior = deps.raws.findByMessageId(envelope.organizationId(), envelope.receivedAt(),
                envelope.messageId());
        if (prior.isPresent()) {
            return replay(prior.get());
        }
        return run(new MessageDraft(envelope, partition, offset, null, mapper), true);
    }

    /** 재처리(API-ING-22): 보관한 원본을 현재 디코더·스크립트로 다시 처리해 같은 행을 갱신한다 */
    public Outcome reprocess(RawMessageRow raw) {
        return reprocess(raw, null);
    }

    /**
     * 재처리(API-ING-22·23). {@code pinned}가 있으면 작업을 만들 때 고정한 스크립트 번들로 처리한다(BR-ING-12: 처리 중 새 버전이
     * 배포되어도 작업 전체가 한 버전으로 돈다). 서명 판정은 원본에 보관한 값을 그대로 쓴다(VERIFIED였던 원본은 quality 0 유지, ADR-042).
     */
    public Outcome reprocess(RawMessageRow raw, ScriptRuntimeRegistry.Plan pinned) {
        // DSC-03.05: 서명 검증에 실패해 거부한 원본은 서명 결과를 보관하지 않으므로 재처리로 되살리지 않는다
        if (SourceTypes.PLATFORM_BROKER.equals(raw.sourceType()) && raw.status() == RawMessageStatus.INVALID
                && SignatureStatus.ERROR_CODE.equals(raw.errorCode())) {
            return new Outcome(raw.status(), raw.id(), false, raw.errorCode(), true);
        }
        RawEnvelope envelope = new RawEnvelope(RawEnvelope.VERSION, raw.messageId(), raw.organizationId(), raw.sourceId(),
                raw.sourceType(), raw.topic(), raw.payload(), raw.receivedAt(), raw.ingressInstance(), raw.dedupKey(),
                raw.virtual(), null, raw.signatureStatus());
        MessageDraft draft = new MessageDraft(envelope, raw.streamPartition(), raw.streamOffset(), raw, mapper);
        draft.pinnedPlan = pinned;
        return run(draft, false);
    }

    /**
     * 일시 장애가 아닌 오류가 계속되는 메시지를 STORE_ERROR로 원본과 함께 남기고 넘어간다(DLQ STORE). 이 기록마저 실패하면
     * 예외가 올라가 다시 시도된다.
     */
    public void recordStoreError(RawEnvelope envelope, int partition, long offset, Throwable error) {
        MessageDraft d = new MessageDraft(envelope, partition, offset, null, mapper);
        d.fail(RawMessageStatus.STORE_ERROR, "ING_STORE_FAILED", String.valueOf(error.getMessage()), mapper);
        d.trace.stage("store", false, 0).put("error", String.valueOf(error));
        deps.store.persist(d, clock.instant());
        deps.metrics.processed(envelope.sourceId(), RawMessageStatus.STORE_ERROR);
    }

    private Outcome replay(RawMessageRow prior) {
        boolean published = false;
        if (prior.status() == RawMessageStatus.OK && !prior.dropped() && prior.processingTrace() != null) {
            JsonNode canonical = mapper.readTree(prior.processingTrace()).get("canonical");
            if (canonical != null && canonical.isObject()) {
                deps.telemetry.publish(MessageCodec.create().read(mapper.writeValueAsBytes(canonical),
                        CanonicalTelemetry.class));
                published = true;
            }
        }
        return new Outcome(prior.status(), prior.id(), published, prior.errorCode(), true);
    }

    private Outcome run(MessageDraft d, boolean live) {
        RawEnvelope env = d.envelope;
        try {
            stages(d);
        } catch (RuntimeException e) {
            if (TransientFailures.isTransient(e)) {
                throw e;
            }
            log.error("처리 단계 오류(원본 {}): {}", env.messageId(), e.toString(), e);
            if (!d.failed()) {
                d.fail(RawMessageStatus.DECODE_ERROR, DecodeException.RESULT_CODE, "처리 오류: " + e.getMessage(), mapper);
            }
        }
        if (!d.failed() && d.metrics.isEmpty() && !d.dropped) {
            d.trace.stage("validate", true, 0).put("note", "측정값 없음(링크만 갱신)");
        }
        IngestStore.Stored stored = deps.store.persist(d, clock.instant());
        deps.metrics.processed(env.sourceId(), d.status);
        boolean published = false;
        if (stored.canonical() != null && shouldPublish(d)) {
            deps.telemetry.publish(stored.canonical());
            published = true;
            if (live) {
                deps.metrics.latency(env.sourceId(), Duration.between(env.receivedAt(), clock.instant()));
            }
        }
        publishEvents(env.organizationId(), stored);
        if (d.status != RawMessageStatus.DUPLICATE) {
            deps.dedup.record(d.partition, env.organizationId(), d.dedupKey, env.receivedAt());
        }
        if (d.status == RawMessageStatus.OK && !d.dropped && d.device != null && !d.reprocessing()) {
            Map<String, Double> values = new LinkedHashMap<>();
            d.metrics.forEach(m -> values.put(m.key(), m.value()));
            deps.recent.record(d.device.deviceId(), d.measuredAt, values, d.late);
        }
        if (d.status == RawMessageStatus.OK && d.link != null && d.link.gateways() != null) {
            d.link.gateways().forEach(g -> deps.gateways.record(env.organizationId(), env.sourceId(), g.eui(),
                    env.receivedAt()));
        }
        return new Outcome(d.status, stored.rawId(), published, d.errorCode, false);
    }

    /**
     * 표준 메시지를 {@code data2flow.telemetry}에 낼지. 실시간은 언제나 낸다. 기간 재처리 작업(고정 번들)은 내지 않는다 — 플로우가 과거 값에
     * 다시 반응하지 않게 하고, 바뀐 구간은 집계 재계산과 {@code ingest.reprocess.finished}로 알린다(ADR-048 후속 결정). 단건 재처리
     * (API-ING-22)는 전에 발행하지 못한(실패한) 원본만 낸다(이미 낸 메시지를 두 번 내지 않음).
     */
    private static boolean shouldPublish(MessageDraft d) {
        if (!d.reprocessing()) {
            return true;
        }
        return d.pinnedPlan == null && d.existing.status() != RawMessageStatus.OK;
    }

    private void publishEvents(long organizationId, IngestStore.Stored stored) {
        if (stored.connectivity() != null) {
            deps.events.publish(EventType.DEVICE_CONNECTIVITY_CHANGED, organizationId, stored.connectivity());
        }
        if (stored.gap() != null) {
            deps.events.publish(EventType.INGEST_GAP_DETECTED, organizationId, stored.gap());
        }
        if (stored.clockSkew() != null) {
            deps.events.publish(EventType.INGEST_CLOCK_SKEW_SUSPECTED, organizationId, stored.clockSkew());
        }
    }

    private void stages(MessageDraft d) {
        RawEnvelope env = d.envelope;
        if (!SourceTypes.KNOWN.contains(env.sourceType())) {
            d.fail(RawMessageStatus.INVALID, "ING_SOURCE_TYPE_UNKNOWN", "모르는 소스 유형입니다: " + env.sourceType(), mapper);
            d.errorDetail.put("sourceType", env.sourceType());
            return;
        }
        // ① 원본 크기(BR-ING-10)
        if (env.payload().length > properties.ingest().maxPayloadBytes()) {
            d.fail(RawMessageStatus.INVALID, MessageLimitValidator.PAYLOAD_EXCEEDED,
                    "payload가 " + properties.ingest().maxPayloadBytes() + "바이트를 넘습니다: " + env.payload().length, mapper);
            return;
        }
        // DSC-03.03: ingress가 서명 키가 있는 기기의 서명 없음·불일치로 판정한 메시지는 디코딩 없이 거부한다
        if (SourceTypes.PLATFORM_BROKER.equals(env.sourceType()) && SignatureStatus.INVALID.equals(env.signatureStatus())) {
            signatureRejected(d, "서명 키가 있는 기기의 메시지인데 서명이 없거나 맞지 않습니다");
            return;
        }
        Optional<SourceContext> source = deps.sources.get(env.sourceId());
        if (source.isEmpty()) {
            d.fail(RawMessageStatus.DECODE_ERROR, DecodeException.RESULT_CODE, "데이터 소스 설정을 찾을 수 없습니다: "
                    + env.sourceId(), mapper);
            return;
        }
        d.source = source.get();
        boolean contentKey = d.dedupKey.startsWith("sha256:");
        // ⑦-1 중복(측정 시각·카운터가 있는 키는 디코딩 전에 바로)
        if (!contentKey && isDuplicate(d, d.dedupKey)) {
            return;
        }
        // ② 디코딩
        ScriptRuntimeRegistry.Plan plan = null;
        DecodedUplink uplink;
        long started = System.nanoTime();
        PayloadDecoder decoder = null;
        try {
            if (requiresPlan(d.source)) {
                plan = planFor(d);
            }
            decoder = deps.decoders.select(d.source, plan == null ? new ScriptRuntimeRegistry.Plan(
                    net.java21.data2flow.pipeline.script.domain.RuntimeBundle.EMPTY) : plan);
            uplink = decoder.decode(env, d.source.decoderConfig());
            d.decoder = new CanonicalTelemetry.DecoderRef(decoder.key(), decoder.version());
            d.trace.decoder(decoder.key(), decoder.version());
            ObjectNode info = d.trace.stage("decode", true, ms(started));
            info.put("decoder", decoder.key());
            info.put("version", decoder.version());
            if (decoder instanceof ScriptPayloadDecoder sd) {
                d.trace.script(sd.script().scriptId(), sd.script().versionNo());
                d.scripts.add(new CanonicalTelemetry.ScriptRef(sd.script().scriptId(), sd.script().versionNo()));
            }
        } catch (IngestDecodeException e) {
            decodeFailed(d, e.errorCode(), e.getMessage(), e.decoderKey(), e.detail(), started);
            return;
        } catch (DecodeException e) {
            decodeFailed(d, DecodeException.RESULT_CODE, e.getMessage(), e.decoderKey(), null, started);
            return;
        }
        // 한도(ING-07.01)
        Optional<MessageLimitValidator.Violation> violation = limits.check(uplink.values());
        if (violation.isPresent()) {
            d.fail(RawMessageStatus.INVALID, violation.get().code(), violation.get().message(), mapper);
            if (violation.get().key() != null) {
                d.errorDetail.put("key", violation.get().key());
            }
            return;
        }
        d.externalId = DeviceDirectory.normalize(uplink.externalId());
        d.link = uplink.link();
        d.tags = uplink.tags();
        // ③ 기기 식별·자동 등록
        if (!identify(d, uplink)) {
            return;
        }
        // ⑦-2 값만 있는 payload: 수신 시각 버킷 키(BR-ING-07, 버킷 폭 = 기기 보고 주기 ÷ 2)
        if (contentKey) {
            if (uplink.measuredAt() == null && (uplink.link() == null || uplink.link().frameCounter() == null)) {
                int interval = d.device.effectiveIntervalSec(properties.offline().defaultIntervalSec());
                d.dedupKey = DedupKeys.contentInBucket(env.sourceId(), env.topic(), env.payload(), env.receivedAt(),
                        Duration.ofMillis(interval * 500L));
            }
            if (isDuplicate(d, d.dedupKey)) {
                return;
            }
        }
        // ④ 별칭·값 변환(ING-04.03, ING-02.06)
        MetricCatalog catalog = deps.catalogs.catalog(env.organizationId());
        if (!mapValues(d, uplink, catalog)) {
            return;
        }
        // 측정 시각 보정(ING-02.05)·늦은 도착(ING-06.03)
        boolean keepOriginal = env.simRunId() != null || SourceTypes.EDGE.equals(env.sourceType());
        MeasuredAtNormalizer.Result time = normalizer.normalize(uplink.measuredAt(), env.receivedAt(), keepOriginal);
        d.measuredAt = time.measuredAt();
        d.timeCorrected = time.corrected();
        if (time.corrected()) {
            d.trace.stage("time", true, 0).put("corrected", time.reason());
        }
        d.late = lateClassifier.isLate(d.measuredAt, env.receivedAt());
        // 시계 오차(ING-06.04): 보정 전 기기 시각으로. 가상·엣지 버퍼·재처리는 실시간 기기 시계가 아니므로 보지 않는다
        if (!keepOriginal && !d.reprocessing()) {
            d.clockSkew = deps.skew.observe(d.device.deviceId(), uplink.measuredAt(), env.receivedAt());
        }
        heartbeat(d);
        // ⑤ TRANSFORM(모델 → 기기, BR-SCR-03)
        if (!transform(d, plan)) {
            return;
        }
        if (d.dropped) {
            return;
        }
        // ⑥ 검증·품질(ING-04.01·04.02, BR-ING-03·06)
        qualify(d, catalog);
    }

    /** 고정한 번들(재처리 작업)이 있으면 그것, 아니면 조직의 현재 계획 */
    private ScriptRuntimeRegistry.Plan planFor(MessageDraft d) {
        return d.pinnedPlan != null ? d.pinnedPlan : deps.scripts.plan(d.envelope.organizationId());
    }

    private static boolean requiresPlan(SourceContext source) {
        String key = source.decoderKey();
        return key != null && (key.equalsIgnoreCase("script") || DecoderKeys.parseScript(key).isPresent());
    }

    private void decodeFailed(MessageDraft d, String code, String message, String decoderKey, Object detail, long started) {
        d.fail(RawMessageStatus.DECODE_ERROR, code, message, mapper);
        d.errorDetail.put("decoder", decoderKey);
        if (detail instanceof Map<?, ?> map) {
            map.forEach((k, v) -> {
                if (v != null) {
                    d.errorDetail.put(k.toString(), v.toString());
                }
            });
        }
        ObjectNode info = d.trace.stage("decode", false, ms(started));
        info.put("decoder", decoderKey);
        info.put("error", code);
    }

    private boolean isDuplicate(MessageDraft d, String key) {
        RawEnvelope env = d.envelope;
        Duration window = properties.ingest().dedupWindow();
        // 재처리는 자기 자신을 파티션 캐시에서 찾지 않도록 DB(자기 messageId 제외)만 본다
        boolean dup = (!d.reprocessing() && deps.dedup.seen(d.partition, env.organizationId(), key, env.receivedAt()))
                || deps.raws.existsDuplicate(env.organizationId(), key, env.receivedAt().minus(window),
                env.receivedAt().plus(window), env.messageId());
        if (dup) {
            d.status = RawMessageStatus.DUPLICATE;
            d.trace.stage("dedup", true, 0).put("duplicateOf", key);
        }
        return dup;
    }

    private boolean identify(MessageDraft d, DecodedUplink uplink) {
        RawEnvelope env = d.envelope;
        long started = System.nanoTime();
        Optional<DeviceInfo> found = deps.devices.find(env.sourceId(), d.externalId);
        boolean autoRegistered = false;
        if (found.isEmpty()) {
            if (d.source.unknownDevicePolicy() == UnknownDevicePolicy.REJECT) {
                d.fail(RawMessageStatus.UNKNOWN_DEVICE_REJECTED, null, "미등록 기기를 거부하는 소스입니다: " + d.externalId, mapper);
                d.trace.stage("identify", false, ms(started)).put("policy", "REJECT");
                return false;
            }
            int limit = d.source.autoRegisterHourlyLimit() > 0 ? d.source.autoRegisterHourlyLimit()
                    : properties.ingest().autoRegisterHourlyLimit();
            AutoRegisterQuota.Decision decision = deps.quota.tryAcquire(env.sourceId(), limit);
            if (!decision.allowed()) {
                quotaRejected(d, limit, decision.firstRejection(), started);
                return false;
            }
            AutoRegisterResult result = deps.core.autoRegister(new CoreDirectory.AutoRegisterCommand(env.organizationId(),
                    env.sourceId(), d.externalId, autoName(d), sourceMeta(d, uplink), env.receivedAt(),
                    uplink.values().stream().map(DecodedValue::key).distinct().toList()));
            if (result.outcome() == AutoRegisterResult.Outcome.QUOTA_EXCEEDED) {
                quotaRejected(d, limit, deps.quota.markExceeded(env.sourceId()), started);
                return false;
            }
            if (result.outcome() == AutoRegisterResult.Outcome.REJECTED) {
                d.fail(RawMessageStatus.UNKNOWN_DEVICE_REJECTED, null, "core가 자동 등록을 거부했습니다(DEVICE_REJECTED)", mapper);
                d.trace.stage("identify", false, ms(started)).put("policy", "REJECTED_BY_CORE");
                return false;
            }
            DeviceInfo pending = deps.devices.find(env.sourceId(), d.externalId).orElse(null);
            if (pending == null || pending.deviceId() != result.deviceId()) {
                pending = new DeviceInfo(result.deviceId(), env.organizationId(), env.sourceId(), d.externalId,
                        autoName(d), "PENDING", d.source.defaultModelId(), null, d.source.defaultSpaceId(), env.virtual(),
                        false, null, null, null, null, null);
                deps.devices.put(pending);
            }
            found = Optional.of(pending);
            autoRegistered = true;
        }
        DeviceInfo device = found.get();
        if (device.ignored()) {
            d.fail(RawMessageStatus.UNKNOWN_DEVICE_REJECTED, "ING_DEVICE_IGNORED", "무시 목록의 기기입니다: " + d.externalId,
                    mapper);
            d.trace.stage("identify", false, ms(started)).put("ignored", true);
            return false;
        }
        d.device = device;
        // DSC-03.05: 승인된 플랫폼 브로커 기기는 서명이 맞는 메시지(VERIFIED)만 받는다. ingress 키 캐시가 늦어도
        // 승인 뒤 서명 없는 메시지·폐기된 키의 메시지가 정상으로 들어오지 않게 여기서 한 번 더 막는다. 재처리는 서명 결과가 없어
        // 거부하지 않고 qualify()에서 미검증(quality 2)으로 둔다
        if (SourceTypes.PLATFORM_BROKER.equals(env.sourceType()) && !d.reprocessing()
                && device.telemetryStatus() != CanonicalTelemetry.DeviceStatus.PENDING
                && !SignatureStatus.VERIFIED.equals(env.signatureStatus())) {
            d.trace.stage("identify", true, ms(started)).put("deviceId", device.deviceId());
            signatureRejected(d, "승인된 기기의 메시지는 서명이 맞아야 합니다(" + env.signatureStatus() + ")");
            return false;
        }
        ObjectNode info = d.trace.stage("identify", true, ms(started));
        info.put("deviceId", device.deviceId());
        info.put("status", device.telemetryStatus().name());
        info.put("autoRegistered", autoRegistered);
        return true;
    }

    /** 서명 거부(DSC-03.03·03.05): 원본만 INVALID + DEVICE_SIGNATURE_INVALID로 남기고 지표를 올린다 */
    private void signatureRejected(MessageDraft d, String message) {
        RawEnvelope env = d.envelope;
        if (d.externalId == null && env.topic() != null) {
            String[] parts = env.topic().split("/");
            if (parts.length >= 2 && "devices".equals(parts[0]) && !parts[1].isBlank()) {
                d.externalId = DeviceDirectory.normalize(parts[1]);
            }
        }
        d.fail(RawMessageStatus.INVALID, SignatureStatus.ERROR_CODE, message, mapper);
        if (env.signatureStatus() != null) {
            d.errorDetail.put("signatureStatus", env.signatureStatus());
        }
        d.trace.stage("signature", false, 0).put("signatureStatus", String.valueOf(env.signatureStatus()));
        deps.metrics.signatureRejected(env.sourceId());
    }

    private void quotaRejected(MessageDraft d, int limit, boolean firstRejection, long started) {
        d.fail(RawMessageStatus.UNKNOWN_DEVICE_REJECTED, IngestAlert.AUTO_REGISTER_QUOTA,
                "소스당 시간당 자동 등록 한도(" + limit + "대)를 넘었습니다", mapper);
        d.trace.stage("identify", false, ms(started)).put("quota", limit);
        if (firstRejection) {
            deps.events.publish(EventType.INGEST_ALERT_RAISED, d.envelope.organizationId(), IngestAlert.of(
                    IngestAlert.AUTO_REGISTER_QUOTA, IngestAlert.Level.WARNING, d.envelope.sourceId(), limit + 1, limit,
                    List.of("토픽 설정 오류로 처음 보는 기기가 계속 생기는지 확인하세요"), clock.instant()));
        }
    }

    private String autoName(MessageDraft d) {
        Map<String, String> meta = DecoderKeys.CHIRPSTACK_V4.equals(d.decoder == null ? null : d.decoder.key())
                ? ChirpStackV4Decoder.deviceMeta(mapper, d.envelope.payload()) : Map.of();
        return meta.getOrDefault("deviceName", d.externalId);
    }

    private JsonNode sourceMeta(MessageDraft d, DecodedUplink uplink) {
        ObjectNode meta = mapper.createObjectNode();
        ObjectNode tags = meta.putObject("tags");
        uplink.tags().forEach(tags::put);
        if (DecoderKeys.CHIRPSTACK_V4.equals(d.decoder == null ? null : d.decoder.key())) {
            ChirpStackV4Decoder.deviceMeta(mapper, d.envelope.payload()).forEach(meta::put);
        }
        meta.put("topic", d.envelope.topic());
        return meta;
    }

    private boolean mapValues(MessageDraft d, DecodedUplink uplink, MetricCatalog catalog) {
        Map<String, MessageDraft.MetricOut> out = new LinkedHashMap<>();
        for (DecodedValue v : uplink.values()) {
            String key = catalog.canonicalKey(v.key());
            if (!ScriptOutputValidator.METRIC_KEY.matcher(key).matches()) {
                d.trace.skipped(v.key(), "KEY_INVALID");
                continue;
            }
            MetricDefinition def = catalog.definition(key);
            if (def != null && def.ignored()) {
                d.trace.skipped(key, "IGNORED");
                continue;
            }
            OptionalDouble number = MetricValueMapper.toNumber(v, def);
            if (number.isEmpty()) {
                d.fail(RawMessageStatus.INVALID, IngestDecodeException.VALUE_NOT_NUMERIC,
                        "숫자로 바꿀 수 없는 값이 있습니다: " + v.key() + "=" + v.value(), mapper);
                d.errorDetail.put("key", v.key());
                return false;
            }
            String unit = def != null && def.unit() != null ? def.unit() : v.unit();
            out.putIfAbsent(key, new MessageDraft.MetricOut(key, number.getAsDouble(), unit, 0, false, v.key()));
        }
        d.metrics.addAll(out.values());
        return true;
    }

    private boolean transform(MessageDraft d, ScriptRuntimeRegistry.Plan plan) {
        ScriptRuntimeRegistry.Plan p = plan != null ? plan : planFor(d);
        List<ScriptRuntimeRegistry.Step> steps = p.transforms(d.device.modelId(), d.device.deviceId());
        if (!steps.isEmpty() && !transformSteps(d, p, steps)) {
            return false;
        }
        if (!d.dropped) {
            formulas(d, p);
        }
        return true;
    }

    /**
     * 수식 파생 항목(SCR-01.06, BR-SCR-03: TRANSFORM 마지막). 결과 키는 파생(derived=true)이고 처음 보는 키는 미검증으로 등록된다
     * (BR-SCR-07). 입력 값이 없는 수식은 그 메시지에서 건너뛴다.
     */
    private void formulas(MessageDraft d, ScriptRuntimeRegistry.Plan p) {
        List<net.java21.data2flow.pipeline.script.domain.RuntimeBundle.Formula> list = FormulaEngine.applicable(p.bundle(),
                d.device.modelId(), d.device.deviceId(), d.device.spaceId());
        if (list.isEmpty()) {
            return;
        }
        long started = System.nanoTime();
        Map<String, Duration> windows = deps.formulas.windows(list);
        Map<String, Double> values = new LinkedHashMap<>();
        d.metrics.forEach(m -> values.put(m.key(), m.value()));
        FormulaEngine.Result result = deps.formulas.evaluate(p.bundle(), list, values, execution(d,
                windows.isEmpty() || d.late ? null : deps.recent.window(d.device.deviceId(), windows.keySet(), d.measuredAt)));
        for (FormulaEngine.Derived x : result.derived()) {
            d.metrics.removeIf(m -> m.key().equals(x.key()));
            d.metrics.add(new MessageDraft.MetricOut(x.key(), x.value(), x.unit(), 0, true, x.key()));
        }
        ObjectNode info = d.trace.stage("formula", result.errors().isEmpty(), ms(started));
        info.put("count", result.derived().size());
        if (!result.errors().isEmpty()) {
            info.put("errors", String.join("; ", result.errors()));
        }
    }

    private ScriptRunner.Execution execution(MessageDraft d, Map<String, List<double[]>> window) {
        return new ScriptRunner.Execution(d.envelope.organizationId(), d.device == null ? null : d.device.deviceId(),
                d.existing == null ? null : d.existing.id(), clock.instant(), d.measuredAt, window, !d.reprocessing());
    }

    private boolean transformSteps(MessageDraft d, ScriptRuntimeRegistry.Plan p, List<ScriptRuntimeRegistry.Step> steps) {
        ObjectNode msg = draftMessage(d);
        ObjectNode ctx = mapper.createObjectNode();
        ObjectNode device = ctx.putObject("device");
        device.put("id", d.device.deviceId());
        device.put("name", d.device.name());
        device.put("modelId", d.device.modelId() == null ? null : Long.toString(d.device.modelId()));
        if (d.device.spaceId() != null) {
            device.put("spaceId", d.device.spaceId());
        }
        device.set("attributes", deps.runtimes.get(d.device.deviceId()).attributes());
        ctx.set("last", lastValues(d));
        for (ScriptRuntimeRegistry.Step step : steps) {
            ctx.set("config", step.script().config());
            long id = step.script().scriptId();
            int version = step.script().versionNo();
            Map<String, Duration> windowKeys = ScriptRunner.windowKeys(step.script().code());
            Map<String, List<double[]>> window = windowKeys.isEmpty() || d.late ? null
                    : deps.recent.window(d.device.deviceId(), windowKeys.keySet(), d.measuredAt);
            ScriptOutcome outcome = deps.runner.run(p.bundle(), step.script(), ScriptKind.TRANSFORM,
                    mapper.writeValueAsString(msg), ctx, execution(d, window));
            deps.metrics.script(id, version, outcome.durationMs(), outcome.ok());
            d.trace.script(id, version);
            d.scripts.add(new CanonicalTelemetry.ScriptRef(id, version));
            ObjectNode info = d.trace.stage("script", outcome.ok(), outcome.durationMs());
            info.put("scriptId", id);
            info.put("version", version);
            info.put("scope", step.scope());
            info.put("configRevision", step.script().configRevision());
            String errorCode = outcome.ok() ? null : outcome.failure().code().name();
            String errorMessage = outcome.ok() ? null : outcome.failure().message();
            if (outcome.ok() && !outcome.returnedNull()) {
                try {
                    List<ScriptOutputValidator.Metric> metrics = outputValidator.transformMetrics(outcome.output());
                    Instant measuredAt = outputValidator.transformMeasuredAt(outcome.output());
                    msg = applyTransform(d, msg, metrics, measuredAt, outcome.output());
                    continue;
                } catch (ScriptOutputValidator.OutputContractException e) {
                    errorCode = "SCRIPT_OUTPUT_INVALID";
                    errorMessage = e.getMessage();
                }
            } else if (outcome.ok()) {
                d.dropped = true;
                info.put("dropped", true);
                return true;
            }
            info.put("error", errorCode);
            info.put("message", errorMessage);
            if (outcome.failure() != null && outcome.failure().line() != null) {
                info.put("line", outcome.failure().line());
                info.put("col", outcome.failure().col());
            }
            info.put("policy", step.failurePolicy().name());
            if (step.failurePolicy() == FailurePolicy.FAIL_CLOSED) {
                d.fail(RawMessageStatus.SCRIPT_ERROR, errorCode, errorMessage, mapper);
                d.errorDetail.put("scriptId", id);
                d.errorDetail.put("version", version);
                if (outcome.failure() != null && outcome.failure().line() != null) {
                    d.errorDetail.put("line", outcome.failure().line());
                    d.errorDetail.put("col", outcome.failure().col());
                }
                return false;
            }
            // fail-open: 이 스크립트의 입력을 그대로 다음 단계로(BR-SCR-04)
        }
        return true;
    }

    private ObjectNode draftMessage(MessageDraft d) {
        ObjectNode msg = mapper.createObjectNode();
        msg.put("v", 1);
        msg.put("messageId", d.telemetryMessageId().toString());
        msg.put("organizationId", d.envelope.organizationId());
        msg.put("sourceId", d.envelope.sourceId());
        msg.put("externalId", d.externalId);
        msg.put("deviceId", d.device.deviceId());
        msg.put("deviceStatus", d.device.telemetryStatus().name());
        if (d.device.modelId() != null) {
            msg.put("modelId", Long.toString(d.device.modelId()));
        }
        if (d.device.spaceId() != null) {
            msg.put("spaceId", d.device.spaceId());
        }
        msg.put("measuredAt", d.measuredAt.toString());
        msg.put("receivedAt", d.envelope.receivedAt().toString());
        msg.put("late", d.late);
        msg.put("virtual", d.envelope.virtual());
        ArrayNode metrics = msg.putArray("metrics");
        for (MessageDraft.MetricOut m : d.metrics) {
            ObjectNode n = metrics.addObject();
            n.put("key", m.key());
            n.put("value", m.value());
            if (m.unit() != null) {
                n.put("unit", m.unit());
            }
            n.put("quality", 0);
        }
        if (d.link != null) {
            msg.set("link", mapper.valueToTree(d.link));
        }
        ObjectNode meta = msg.putObject("meta");
        ObjectNode tags = meta.putObject("tags");
        d.tags.forEach(tags::put);
        return msg;
    }

    /** TRANSFORM이 바꿀 수 있는 것은 metrics·measuredAt·link·meta뿐이다(SCR-api §3.2). 새 키는 파생 항목(BR-SCR-07) */
    private ObjectNode applyTransform(MessageDraft d, ObjectNode msg, List<ScriptOutputValidator.Metric> metrics,
                                      Instant measuredAt, JsonNode output) {
        Map<String, MessageDraft.MetricOut> before = new LinkedHashMap<>();
        d.metrics.forEach(m -> before.put(m.key(), m));
        d.metrics.clear();
        for (ScriptOutputValidator.Metric m : metrics) {
            MessageDraft.MetricOut prev = before.get(m.key());
            d.metrics.add(new MessageDraft.MetricOut(m.key(), m.value(), m.unit() != null ? m.unit()
                    : prev == null ? null : prev.unit(), 0, prev == null || prev.derived(),
                    prev == null ? m.key() : prev.originalKey()));
        }
        if (measuredAt != null) {
            d.measuredAt = measuredAt;
            d.late = lateClassifier.isLate(measuredAt, d.envelope.receivedAt());
        }
        if (output.has("link") && output.get("link").isObject()) {
            JsonNode l = output.get("link");
            d.link = new CanonicalTelemetry.Link(l.path("rssi").isNumber() ? l.get("rssi").asDouble() : null,
                    l.path("snr").isNumber() ? l.get("snr").asDouble() : null,
                    l.path("frameCounter").isNumber() ? l.get("frameCounter").asLong() : null,
                    d.link == null ? null : d.link.gateways());
        }
        ObjectNode next = draftMessage(d);
        if (output.has("meta") && output.get("meta").isObject()) {
            next.set("meta", output.get("meta"));
        }
        return next;
    }

    private JsonNode lastValues(MessageDraft d) {
        ObjectNode last = mapper.createObjectNode();
        deps.states.findLatest(d.envelope.organizationId(), d.device.deviceId()).ifPresent(json -> {
            JsonNode latest = mapper.readTree(json);
            latest.properties().forEach(e -> {
                ObjectNode v = last.putObject(e.getKey());
                v.set("value", e.getValue().get("v"));
                v.set("measuredAt", e.getValue().get("t"));
            });
        });
        return last;
    }

    private void qualify(MessageDraft d, MetricCatalog catalog) {
        Map<String, Double> unknown = new LinkedHashMap<>();
        for (MessageDraft.MetricOut m : d.metrics) {
            if (catalog.definition(m.key()) == null && !deps.catalogs.isRegisteredUnverified(d.envelope.organizationId(), m.key())) {
                unknown.put(m.key(), m.value());
            }
        }
        if (!unknown.isEmpty()) {
            deps.catalogs.registerUnverified(d.envelope.organizationId(), d.device.deviceId(), unknown);
            d.trace.stage("metrics", true, 0).put("unverifiedRegistered", String.join(",", unknown.keySet()));
        }
        // 승인 대기 기기는 격리(quality 2). 재처리는 보관한 서명 판정이 VERIFIED일 때만 정상 품질을 유지한다(ADR-042, M5 이전 원본은 null)
        boolean quarantine = SourceTypes.PLATFORM_BROKER.equals(d.envelope.sourceType())
                && (d.device.telemetryStatus() == CanonicalTelemetry.DeviceStatus.PENDING
                || (d.reprocessing() && !SignatureStatus.VERIFIED.equals(d.envelope.signatureStatus())));
        boolean forecast = SourceTypes.KMA_WEATHER.equals(d.envelope.sourceType())
                && d.envelope.topic() != null && d.envelope.topic().contains("forecast");
        List<MessageDraft.MetricOut> qualified = d.metrics.stream().map(m -> {
            MetricDefinition def = catalog.definition(m.key());
            boolean unverified = def == null || def.unverified() || quarantine;
            boolean outOfRange = def != null && def.outOfRange(m.value());
            SuspectDetector detector = d.reprocessing() ? reprocessSuspects : deps.suspects;
            boolean suspect = !forecast && detector.observe(d.device.deviceId(), m.key(), def, m.value(), d.measuredAt, d.late);
            return m.withQuality(QualityAssigner.assign(d.timeCorrected, outOfRange, suspect, unverified, forecast));
        }).toList();
        d.metrics.clear();
        d.metrics.addAll(qualified);
        if (d.metrics.size() > properties.ingest().maxMetrics()) {
            d.fail(RawMessageStatus.INVALID, MessageLimitValidator.METRICS_EXCEEDED,
                    d.metrics.size() + "/" + properties.ingest().maxMetrics(), mapper);
        }
    }

    /**
     * 하트비트 카나리(ING-07.05, EVT-ING-07): 시스템 가상 기기 {@code __heartbeat__}의 메시지에 pipeline 통과 시각을
     * {@code meta.heartbeat.stages[]}에 덧붙인다. 앞 단계(시뮬레이터·ingress)가 payload의 {@code meta.heartbeat}에 남긴 값은 잇고,
     * 없으면 수신 시각을 ingress 단계로 넣는다.
     */
    private void heartbeat(MessageDraft d) {
        if (!HEARTBEAT_DEVICE.equals(d.externalId)) {
            return;
        }
        ObjectNode hb = mapper.createObjectNode();
        ArrayNode stages = hb.putArray("stages");
        try {
            JsonNode prior = mapper.readTree(d.envelope.payload()).path("meta").path("heartbeat");
            if (prior.hasNonNull("sentAt")) {
                hb.set("sentAt", prior.get("sentAt"));
            }
            prior.path("stages").forEach(stages::add);
        } catch (RuntimeException e) {
            // JSON이 아닌 payload: 앞 단계 기록 없음
        }
        if (!hb.has("sentAt")) {
            hb.put("sentAt", d.measuredAt.toString());
        }
        boolean ingress = false;
        for (JsonNode st : stages) {
            ingress |= "ingress".equals(st.path("name").asString(""));
        }
        if (!ingress) {
            stages.addObject().put("name", "ingress").put("at", d.envelope.receivedAt().toString());
        }
        stages.addObject().put("name", "pipeline").put("at", clock.instant().toString());
        d.heartbeat = hb;
    }

    /** 하트비트 카나리 시스템 가상 기기의 외부 ID(reliability-and-ha.md §6) */
    public static final String HEARTBEAT_DEVICE = "__heartbeat__";

    private static double ms(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000.0;
    }

    /** core-api가 4xx로 거부한 호출을 처리 결과로 바꾼다(일시 장애가 아님) */
    static boolean isRejected(Throwable e) {
        return e instanceof CoreApiClient.CoreRejectedException;
    }
}
