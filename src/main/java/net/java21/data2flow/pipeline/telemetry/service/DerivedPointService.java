package net.java21.data2flow.pipeline.telemetry.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRepository;
import net.java21.data2flow.pipeline.messaging.TelemetryPublisher;
import net.java21.data2flow.pipeline.telemetry.dto.DerivedPointDtos.Point;
import net.java21.data2flow.pipeline.telemetry.dto.DerivedPointDtos.Rejected;
import net.java21.data2flow.pipeline.telemetry.dto.DerivedPointDtos.Request;
import net.java21.data2flow.pipeline.telemetry.dto.DerivedPointDtos.Response;
import net.java21.data2flow.pipeline.telemetry.repository.DeviceStateRepository;
import net.java21.data2flow.pipeline.telemetry.repository.TelemetryRepository;
import net.java21.data2flow.pipeline.telemetry.repository.TelemetryRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 계산값 저장(API-ING-25 {@code POST /internal/pipeline/derived-points}, ENE-02.02). analytics 가상 센서가 계산한 값을 공간 가상 기기의
 * 일반 텔레메트리로 저장해 규칙·플로우·대시보드가 실측값처럼 쓰게 한다(ING 소유 규칙 유지).
 * <ul>
 *   <li>점마다 검사하고 통과한 점만 저장한다: 모르는 기기 UNKNOWN_DEVICE(캐시에 없으면 한 번 다시 읽는다), 다른 조직 기기 UNKNOWN_DEVICE,
 *       삭제·비활성 기기 DEVICE_INACTIVE, 측정 키 형식 INVALID_METRIC, 값 없음·무한대 INVALID_VALUE, 5분 넘는 미래 시각 FUTURE_TIME</li>
 *   <li>telemetry에는 품질 0(정상), flags 16(계산값)으로 덮어쓴다(같은 기기·키·시각을 다시 보내면 마지막 값). 이미 집계한 1분 구간이면
 *       재계산 구간을 남긴다. 기기 최신값(device_state.latest)도 갱신한다</li>
 *   <li>커밋 뒤 기기·시각마다 표준 메시지(EVT-ING-02, metrics[].derived = true, meta.derived = 요청 meta)를 낸다. 메시지 ID는
 *       기기·시각·키로 정해져 다시 보내도 하위에서 중복으로 걸러진다. 발행이 실패해도 저장은 유지한다(다음 계산 주기에 새 값이 간다)</li>
 * </ul>
 */
public class DerivedPointService {

    static final String SOURCE = "DERIVED";
    static final Duration FUTURE_TOLERANCE = Duration.ofMinutes(5);
    private static final Pattern KEY = Pattern.compile("^[a-z][a-z0-9_]{0,63}$");
    private static final Logger log = LoggerFactory.getLogger(DerivedPointService.class);

    private final DeviceDirectory devices;
    private final TelemetryRepository telemetry;
    private final DeviceStateRepository states;
    private final RawMessageRepository raws;
    private final TelemetryPublisher publisher;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final JsonMapper mapper = MessageCodec.newMapper();

    public DerivedPointService(DeviceDirectory devices, TelemetryRepository telemetry, DeviceStateRepository states,
                               RawMessageRepository raws, TelemetryPublisher publisher, TransactionTemplate tx, Clock clock) {
        this.devices = devices;
        this.telemetry = telemetry;
        this.states = states;
        this.raws = raws;
        this.publisher = publisher;
        this.tx = tx;
        this.clock = clock;
    }

    /** 한 기기·한 시각의 점 묶음 */
    record Group(DeviceInfo device, Instant ts, Map<String, Double> values, JsonNode meta) {
    }

    public Response store(Request request) {
        if (request.source() != null && !SOURCE.equals(request.source())) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("source", "INVALID", SOURCE)));
        }
        long org = request.orgId();
        Instant now = clock.instant();
        List<Rejected> rejected = new ArrayList<>();
        Map<String, Group> groups = new LinkedHashMap<>();
        boolean warmed = false;
        List<Point> points = request.points();
        for (int i = 0; i < points.size(); i++) {
            Point p = points.get(i);
            if (p.deviceId() == null) {
                rejected.add(new Rejected(i, "UNKNOWN_DEVICE"));
                continue;
            }
            DeviceInfo device = devices.byId(p.deviceId()).orElse(null);
            if (device == null && !warmed) {
                devices.warm(now);  // 방금 만든 가상 기기일 수 있다(바뀐 것만 읽는다)
                warmed = true;
                device = devices.byId(p.deviceId()).orElse(null);
            }
            String reason = reason(org, device, p, now);
            if (reason != null) {
                rejected.add(new Rejected(i, reason));
                continue;
            }
            Instant ts = p.ts().truncatedTo(ChronoUnit.MILLIS);
            DeviceInfo found = device;
            Group g = groups.computeIfAbsent(found.deviceId() + "@" + ts, k -> new Group(found, ts, new LinkedHashMap<>(), p.meta()));
            g.values().put(p.metricKey(), p.value());
        }
        if (groups.isEmpty()) {
            return new Response(0, rejected);
        }
        List<CanonicalTelemetry> messages = tx.execute(status -> persist(org, groups.values(), now));
        for (CanonicalTelemetry m : messages) {
            try {
                publisher.publish(m);
            } catch (RuntimeException e) {
                log.warn("계산값 표준 메시지 발행 실패(기기 {}, {}): {}", m.deviceId(), m.measuredAt(), e.getMessage());
            }
        }
        int accepted = groups.values().stream().mapToInt(g -> g.values().size()).sum();
        return new Response(accepted, rejected);
    }

    static String reason(long org, DeviceInfo device, Point p, Instant now) {
        if (device == null || device.organizationId() != org) {
            return "UNKNOWN_DEVICE";
        }
        if (device.deleted() || "INACTIVE".equalsIgnoreCase(device.status())) {
            return "DEVICE_INACTIVE";
        }
        if (p.metricKey() == null || !KEY.matcher(p.metricKey()).matches()) {
            return "INVALID_METRIC";
        }
        if (p.value() == null || p.value().isNaN() || p.value().isInfinite()) {
            return "INVALID_VALUE";
        }
        if (p.ts() == null) {
            return "INVALID_TIME";
        }
        if (p.ts().isAfter(now.plus(FUTURE_TOLERANCE))) {
            return "FUTURE_TIME";
        }
        return null;
    }

    private List<CanonicalTelemetry> persist(long org, java.util.Collection<Group> groups, Instant now) {
        List<TelemetryRow> rows = new ArrayList<>();
        Instant watermark = telemetry.currentWatermark("1m");
        List<CanonicalTelemetry> out = new ArrayList<>();
        Map<Long, List<Group>> byDevice = new LinkedHashMap<>();
        for (Group g : groups) {
            g.values().forEach((key, value) -> rows.add(new TelemetryRow(g.device().deviceId(), key, g.ts(), org, value, 0, TelemetryRow.FLAG_DERIVED,
                    g.device().virtual(), now, null)));
            byDevice.computeIfAbsent(g.device().deviceId(), k -> new ArrayList<>()).add(g);
            if (watermark != null && g.ts().isBefore(watermark)) {
                Instant from = g.ts().truncatedTo(ChronoUnit.MINUTES);
                g.values().keySet().forEach(key -> telemetry.insertDirtyRange(org, "1m", g.device().deviceId(), key, from,
                        from.plus(Duration.ofMinutes(1)), "LATE"));
            }
        }
        telemetry.insertAll(rows, true);
        for (List<Group> list : byDevice.values()) {
            Group last = list.stream().max(java.util.Comparator.comparing(Group::ts)).orElseThrow();
            ObjectNode latest = mapper.createObjectNode();
            last.values().forEach((key, value) -> latest.putObject(key).put("v", value).put("t", last.ts().toString()).put("q", 0)
                    .put("s", last.ts().toString()));
            states.upsertReceived(org, last.device().deviceId(), now, last.ts(), mapper.writeValueAsString(latest), null, null, null, null,
                    false, last.device().timezone());
            for (Group g : list) {
                out.add(message(org, g, now, raws.allocateId()));
            }
        }
        return out;
    }

    CanonicalTelemetry message(long org, Group g, Instant now, long traceId) {
        DeviceInfo d = g.device();
        List<CanonicalTelemetry.Metric> metrics = g.values().entrySet().stream()
                .map(e -> new CanonicalTelemetry.Metric(e.getKey(), e.getValue(), null, 0, true)).toList();
        Map<String, JsonNode> extra = new LinkedHashMap<>();
        ObjectNode derived = mapper.createObjectNode().put("source", SOURCE).put("estimated", true);
        if (g.meta() != null && g.meta().isObject()) {
            derived.set("meta", g.meta());
        }
        extra.put("derived", derived);
        return CanonicalTelemetry.builder()
                .messageId(messageId(d.deviceId(), g.ts(), g.values().keySet()))
                .organizationId(org)
                .sourceId(d.sourceId())
                .externalId(d.externalId())
                .deviceId(d.deviceId())
                .deviceStatus(d.telemetryStatus())
                .modelId(d.modelId() == null ? null : Long.toString(d.modelId()))
                .spaceId(d.spaceId())
                .measuredAt(g.ts())
                .receivedAt(now)
                .late(false)
                .virtual(d.virtual())
                .metrics(metrics)
                .meta(new CanonicalTelemetry.Meta(null, null, null, extra))
                .rawMessageId(traceId)
                .build();
    }

    /** 기기·시각·키로 정해지는 메시지 ID(다시 보내도 같다) */
    static UUID messageId(long deviceId, Instant ts, java.util.Collection<String> keys) {
        String seed = "derived:" + deviceId + ":" + ts + ":" + String.join(",", new java.util.TreeSet<>(keys));
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    }
}
