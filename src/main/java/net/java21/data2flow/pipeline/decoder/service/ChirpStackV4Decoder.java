package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.RawEnvelope;
import net.java21.data2flow.contracts.message.decoder.DecodeException;
import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import net.java21.data2flow.contracts.message.decoder.DecoderKeys;
import net.java21.data2flow.contracts.message.decoder.PayloadDecoder;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 기본 디코더 {@code chirpstack-v4}(ING-02.02, ADR-001): ChirpStack v4 {@code application/+/device/+/event/up} JSON에서
 * devEui·tags·object·rxInfo·fCnt를 꺼낸다.
 *
 * <ul>
 *   <li>externalId: {@code deviceInfo.devEui}(없으면 토픽의 devEui), 소문자</li>
 *   <li>measuredAt: 게이트웨이 수신 시각 {@code rxInfo[].nsTime} 중 가장 이른 값, 없으면 {@code time}, 둘 다 없으면 null(수신 시각 + 품질 4)</li>
 *   <li>측정값: {@code object}의 숫자·불린·글자 값(중첩 객체·배열은 건너뜀). {@code object}가 없고 {@code data}가 있으면
 *       기기 프로필 이름으로 Milesight 기본 디코더({@link MilesightCodec})를 쓴다. join·status 이벤트는 측정값 0개</li>
 *   <li>link: 가장 센 게이트웨이의 rssi·snr, fCnt, 게이트웨이별 품질</li>
 * </ul>
 */
public class ChirpStackV4Decoder implements PayloadDecoder {

    private static final Pattern TOPIC_DEV_EUI = Pattern.compile("application/[^/]+/device/([0-9a-fA-F]{16})/event/.*");

    private final JsonMapper mapper = MessageCodec.newMapper();

    @Override
    public String key() {
        return DecoderKeys.CHIRPSTACK_V4;
    }

    @Override
    public String version() {
        return "1";
    }

    @Override
    public DecodedUplink decode(RawEnvelope raw, JsonNode config) throws DecodeException {
        JsonNode root = parse(raw.payload());
        JsonNode deviceInfo = root.path("deviceInfo");
        String devEui = text(deviceInfo, "devEui");
        if (devEui == null && raw.topic() != null) {
            Matcher m = TOPIC_DEV_EUI.matcher(raw.topic());
            if (m.matches()) {
                devEui = m.group(1);
            }
        }
        if (devEui == null || devEui.isBlank()) {
            throw new IngestDecodeException(key(), IngestDecodeException.EXTERNAL_ID_MISSING, "devEui가 없습니다");
        }
        List<DecodedValue> values = values(root, deviceInfo, config);
        Map<String, String> tags = new LinkedHashMap<>();
        deviceInfo.path("tags").properties().forEach(e -> {
            if (e.getValue().isValueNode() && !e.getValue().isNull()) {
                tags.put(e.getKey(), e.getValue().asString());
            }
        });
        return new DecodedUplink(devEui.toLowerCase(Locale.ROOT), measuredAt(root), values, link(root), tags);
    }

    /** 자동 등록 이름 후보({@code deviceInfo.deviceName})와 기기 프로필 이름. 없으면 null */
    public static Map<String, String> deviceMeta(JsonMapper mapper, byte[] payload) {
        Map<String, String> meta = new LinkedHashMap<>();
        try {
            JsonNode info = mapper.readTree(payload).path("deviceInfo");
            for (String field : List.of("deviceName", "deviceProfileName", "applicationName")) {
                if (info.hasNonNull(field)) {
                    meta.put(field, info.get(field).asString());
                }
            }
        } catch (RuntimeException ignored) {
            // 디코딩 단계에서 이미 걸러진다
        }
        return meta;
    }

    private JsonNode parse(byte[] payload) throws IngestDecodeException {
        try {
            JsonNode root = mapper.readTree(payload);
            if (root == null || !root.isObject()) {
                throw IngestDecodeException.failed(key(), "ChirpStack 업링크는 JSON 객체여야 합니다");
            }
            return root;
        } catch (IngestDecodeException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IngestDecodeException(key(), IngestDecodeException.RESULT_CODE,
                    "JSON을 해석하지 못했습니다: " + firstLine(e.getMessage()), null, e);
        }
    }

    private List<DecodedValue> values(JsonNode root, JsonNode deviceInfo, JsonNode config) throws IngestDecodeException {
        JsonNode object = root.get("object");
        List<DecodedValue> values = new ArrayList<>();
        if (object != null && object.isObject() && !object.isEmpty()) {
            object.properties().forEach(e -> {
                JsonNode v = e.getValue();
                if (v.isNumber()) {
                    values.add(new DecodedValue(e.getKey(), v.asDouble(), null));
                } else if (v.isBoolean()) {
                    values.add(new DecodedValue(e.getKey(), v.asBoolean(), null));
                } else if (v.isString()) {
                    values.add(new DecodedValue(e.getKey(), v.asString(), null));
                }
            });
            return values;
        }
        String data = text(root, "data");
        if (data == null || data.isEmpty()) {
            return values; // join·status 이벤트 등: 링크만 갱신
        }
        String model = config != null && config.hasNonNull("codec") ? MilesightCodec.model(config.get("codec").asString())
                : MilesightCodec.model(text(deviceInfo, "deviceProfileName"));
        try {
            return MilesightCodec.decode(Base64.getDecoder().decode(data), model);
        } catch (IllegalArgumentException e) {
            throw IngestDecodeException.failed(key(), "data를 해석하지 못했습니다: " + e.getMessage());
        }
    }

    private static Instant measuredAt(JsonNode root) {
        Instant earliest = null;
        for (JsonNode rx : root.path("rxInfo")) {
            Instant t = instant(text(rx, "nsTime"));
            if (t == null) {
                t = instant(text(rx, "time"));
            }
            if (t != null && (earliest == null || t.isBefore(earliest))) {
                earliest = t;
            }
        }
        return earliest != null ? earliest : instant(text(root, "time"));
    }

    private static CanonicalTelemetry.Link link(JsonNode root) {
        List<CanonicalTelemetry.GatewayReception> gateways = new ArrayList<>();
        for (JsonNode rx : root.path("rxInfo")) {
            String eui = text(rx, "gatewayId");
            if (eui != null) {
                gateways.add(new CanonicalTelemetry.GatewayReception(eui.toLowerCase(Locale.ROOT),
                        rx.hasNonNull("rssi") ? rx.get("rssi").asDouble() : null,
                        rx.hasNonNull("snr") ? rx.get("snr").asDouble() : null));
            }
        }
        Long fCnt = root.hasNonNull("fCnt") ? root.get("fCnt").asLong() : null;
        if (gateways.isEmpty() && fCnt == null) {
            return null;
        }
        CanonicalTelemetry.GatewayReception best = gateways.stream()
                .filter(g -> g.rssi() != null)
                .max(Comparator.comparingDouble(CanonicalTelemetry.GatewayReception::rssi))
                .orElse(null);
        return new CanonicalTelemetry.Link(best == null ? null : best.rssi(), best == null ? null : best.snr(), fCnt,
                gateways.isEmpty() ? null : gateways);
    }

    private static Instant instant(String text) {
        if (text == null) {
            return null;
        }
        try {
            return Instant.parse(text);
        } catch (DateTimeParseException e) {
            try {
                return OffsetDateTime.parse(text).toInstant();
            } catch (DateTimeParseException e2) {
                return null;
            }
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return v == null || v.isNull() || !v.isValueNode() ? null : v.asString();
    }

    static String firstLine(String message) {
        if (message == null) {
            return "";
        }
        int nl = message.indexOf('\n');
        return nl < 0 ? message : message.substring(0, nl);
    }
}
