package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ING-02.03 TC-ING-039·040: generic-json 매핑과 검증 */
class GenericJsonDecoderTest {

    private final GenericJsonDecoder decoder = new GenericJsonDecoder();

    private static JsonNode config(String json) {
        return DecoderTestSupport.MAPPER.readTree(json);
    }

    @Test
    @DisplayName("[ING-02.03][AT-ING-02.3] TC-ING-039 {deviceIdFrom: topic[1], metrics:[$.temp→temperature]}")
    void topicAndPath() throws Exception {
        DecodedUplink uplink = decoder.decode(DecoderTestSupport.raw("devices/esp-01/telemetry", "{\"temp\":22.4}"),
                config("{\"deviceIdFrom\":\"topic[1]\",\"metrics\":[{\"path\":\"$.temp\",\"key\":\"temperature\",\"unit\":\"℃\"}]}"));

        assertThat(uplink.externalId()).isEqualTo("esp-01");
        assertThat(uplink.values()).singleElement().satisfies(v -> {
            assertThat(v.key()).isEqualTo("temperature");
            assertThat(v.asDouble()).isEqualTo(22.4);
            assertThat(v.unit()).isEqualTo("℃");
        });
    }

    @Test
    @DisplayName("[ING-02.03][AT-ING-02.3] TC-ING-039 시각 경로(epoch ms·ISO), 배열 경로 $.sensors[*], 없는 경로는 건너뜀, 맵 형식 매핑")
    void timeArraysAndMissing() throws Exception {
        String payload = """
                {"id":"dev-9","ts":1790994000000,"t":21.5,"sensors":[{"name":"co2","value":800,"unit":"ppm"},{"name":"door","value":"open"}]}
                """;
        DecodedUplink uplink = decoder.decode(DecoderTestSupport.raw("x", payload), config("""
                {"deviceIdFrom":"$.id","timeFrom":"$.ts","metrics":{"$.t":"temperature","$.missing":"humidity"},
                 "items":{"path":"$.sensors[*]","keyFrom":"$.name","valueFrom":"$.value","unitFrom":"$.unit"}}"""));
        DecodedUplink iso = decoder.decode(DecoderTestSupport.raw("x", "{\"id\":\"a\",\"at\":\"2026-10-03T01:00:00Z\",\"v\":[1,2]}"),
                config("{\"deviceIdFrom\":\"$['id']\",\"timeFrom\":\"$.at\",\"metrics\":[{\"path\":\"$.v[1]\",\"key\":\"second\"}]}"));

        assertThat(uplink.externalId()).isEqualTo("dev-9");
        assertThat(uplink.measuredAt()).isEqualTo(Instant.ofEpochMilli(1790994000000L));
        assertThat(uplink.values()).extracting(DecodedValue::key).containsExactly("temperature", "co2", "door");
        assertThat(uplink.values().get(1).unit()).isEqualTo("ppm");
        assertThat(uplink.values().get(2).value()).isEqualTo("open");
        assertThat(iso.measuredAt()).isEqualTo(Instant.parse("2026-10-03T01:00:00Z"));
        assertThat(iso.values().getFirst().asDouble()).isEqualTo(2);
    }

    @Test
    @DisplayName("[ING-02.03][AT-ING-02.3] TC-ING-040 잘못된 JSONPath·토픽 인덱스 범위 밖·키 중복 매핑은 거부")
    void invalidMappings() {
        assertThat(GenericJsonMappingValidator.validate(config("{\"deviceIdFrom\":\"$..bad\",\"metrics\":[{\"path\":\"$.a\",\"key\":\"a\"}]}")))
                .anyMatch(e -> e.contains("deviceIdFrom"));
        assertThat(GenericJsonMappingValidator.validate(config("{\"deviceIdFrom\":\"topic[40]\",\"metrics\":[{\"path\":\"$.a\",\"key\":\"a\"}]}")))
                .anyMatch(e -> e.contains("토픽 인덱스"));
        assertThat(GenericJsonMappingValidator.validate(config("{\"deviceIdFrom\":\"topic[1]\",\"metrics\":[{\"path\":\"$.a\",\"key\":\"t\"},{\"path\":\"$.b\",\"key\":\"t\"}]}")))
                .anyMatch(e -> e.contains("중복"));
        assertThat(GenericJsonMappingValidator.validate(config("{\"deviceIdFrom\":\"topic[1]\"}")))
                .anyMatch(e -> e.contains("metrics"));
        assertThat(GenericJsonMappingValidator.validate(config("{\"deviceIdFrom\":\"topic[1]\",\"timeFrom\":\"x\",\"items\":{\"keyFrom\":\"y\"}}")))
                .hasSizeGreaterThanOrEqualTo(3);
        assertThat(GenericJsonMappingValidator.validate(null)).isNotEmpty();
        assertThatThrownBy(() -> decoder.decode(DecoderTestSupport.raw("x", "{}"), config("{}")))
                .isInstanceOf(IngestDecodeException.class);
    }

    @Test
    @DisplayName("[ING-02.03] 기기 ID를 찾지 못하면 ING_EXTERNAL_ID_MISSING, 깨진 JSON은 ING_DECODE_FAILED")
    void failures() {
        JsonNode cfg = config("{\"deviceIdFrom\":\"$.id\",\"metrics\":[{\"path\":\"$.t\",\"key\":\"t\"}]}");
        assertThatThrownBy(() -> decoder.decode(DecoderTestSupport.raw("x", "{\"t\":1}"), cfg))
                .isInstanceOfSatisfying(IngestDecodeException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("ING_EXTERNAL_ID_MISSING"));
        assertThatThrownBy(() -> decoder.decode(DecoderTestSupport.raw("x", "{oops"), cfg))
                .isInstanceOfSatisfying(IngestDecodeException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("ING_DECODE_FAILED"));
        assertThat(JsonPaths.isValid("$.a[0]['b c'][*]")).isTrue();
        assertThatThrownBy(() -> JsonPaths.select(DecoderTestSupport.MAPPER.createObjectNode(), "nope"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
