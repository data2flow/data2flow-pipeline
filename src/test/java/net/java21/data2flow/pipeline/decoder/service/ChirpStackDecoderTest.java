package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import net.java21.data2flow.contracts.test.message.MessageFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ING-02.02 TC-ING-036·037: nsTime 우선, 게이트웨이·fCnt, devEui 소문자, 경계(rxInfo 없음·object 없음·devEui 없음) */
class ChirpStackDecoderTest {

    private final ChirpStackV4Decoder decoder = new ChirpStackV4Decoder();

    @Test
    @DisplayName("[ING-02.02][AT-ING-02.2] TC-ING-036 rxInfo 2개 → measuredAt은 더 이른 nsTime, 게이트웨이 2건, fCnt, devEui 소문자")
    void earliestNsTime() throws Exception {
        String payload = """
                {"time":"2026-10-03T01:02:04Z","deviceInfo":{"devEui":"24E124743D012436","tags":{"location":"실습실"}},
                 "fCnt":42,"object":{"temperature":22.5},
                 "rxInfo":[{"gatewayId":"24E124FFFEF79304","rssi":-97,"snr":3.5,"nsTime":"2026-10-03T01:02:03.120Z"},
                           {"gatewayId":"24e124fffef5dccc","rssi":-110,"snr":-4.0,"nsTime":"2026-10-03T01:02:03.080Z"}]}
                """;

        DecodedUplink uplink = decoder.decode(DecoderTestSupport.raw("application/a/device/24e124743d012436/event/up", payload),
                DecoderTestSupport.MAPPER.createObjectNode());

        assertThat(uplink.externalId()).isEqualTo("24e124743d012436");
        assertThat(uplink.measuredAt()).isEqualTo(Instant.parse("2026-10-03T01:02:03.080Z"));
        assertThat(uplink.link().gateways()).extracting(g -> g.eui()).containsExactly("24e124fffef79304", "24e124fffef5dccc");
        assertThat(uplink.link().rssi()).isEqualTo(-97);
        assertThat(uplink.link().frameCounter()).isEqualTo(42);
        assertThat(uplink.tags()).containsEntry("location", "실습실");
    }

    @Test
    @DisplayName("[ING-02.02][AT-ING-02.2] TC-ING-037 rxInfo 없으면 time, 둘 다 없으면 null(수신 시각 + 품질 4는 다음 단계)")
    void timeFallbacks() throws Exception {
        DecodedUplink withTime = decoder.decode(DecoderTestSupport.raw("t",
                "{\"time\":\"2026-10-03T01:00:00+09:00\",\"deviceInfo\":{\"devEui\":\"aa\"},\"object\":{\"t\":1}}"), null);
        DecodedUplink none = decoder.decode(DecoderTestSupport.raw("t",
                "{\"deviceInfo\":{\"devEui\":\"aa\"},\"object\":{\"t\":1}}"), null);

        assertThat(withTime.measuredAt()).isEqualTo(Instant.parse("2026-10-02T16:00:00Z"));
        assertThat(none.measuredAt()).isNull();
    }

    @Test
    @DisplayName("[ING-02.02][AT-ING-02.2] TC-ING-037 object 없는 join·status 이벤트는 측정값 0개, 링크만")
    void noObject() throws Exception {
        DecodedUplink uplink = decoder.decode(DecoderTestSupport.raw("application/a/device/aabbccddeeff0011/event/status",
                "{\"deviceInfo\":{\"devEui\":\"AABBCCDDEEFF0011\"},\"rxInfo\":[{\"gatewayId\":\"g1\",\"rssi\":-50,\"snr\":9}]}"), null);

        assertThat(uplink.values()).isEmpty();
        assertThat(uplink.link().rssi()).isEqualTo(-50);
    }

    @Test
    @DisplayName("[ING-02.02][AT-ING-02.2] TC-ING-037 devEui 없음 → ING_EXTERNAL_ID_MISSING, 토픽에 있으면 토픽에서")
    void devEuiMissing() throws Exception {
        assertThatThrownBy(() -> decoder.decode(DecoderTestSupport.raw("t", "{\"object\":{}}"), null))
                .isInstanceOfSatisfying(IngestDecodeException.class,
                        e -> assertThat(e.errorCode()).isEqualTo(IngestDecodeException.EXTERNAL_ID_MISSING));
        DecodedUplink fromTopic = decoder.decode(
                DecoderTestSupport.raw("application/a/device/AABBCCDDEEFF0011/event/up", "{\"object\":{\"x\":1}}"), null);
        assertThat(fromTopic.externalId()).isEqualTo("aabbccddeeff0011");
    }

    @Test
    @DisplayName("[ING-01.02][AT-ING-01.4] TC-ING-012 깨진 JSON → ING_DECODE_FAILED")
    void brokenJson() {
        assertThatThrownBy(() -> decoder.decode(DecoderTestSupport.raw("t", "{\"broken"), null))
                .isInstanceOfSatisfying(IngestDecodeException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("ING_DECODE_FAILED"));
        assertThatThrownBy(() -> decoder.decode(DecoderTestSupport.raw("t", "[1,2]"), null))
                .isInstanceOf(IngestDecodeException.class);
    }

    @Test
    @DisplayName("[ING-02.02][DEV-03.02] object가 없고 data만 있으면 기기 프로필 이름으로 Milesight 기본 디코더를 쓴다")
    void milesightFallback() throws Exception {
        String data = Base64.getEncoder().encodeToString(new byte[]{0x01, 0x75, 0x5C, 0x03, 0x67, 0x34, 0x01, 0x04, 0x68, 0x65});
        DecodedUplink uplink = decoder.decode(DecoderTestSupport.raw("t",
                "{\"deviceInfo\":{\"devEui\":\"aa\",\"deviceProfileName\":\"Milesight EM300-TH\"},\"data\":\"" + data + "\"}"),
                null);

        assertThat(uplink.values()).extracting(DecodedValue::key).containsExactly("battery", "temperature", "humidity");
        assertThat(uplink.values().get(1).asDouble()).isEqualTo(30.8);
        assertThatThrownBy(() -> decoder.decode(DecoderTestSupport.raw("t",
                "{\"deviceInfo\":{\"devEui\":\"aa\"},\"data\":\"" + Base64.getEncoder().encodeToString(new byte[]{0x77, 0x77, 1})
                        + "\"}"), null)).isInstanceOf(IngestDecodeException.class);
    }

    @Test
    @DisplayName("[ING-02.01][AT-ING-02.1] 공유 픽스처(contracts chirpstack-ws302-uplink)를 디코딩하면 WS302 측정값 4개(LAeq·LAI·LAImax 대소문자 유지)")
    void contractsFixture() throws Exception {
        DecodedUplink uplink = decoder.decode(MessageFixtures.rawEnvelope("chirpstack-ws302-uplink"), null);

        assertThat(uplink.values()).extracting(DecodedValue::key).containsExactlyInAnyOrder("battery", "LAeq", "LAI", "LAImax");
        assertThat(decoder.key()).isEqualTo("chirpstack-v4");
        assertThat(decoder.version()).isEqualTo("1");
        assertThat(ChirpStackV4Decoder.deviceMeta(DecoderTestSupport.MAPPER,
                MessageFixtures.chirpStackUplinkPayload())).containsEntry("deviceName", "WS302-012436");
    }
}
