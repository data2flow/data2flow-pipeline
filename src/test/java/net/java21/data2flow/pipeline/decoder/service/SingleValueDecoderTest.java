package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ING-02.04 TC-ING-042 · AT-ING-02.7·02.8: single-value 디코더 */
class SingleValueDecoderTest {

    private final SingleValueDecoder decoder = new SingleValueDecoder();

    @ParameterizedTest(name = "{0} → {1}")
    @CsvSource(delimiter = '|', value = {
            "22.8|22.8", "-3|-3", "1e2|100", "' 22.8\n'|22.8", "+4.5|4.5", ".5|0.5"})
    @DisplayName("[ING-02.04][AT-ING-02.7] TC-ING-042 토픽 sensors/esp-02/temperature + 숫자 → esp-02, temperature")
    void numeric(String payload, double expected) throws Exception {
        DecodedUplink uplink = decoder.decode(DecoderTestSupport.raw("sensors/esp-02/temperature", payload), null);

        assertThat(uplink.externalId()).isEqualTo("esp-02");
        assertThat(uplink.values()).singleElement().satisfies(v -> {
            assertThat(v.key()).isEqualTo("temperature");
            assertThat(v.asDouble()).isEqualTo(expected);
        });
        assertThat(uplink.measuredAt()).isNull();
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {"abc", "''", "NaN", "Infinity", "1,5"})
    @DisplayName("[ING-02.04][AT-ING-02.8] TC-ING-042 숫자가 아니면 DECODE_ERROR + ING_VALUE_NOT_NUMERIC")
    void notNumeric(String payload) {
        assertThatThrownBy(() -> decoder.decode(DecoderTestSupport.raw("sensors/esp-02/temperature",
                payload == null ? "" : payload), null))
                .isInstanceOfSatisfying(IngestDecodeException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("ING_VALUE_NOT_NUMERIC"));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({"temperature", "''"})
    @DisplayName("[ING-02.04] 토픽 칸이 2개 미만이면 ING_EXTERNAL_ID_MISSING")
    void shortTopic(String topic) {
        assertThatThrownBy(() -> decoder.decode(DecoderTestSupport.raw(topic, "1"), null))
                .isInstanceOfSatisfying(IngestDecodeException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("ING_EXTERNAL_ID_MISSING"));
    }
}
