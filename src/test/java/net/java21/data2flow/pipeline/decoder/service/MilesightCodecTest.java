package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ING-02.02 · DEV-03.02: 아카데미 실측 6종 Milesight 채널 형식 기본 디코더(공식 디코더와 같은 값) */
class MilesightCodecTest {

    private static Map<String, Double> decode(String hex, String model) {
        Map<String, Double> values = new LinkedHashMap<>();
        for (DecodedValue v : MilesightCodec.decode(HexFormat.of().parseHex(hex.replace(" ", "")), model)) {
            values.put(v.key(), v.asDouble());
        }
        return values;
    }

    @Test
    @DisplayName("[ING-02.02] EM300-TH·EM320-TH: 배터리·온도(int16/10)·습도(/2), 영하 온도")
    void em300() {
        assertThat(decode("01755C 03673401 046865", "EM300-TH"))
                .containsExactly(Map.entry("battery", 92.0), Map.entry("temperature", 30.8), Map.entry("humidity", 50.5));
        assertThat(decode("0367F6FF", "EM320-TH")).containsEntry("temperature", -1.0);
    }

    @Test
    @DisplayName("[ING-02.02] EM500-CO2: CO2(ch5)·기압(/10)")
    void em500() {
        assertThat(decode("017564 0367F800 04686E 057D3402 06731C27", "EM500-CO2"))
                .containsEntry("co2", 564.0).containsEntry("pressure", 1001.2).containsEntry("temperature", 24.8)
                .containsEntry("humidity", 55.0);
    }

    @Test
    @DisplayName("[ING-02.02] AM103: CO2(ch7)")
    void am103() {
        assertThat(decode("03671001 046871 077D2C03 017564", "AM103"))
                .containsExactly(Map.entry("temperature", 27.2), Map.entry("humidity", 56.5), Map.entry("co2", 812.0),
                        Map.entry("battery", 100.0));
    }

    @Test
    @DisplayName("[ING-02.02] AM107: 활동·조도 3값·CO2·TVOC·기압")
    void am107() {
        assertThat(decode("01755C 03673401 046865 056A4900 06651C0079001400 077DE704 087D0700 09733F27", "AM107"))
                .containsEntry("activity", 73.0).containsEntry("illumination", 28.0)
                .containsEntry("infrared_and_visible", 121.0).containsEntry("infrared", 20.0).containsEntry("co2", 1255.0)
                .containsEntry("tvoc", 7.0).containsEntry("pressure", 1004.7);
    }

    @Test
    @DisplayName("[ING-02.02] WS302: 가중치 + LAI·LAImax·LAeq(공식 디코더 순서, /10 dB), 키 대소문자 유지")
    void ws302() {
        assertThat(decode("017537 055B01A40131 01A401", "WS302"))
                .containsExactly(Map.entry("battery", 55.0), Map.entry("LAI", 42.0), Map.entry("LAImax", 30.5),
                        Map.entry("LAeq", 42.0));
    }

    @Test
    @DisplayName("[ING-02.02] 기기 정보 채널(FF)은 건너뛰고, 모르는 채널·짧은 payload·모델에 없는 채널은 오류")
    void errors() {
        assertThat(decode("FF0101 FF09 0140 FF0A 0114 FF0F00 FF16 6136A04478890000 01755C", null))
                .containsExactly(Map.entry("battery", 92.0));
        assertThatThrownBy(() -> MilesightCodec.decode(HexFormat.of().parseHex("0999"), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MilesightCodec.decode(HexFormat.of().parseHex("0367"), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MilesightCodec.decode(HexFormat.of().parseHex("077D2C03"), "EM300-TH"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(MilesightCodec.model("WS302-915M")).isEqualTo("WS302");
        assertThat(MilesightCodec.model("unknown")).isNull();
        assertThat(MilesightCodec.model(null)).isNull();
        assertThat(MilesightCodec.MODELS.keySet()).containsAll(List.of("EM300-TH", "EM320-TH", "EM500-CO2", "AM103",
                "AM107", "WS302"));
    }
}
