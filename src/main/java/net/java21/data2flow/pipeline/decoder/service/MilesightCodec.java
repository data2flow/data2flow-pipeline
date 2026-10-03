package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.decoder.DecodedValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Milesight 채널 형식(채널 ID + 채널 유형 + 값) 기본 디코더(ING-02.02, DEV-03.02 아카데미 실측 6종: EM300-TH, EM320-TH, EM500-CO2,
 * AM103, AM107, WS302). ChirpStack 기기 프로필에 코덱이 없어 업링크에 {@code object}가 없을 때 {@code data}(base64)를 직접 푼다.
 * 값 형식은 Milesight 공식 디코더(github.com/Milesight-IoT/SensorDecoders)와 같다.
 */
public final class MilesightCodec {

    /** 모델 코드(대문자) → 그 모델이 보내는 채널(디코더 선택·검증용) */
    public static final Map<String, Set<String>> MODELS = Map.of(
            "EM300-TH", Set.of("battery", "temperature", "humidity"),
            "EM320-TH", Set.of("battery", "temperature", "humidity"),
            "EM500-CO2", Set.of("battery", "temperature", "humidity", "co2", "pressure"),
            "AM103", Set.of("battery", "temperature", "humidity", "co2"),
            "AM107", Set.of("battery", "temperature", "humidity", "activity", "illumination", "infrared_and_visible",
                    "infrared", "co2", "tvoc", "pressure"),
            "WS302", Set.of("battery", "LAI", "LAImax", "LAeq"));

    private MilesightCodec() {
    }

    /** 모델 이름(예: {@code WS302-915M}, {@code Milesight EM300-TH})에서 아는 모델 코드를 찾는다. 없으면 null */
    public static String model(String name) {
        if (name == null) {
            return null;
        }
        String upper = name.toUpperCase(Locale.ROOT);
        return MODELS.keySet().stream().filter(upper::contains).findFirst().orElse(null);
    }

    /**
     * @param bytes 업링크 바이트
     * @param model 모델 코드(모르면 null: 6종 채널을 모두 받아들임)
     * @throws IllegalArgumentException 형식 오류·모르는 채널
     */
    public static List<DecodedValue> decode(byte[] bytes, String model) {
        List<DecodedValue> values = new ArrayList<>();
        int i = 0;
        while (i < bytes.length) {
            need(bytes, i, 2);
            int channel = bytes[i] & 0xff;
            int type = bytes[i + 1] & 0xff;
            i += 2;
            switch (channel << 8 | type) {
                case 0x0175 -> { // 배터리 %
                    need(bytes, i, 1);
                    values.add(new DecodedValue("battery", u8(bytes, i), "%"));
                    i += 1;
                }
                case 0x0367 -> { // 온도 int16 LE / 10 ℃
                    need(bytes, i, 2);
                    values.add(new DecodedValue("temperature", i16(bytes, i) / 10.0, "℃"));
                    i += 2;
                }
                case 0x0468 -> { // 습도 uint8 / 2 %
                    need(bytes, i, 1);
                    values.add(new DecodedValue("humidity", u8(bytes, i) / 2.0, "%"));
                    i += 1;
                }
                case 0x057D, 0x077D -> { // CO2 ppm (EM500-CO2 채널 5, AM103·AM107 채널 7)
                    need(bytes, i, 2);
                    values.add(new DecodedValue("co2", u16(bytes, i), "ppm"));
                    i += 2;
                }
                case 0x087D -> { // TVOC (AM107)
                    need(bytes, i, 2);
                    values.add(new DecodedValue("tvoc", u16(bytes, i), null));
                    i += 2;
                }
                case 0x0673, 0x0973 -> { // 기압 uint16 / 10 hPa
                    need(bytes, i, 2);
                    values.add(new DecodedValue("pressure", u16(bytes, i) / 10.0, "hPa"));
                    i += 2;
                }
                case 0x056A -> { // 재실 활동(PIR) (AM107)
                    need(bytes, i, 2);
                    values.add(new DecodedValue("activity", u16(bytes, i), null));
                    i += 2;
                }
                case 0x0665 -> { // 조도·적외선+가시광·적외선 (AM107)
                    need(bytes, i, 6);
                    values.add(new DecodedValue("illumination", u16(bytes, i), "lux"));
                    values.add(new DecodedValue("infrared_and_visible", u16(bytes, i + 2), null));
                    values.add(new DecodedValue("infrared", u16(bytes, i + 4), null));
                    i += 6;
                }
                case 0x055B -> { // 소음: 가중치 1바이트 + LAI + LAImax + LAeq (uint16 / 10 dB, WS302)
                    need(bytes, i, 7);
                    values.add(new DecodedValue("LAI", u16(bytes, i + 1) / 10.0, "dB"));
                    values.add(new DecodedValue("LAImax", u16(bytes, i + 3) / 10.0, "dB"));
                    values.add(new DecodedValue("LAeq", u16(bytes, i + 5) / 10.0, "dB"));
                    i += 7;
                }
                case 0xFF01, 0xFF0B, 0xFF0F -> i += 1;   // 프로토콜 버전·전원 켬·기기 등급
                case 0xFF09, 0xFF0A, 0xFFFF -> i += 2;   // 하드웨어·펌웨어·TSL 버전
                case 0xFF16 -> i += 8;                   // 일련번호
                case 0x20CE -> i += model != null && model.startsWith("EM500") ? 14 : 7; // 저장 기록(건너뜀)
                default -> throw new IllegalArgumentException(
                        String.format("알 수 없는 채널 0x%02X 0x%02X (위치 %d)", channel, type, i - 2));
            }
        }
        if (model != null && MODELS.containsKey(model)) {
            Set<String> allowed = MODELS.get(model);
            for (DecodedValue v : values) {
                if (!allowed.contains(v.key())) {
                    throw new IllegalArgumentException(model + " 모델에 없는 채널입니다: " + v.key());
                }
            }
        }
        return values;
    }

    private static void need(byte[] b, int offset, int size) {
        if (offset + size > b.length) {
            throw new IllegalArgumentException("payload가 짧습니다(위치 " + offset + ", " + size + "바이트 필요)");
        }
    }

    private static int u8(byte[] b, int i) {
        return b[i] & 0xff;
    }

    private static int u16(byte[] b, int i) {
        return (b[i] & 0xff) | (b[i + 1] & 0xff) << 8;
    }

    private static int i16(byte[] b, int i) {
        return (short) u16(b, i);
    }
}
