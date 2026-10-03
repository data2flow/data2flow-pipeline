package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ING-02.02 TC-ING-035 · NFR-06.02 TC-NFR-050: 골든 업링크 10종(EM300-TH 정상·저배터리, EM320-TH, EM500-CO2, AM103, AM107,
 * WS302, WS301 문, EM300-MCS, GS101)의 디코딩 결과가 {@code *.expected.json}과 STRICT로 같다(키 순서 무관, 값·단위 일치).
 * 픽스처는 src/test/resources/golden/chirpstack(실측 아카데미 업링크 형식, contracts에 옮기는 일은 문서 참고).
 */
class ChirpStackDecoderGoldenTest {

    private final ChirpStackV4Decoder decoder = new ChirpStackV4Decoder();

    static Stream<String> fixtures() throws IOException, URISyntaxException {
        Path dir = Path.of(ChirpStackDecoderGoldenTest.class.getResource("/golden/chirpstack").toURI());
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".json") && !n.endsWith(".expected.json"))
                    .map(n -> n.substring(0, n.length() - ".json".length()))
                    .sorted().toList().stream();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixtures")
    @DisplayName("[ING-02.02][AT-ING-02.1] TC-ING-035 골든 업링크 → 기대 결과와 STRICT 일치")
    void golden(String name) throws Exception {
        byte[] payload = read(name + ".json");
        String expected = new String(read(name + ".expected.json"));

        DecodedUplink uplink = decoder.decode(DecoderTestSupport.raw("application/a/device/x/event/up", payload),
                DecoderTestSupport.MAPPER.createObjectNode());

        JSONAssert.assertEquals(expected, DecoderTestSupport.toJson(uplink).toString(), JSONCompareMode.STRICT);
    }

    @Test
    @DisplayName("[ING-02.02][AT-ING-02.1] TC-ING-035 골든 픽스처는 10쌍이고 기대 결과가 빠진 것이 없다")
    void fixtureCount() throws Exception {
        List<String> names = fixtures().toList();
        assertThat(names).hasSize(10);
        for (String n : names) {
            assertThat(read(n + ".expected.json")).as(n).isNotEmpty();
        }
    }

    private static byte[] read(String file) throws IOException {
        try (var in = ChirpStackDecoderGoldenTest.class.getResourceAsStream("/golden/chirpstack/" + file)) {
            return in.readAllBytes();
        }
    }
}
