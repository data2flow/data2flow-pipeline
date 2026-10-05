package net.java21.data2flow.pipeline.decoder.service;

import net.java21.data2flow.contracts.message.IngressStatus;
import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DSC-09.08 AT-DSC-16.4: 기본 디코더가 토픽 템플릿 값(RawEnvelope.topicAttributes)을 쓴다 */
class TopicAttributesDecoderTest {

    @Test
    @DisplayName("[DSC-09.08][AT-DSC-16.4] single-value: 템플릿의 externalId·metric이 토픽 칸보다 먼저, 칸이 모자라도 된다")
    void singleValue() throws Exception {
        SingleValueDecoder d = new SingleValueDecoder();
        DecodedUplink u = d.decode(DecoderTestSupport.raw("x", "3.5").withTopicAttributes(
                Map.of(IngressStatus.ATTR_EXTERNAL_ID, "em-1", IngressStatus.ATTR_METRIC, "co2")), null);
        assertThat(u.externalId()).isEqualTo("em-1");
        assertThat(u.values().getFirst().key()).isEqualTo("co2");
        DecodedUplink partial = d.decode(DecoderTestSupport.raw("a/b/c", "1").withTopicAttributes(
                Map.of(IngressStatus.ATTR_METRIC, "co2")), null);
        assertThat(partial.externalId()).isEqualTo("b");
        assertThat(partial.values().getFirst().key()).isEqualTo("co2");
        assertThatThrownBy(() -> d.decode(DecoderTestSupport.raw("x", "1").withTopicAttributes(
                Map.of(IngressStatus.ATTR_METRIC, "co2")), null)).isInstanceOf(IngestDecodeException.class);
    }

    @Test
    @DisplayName("[DSC-09.08] generic-json: deviceIdFrom 경로에 값이 없으면 템플릿 externalId, 둘 다 없으면 EXTERNAL_ID_MISSING")
    void genericJson() throws Exception {
        GenericJsonDecoder d = new GenericJsonDecoder();
        var config = DecoderTestSupport.MAPPER.readTree("{\"deviceIdFrom\":\"$.id\",\"metrics\":{\"$.t\":\"temperature\"}}");
        DecodedUplink u = d.decode(DecoderTestSupport.raw("x", "{\"t\":1}").withTopicAttributes(
                Map.of(IngressStatus.ATTR_EXTERNAL_ID, "em-2")), config);
        assertThat(u.externalId()).isEqualTo("em-2");
        assertThat(d.decode(DecoderTestSupport.raw("x", "{\"id\":\"p\",\"t\":1}").withTopicAttributes(
                Map.of(IngressStatus.ATTR_EXTERNAL_ID, "em-2")), config).externalId()).as("payload 값이 먼저").isEqualTo("p");
        assertThatThrownBy(() -> d.decode(DecoderTestSupport.raw("x", "{\"t\":1}").withTopicAttributes(Map.of("site", "a")),
                config)).isInstanceOf(IngestDecodeException.class);
    }
}
