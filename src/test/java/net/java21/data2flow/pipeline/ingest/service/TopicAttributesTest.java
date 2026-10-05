package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.message.IngressStatus;
import net.java21.data2flow.contracts.message.decoder.DecodedUplink;
import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** DSC-09.08 AT-DSC-16.4: 디코딩 결과에 토픽 템플릿 값을 적용하는 규칙 */
class TopicAttributesTest {

    static final DecodedUplink ONE = new DecodedUplink("dec-id", null, List.of(new DecodedValue("value", 1.5, "C")), null,
            Map.of("site", "from-decoder"));

    @Test
    @DisplayName("[DSC-09.08][AT-DSC-16.4] externalId는 템플릿 값, 값이 하나면 metric으로 키 이름, 나머지는 태그(디코더 태그 우선)")
    void applies() {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put(IngressStatus.ATTR_EXTERNAL_ID, "em-1");
        attrs.put(IngressStatus.ATTR_METRIC, "temperature");
        attrs.put("site", "a");
        attrs.put(IngressStatus.ATTR_SPACE_HINT, "a/301");
        attrs.put("empty", "");
        DecodedUplink u = IngestProcessor.withTopicAttributes(ONE, attrs);
        assertThat(u.externalId()).isEqualTo("em-1");
        assertThat(u.values()).singleElement().satisfies(v -> {
            assertThat(v.key()).isEqualTo("temperature");
            assertThat(v.unit()).isEqualTo("C");
        });
        assertThat(u.tags()).containsEntry("site", "from-decoder").containsEntry("spaceHint", "a/301")
                .doesNotContainKeys("externalId", "metric", "empty");
    }

    @Test
    @DisplayName("[DSC-09.08] 템플릿 값이 없으면 그대로, 값이 여럿이면 키는 그대로, externalId가 없으면 디코더 값")
    void noOp() {
        assertThat(IngestProcessor.withTopicAttributes(ONE, null)).isSameAs(ONE);
        assertThat(IngestProcessor.withTopicAttributes(ONE, Map.of())).isSameAs(ONE);
        DecodedUplink two = new DecodedUplink("d", null, List.of(DecodedValue.of("a", 1), DecodedValue.of("b", 2)), null, null);
        DecodedUplink u = IngestProcessor.withTopicAttributes(two, Map.of(IngressStatus.ATTR_METRIC, "m", "room", "301"));
        assertThat(u.values()).extracting(DecodedValue::key).containsExactly("a", "b");
        assertThat(u.externalId()).isEqualTo("d");
        assertThat(u.tags()).containsEntry("room", "301");
    }
}
