package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-03.03 테스트 케이스 기대 출력 비교(EXACT·FIELDS·TOLERANCE) */
class TestCaseComparatorTest {

    private static JsonNode json(String s) {
        return MessageCodec.newMapper().readTree(s);
    }

    @Test
    @DisplayName("[SCR-03.03][AT-SCR-04.1] EXACT: 키 순서 무관, 다른 곳을 경로로")
    void exact() {
        JsonNode expected = json("{\"metrics\":[{\"key\":\"t\",\"value\":1}],\"a\":true}");
        assertThat(TestCaseComparator.compare("EXACT", expected, json("{\"a\":true,\"metrics\":[{\"value\":1,\"key\":\"t\"}]}"),
                null, null)).isEmpty();
        assertThat(TestCaseComparator.compare(null, expected, json("{\"metrics\":[{\"key\":\"t\",\"value\":2}],\"b\":1}"), null, null))
                .extracting(TestCaseComparator.Difference::path).containsExactly("$.metrics[0].value", "$.a", "$.b");
        assertThat(TestCaseComparator.compare("EXACT", json("null"), json("null"), null, null)).isEmpty();
    }

    @Test
    @DisplayName("[SCR-03.03] FIELDS: 지정한 경로만(배열은 key 이름으로도), TOLERANCE: 허용 오차 안이면 같음")
    void fieldsAndTolerance() {
        JsonNode expected = json("{\"metrics\":[{\"key\":\"t\",\"value\":20.5},{\"key\":\"h\",\"value\":40}],\"x\":1}");
        JsonNode actual = json("{\"metrics\":[{\"key\":\"h\",\"value\":41},{\"key\":\"t\",\"value\":20.5}],\"x\":2}");
        assertThat(TestCaseComparator.compare("FIELDS", expected, actual, List.of("metrics.t.value"), null)).isEmpty();
        assertThat(TestCaseComparator.compare("FIELDS", expected, actual, List.of("$.metrics.h.value", "x"), null))
                .hasSize(2);
        assertThat(TestCaseComparator.compare("FIELDS", expected, actual, List.of("$.metrics[0].key"), null))
                .singleElement().satisfies(d -> assertThat(d.toMap()).containsKeys("path", "expected", "actual"));
        assertThat(TestCaseComparator.compare("TOLERANCE", json("{\"v\":[1.0, 2.0]}"), json("{\"v\":[1.04, 1.96]}"), null, 0.05))
                .isEmpty();
        assertThat(TestCaseComparator.compare("TOLERANCE", json("{\"v\":1.0}"), json("{\"v\":1.2}"), null, 0.05)).hasSize(1);
        assertThat(TestCaseComparator.compare("EXACT", json("{\"v\":\"a\"}"), json("{\"v\":1}"), null, null)).hasSize(1);
    }
}
