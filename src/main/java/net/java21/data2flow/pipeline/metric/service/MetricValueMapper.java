package net.java21.data2flow.pipeline.metric.service;

import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import net.java21.data2flow.pipeline.metric.domain.MetricDefinition;

import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;

/**
 * 숫자가 아닌 값의 변환(ING-02.06, BR-ING-05): 불린 true/false → 1/0, 글자는 측정 항목 정의의 열거형 매핑(대소문자 무시),
 * 정의에 매핑이 없으면 흔한 상태어(open/close, on/off, true/false)와 숫자 글자를 받아들인다. 바꿀 수 없으면 빈 값
 * (메시지 INVALID, {@code ING_VALUE_NOT_NUMERIC}).
 */
public final class MetricValueMapper {

    private static final Map<String, Double> COMMON = Map.of(
            "open", 1.0, "close", 0.0, "closed", 0.0, "on", 1.0, "off", 0.0, "true", 1.0, "false", 0.0);

    private MetricValueMapper() {
    }

    public static OptionalDouble toNumber(DecodedValue value, MetricDefinition definition) {
        Object v = value.value();
        if (v instanceof Number n) {
            double d = n.doubleValue();
            return Double.isFinite(d) ? OptionalDouble.of(d) : OptionalDouble.empty();
        }
        if (v instanceof Boolean b) {
            return OptionalDouble.of(b ? 1 : 0);
        }
        String text = v.toString().trim().toLowerCase(Locale.ROOT);
        if (definition != null && !definition.enumMap().isEmpty()) {
            Double mapped = definition.enumMap().get(text);
            return mapped == null ? OptionalDouble.empty() : OptionalDouble.of(mapped);
        }
        Double common = COMMON.get(text);
        if (common != null) {
            return OptionalDouble.of(common);
        }
        try {
            double d = Double.parseDouble(text);
            return Double.isFinite(d) ? OptionalDouble.of(d) : OptionalDouble.empty();
        } catch (NumberFormatException e) {
            return OptionalDouble.empty();
        }
    }
}
