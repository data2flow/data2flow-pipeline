package net.java21.data2flow.pipeline.script.service;

import org.graalvm.polyglot.Value;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.time.ZoneOffset;

/**
 * 스크립트 반환값을 호스트 JSON(Jackson)으로 옮긴다(SCR-02.02 TC-SCR-029, TC-SCR-024).
 *
 * <p>직렬화를 스크립트 안의 {@code JSON.stringify}에 맡기지 않으므로 {@code toJSON}·{@code JSON} 덮어쓰기가 결과에 영향을 주지 않는다.
 * 깊이와 대략적인 크기를 세면서 옮기므로 순환 참조·깊은 중첩·거대한 배열에서도 호스트 스택이나 힙이 넘치지 않는다.
 */
final class GuestValueConverter {

    private final int maxDepth;
    private final int maxBytes;
    private long budget;

    GuestValueConverter(int maxDepth, int maxBytes) {
        this.maxDepth = maxDepth;
        this.maxBytes = maxBytes;
        this.budget = maxBytes + 1024L;
    }

    JsonNode convert(Value value) {
        return convert(value, 0);
    }

    private JsonNode convert(Value value, int depth) {
        if (depth > maxDepth) {
            throw new OutputInvalidException("반환값 중첩이 너무 깊습니다(최대 " + maxDepth + "단계, 순환 참조일 수 있습니다)");
        }
        spend(2);
        if (value == null || value.isNull()) {
            return JsonNodeFactory.instance.nullNode();
        }
        if (value.isBoolean()) {
            return JsonNodeFactory.instance.booleanNode(value.asBoolean());
        }
        if (value.isNumber()) {
            spend(8);
            if (value.fitsInLong()) {
                return JsonNodeFactory.instance.numberNode(value.asLong());
            }
            double d = value.asDouble();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw new OutputInvalidException("숫자가 유한하지 않습니다: " + d);
            }
            return JsonNodeFactory.instance.numberNode(d);
        }
        if (value.isString()) {
            String s = value.asString();
            spend(s.length() + 2L);
            return JsonNodeFactory.instance.stringNode(s);
        }
        if (value.isInstant()) {
            return JsonNodeFactory.instance.stringNode(value.asInstant().toString());
        }
        if (value.isDate() && value.isTime() && value.isTimeZone()) {
            return JsonNodeFactory.instance.stringNode(
                    value.asDate().atTime(value.asTime()).atZone(value.asTimeZone()).withZoneSameInstant(ZoneOffset.UTC)
                            .toInstant().toString());
        }
        if (value.canExecute()) {
            throw new OutputInvalidException("함수는 반환할 수 없습니다");
        }
        if (isPromise(value)) {
            throw new PromiseOutputException();
        }
        if (value.hasArrayElements()) {
            long size = value.getArraySize();
            if (size * 2 > budget) {
                throw new OutputInvalidException("반환값이 " + maxBytes + "바이트를 넘습니다");
            }
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            for (long i = 0; i < size; i++) {
                array.add(convert(value.getArrayElement(i), depth + 1));
            }
            return array;
        }
        if (value.hasMembers()) {
            ObjectNode object = JsonNodeFactory.instance.objectNode();
            for (String key : value.getMemberKeys()) {
                Value member = value.getMember(key);
                if (member == null || member.canExecute() || isUndefined(member)) {
                    continue; // 함수·undefined 멤버는 JSON.stringify처럼 건너뛴다
                }
                spend(key.length() + 3L);
                object.set(key, convert(member, depth + 1));
            }
            return object;
        }
        throw new OutputInvalidException("JSON으로 바꿀 수 없는 값입니다");
    }

    private static boolean isUndefined(Value value) {
        return value.isNull() && "undefined".equals(value.toString());
    }

    private static boolean isPromise(Value value) {
        if (!value.hasMembers()) {
            return false;
        }
        Value meta = value.getMetaObject();
        return meta != null && "Promise".equals(meta.getMetaSimpleName());
    }

    private void spend(long bytes) {
        budget -= bytes;
        if (budget < 0) {
            throw new OutputInvalidException("반환값이 " + maxBytes + "바이트를 넘습니다");
        }
    }

    /** 반환값 계약 위반 */
    static class OutputInvalidException extends RuntimeException {
        OutputInvalidException(String message) {
            super(message, null, false, false);
        }
    }

    /** Promise 반환(비동기·동적 import는 쓸 수 없음) */
    static final class PromiseOutputException extends OutputInvalidException {
        PromiseOutputException() {
            super("Promise(비동기 함수·동적 import)는 쓸 수 없습니다");
        }
    }
}
