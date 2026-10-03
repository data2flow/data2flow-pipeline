package net.java21.data2flow.pipeline.script.domain;

import java.time.Duration;

/**
 * 실행 제한(SCR-02.02, BR-SCR-02, ADR-008). GraalJS 커뮤니티판에는 메모리 상한이 없으므로 문장 수·출력 크기·문자열 길이·
 * 타입 배열 길이·배열 생성 길이로 간접 제한한다.
 *
 * @param cpuTime          1회 실행 CPU 시간 한도(워치독, 기본 50ms)
 * @param wallTime         벽시계 시간 상한(CPU 시간을 잴 수 없는 스레드이거나 대기 중일 때, 기본 200ms)
 * @param statementLimit   실행 문장 수 한도
 * @param maxOutputBytes   반환값 JSON 크기 한도(64KB)
 * @param maxLogBytes      console.log 한 번의 크기 한도(1KB)
 * @param maxLogEntries    한 번 실행에서 모으는 로그 수 한도
 * @param maxStringLength  문자열 길이 한도(문자 수)
 * @param maxArrayLength   Array.from 등 배열 생성 길이·타입 배열 길이 한도
 * @param maxCodeBytes     코드 크기 한도(64KB, BR-SCR-14)
 * @param maxOutputDepth   반환값 중첩 깊이 한도(순환 참조 차단)
 */
public record ScriptLimits(Duration cpuTime, Duration wallTime, long statementLimit, int maxOutputBytes, int maxLogBytes,
                           int maxLogEntries, int maxStringLength, int maxArrayLength, int maxCodeBytes,
                           int maxOutputDepth) {

    public static ScriptLimits defaults() {
        return new ScriptLimits(Duration.ofMillis(50), Duration.ofSeconds(1), 1_000_000, 65_536, 1_024, 100,
                1 << 20, 1 << 20, 65_536, 32);
    }
}
