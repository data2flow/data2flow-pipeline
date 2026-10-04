package net.java21.data2flow.pipeline.quality.domain;

/**
 * 기기별 일일 데이터 품질 점수(ING-06.01, ING domain-model "DataQualityDaily"). 네 요소는 각 0~100이고 총점은
 * 완전성×0.4 + 적시성×0.2 + 유효성×0.2 + 안정성×0.2(반올림)이다.
 *
 * <ul>
 *   <li>완전성: 수신 ÷ 예상 × 100(100 상한). 예상 = 그 날의 길이 ÷ 예상 주기. 예상 주기를 모르는 기기는 완전성을 빼고 나머지 세 요소의
 *       가중치를 다시 나눈다(TC-ING-069)</li>
 *   <li>적시성: 늦게 도착(ING-06.03 late)하지 않은 수신 비율</li>
 *   <li>유효성: 범위 안(quality≠1) 값 비율</li>
 *   <li>안정성: 의심(quality≠3, 값 멈춤·급변) 아닌 값 비율</li>
 * </ul>
 * 아무것도 받지 못한 날은 모든 요소가 0이다.
 *
 * @param score            총점
 * @param completeness     완전성(예상 주기 미정이면 100이고 총점에 들어가지 않음)
 * @param expectedCount    예상 수신 수(예상 주기 미정이면 0)
 * @param receivedCount    수신 수(측정 시각 수)
 * @param lateCount        늦은 도착 수
 * @param outOfRangeCount  범위 초과 값 수
 * @param suspectCount     의심 값 수
 */
public record DataQualityScore(int score, int completeness, int timeliness, int validity, int stability, int expectedCount,
                               int receivedCount, int lateCount, int outOfRangeCount, int suspectCount) {

    /**
     * @param daySeconds   그 날의 길이(초, 사이트 시간대 — 일광 절약 시간이면 23·25시간)
     * @param intervalSec  예상 보고 주기(초). 모르면 null
     * @param received     수신 수(측정 시각 수)
     * @param late         그중 늦게 도착한 수
     * @param values       값 수(측정 항목 행)
     * @param outOfRange   그중 범위 초과(quality 1)
     * @param suspect      그중 의심(quality 3)
     */
    public static DataQualityScore calculate(long daySeconds, Integer intervalSec, int received, int late, int values,
                                             int outOfRange, int suspect) {
        boolean knownInterval = intervalSec != null && intervalSec > 0;
        int expected = knownInterval ? (int) (daySeconds / intervalSec) : 0;
        if (received == 0) {
            return new DataQualityScore(0, knownInterval ? 0 : 100, 0, 0, 0, expected, 0, 0, 0, 0);
        }
        int completeness = knownInterval ? (expected == 0 ? 100 : pct(Math.min(received, expected), expected)) : 100;
        int timeliness = pct(received - late, received);
        int validity = values == 0 ? 100 : pct(values - outOfRange, values);
        int stability = values == 0 ? 100 : pct(values - suspect, values);
        double total = knownInterval
                ? completeness * 0.4 + timeliness * 0.2 + validity * 0.2 + stability * 0.2
                : (timeliness * 0.2 + validity * 0.2 + stability * 0.2) / 0.6;
        return new DataQualityScore((int) Math.round(total), completeness, timeliness, validity, stability, expected, received,
                late, outOfRange, suspect);
    }

    /** 근거 문구(예: "예상 1440 / 수신 720") */
    public String completenessEvidence() {
        return "예상 " + expectedCount + " / 수신 " + receivedCount;
    }

    private static int pct(long part, long whole) {
        return (int) Math.round(part * 100.0 / whole);
    }
}
