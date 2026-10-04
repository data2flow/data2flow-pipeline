package net.java21.data2flow.pipeline.quality.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** ING-06.01 기기별 일일 품질 점수 */
class DataQualityScoreCalculatorTest {

    @Test
    @DisplayName("[ING-06.01][AT-ING-09.1] TC-ING-069 예상 주기 60초·수신 720 → 완전성 50(근거 \"예상 1440 / 수신 720\"), 총점 가중 평균")
    void completenessAndWeighted() {
        DataQualityScore s = DataQualityScore.calculate(86_400, 60, 720, 72, 1440, 144, 36);

        assertThat(s.completeness()).isEqualTo(50);
        assertThat(s.completenessEvidence()).isEqualTo("예상 1440 / 수신 720");
        assertThat(s.timeliness()).isEqualTo(90);
        assertThat(s.validity()).isEqualTo(90);
        assertThat(s.stability()).isEqualTo(98);
        assertThat(s.score()).isEqualTo((int) Math.round(50 * 0.4 + 90 * 0.2 + 90 * 0.2 + 98 * 0.2));
        assertThat(s.score()).isBetween(0, 100);
    }

    @Test
    @DisplayName("[ING-06.01] TC-ING-069 예상 주기를 모르는 기기는 완전성을 빼고 나머지 세 요소로, 하루가 25시간이면 예상도 늘고, 넘쳐도 100")
    void unknownIntervalAndBounds() {
        DataQualityScore unknown = DataQualityScore.calculate(86_400, null, 100, 0, 100, 0, 0);
        assertThat(unknown.score()).isEqualTo(100);
        assertThat(unknown.expectedCount()).isZero();
        assertThat(DataQualityScore.calculate(90_000, 60, 1500, 0, 1500, 0, 0).expectedCount()).isEqualTo(1500);
        assertThat(DataQualityScore.calculate(86_400, 60, 2000, 0, 2000, 0, 0).completeness()).isEqualTo(100);
    }

    @Test
    @DisplayName("[ING-06.01] 아무것도 받지 못한 날은 모든 요소 0")
    void nothingReceived() {
        DataQualityScore s = DataQualityScore.calculate(86_400, 300, 0, 0, 0, 0, 0);
        assertThat(s.score()).isZero();
        assertThat(s.completeness()).isZero();
        assertThat(s.expectedCount()).isEqualTo(288);
    }
}
