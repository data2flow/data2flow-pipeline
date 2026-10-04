package net.java21.data2flow.pipeline.ingest.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static net.java21.data2flow.pipeline.ingest.domain.ReprocessProgress.Status.CANCELLED;
import static net.java21.data2flow.pipeline.ingest.domain.ReprocessProgress.Status.COMPLETED;
import static net.java21.data2flow.pipeline.ingest.domain.ReprocessProgress.Status.FAILED;
import static net.java21.data2flow.pipeline.ingest.domain.ReprocessProgress.Status.PENDING;
import static net.java21.data2flow.pipeline.ingest.domain.ReprocessProgress.Status.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

/** ING-01.04 재처리 작업 상태 */
class ReprocessJobStateTest {

    @Test
    @DisplayName("[ING-01.04][AT-ING-08.2] TC-ING-025 QUEUED→RUNNING→COMPLETED/FAILED/CANCELLED 전이, 끝난 뒤에는 움직이지 않음")
    void transitions() {
        assertThat(PENDING.displayName()).isEqualTo("QUEUED");
        assertThat(PENDING.canMoveTo(RUNNING)).isTrue();
        assertThat(PENDING.canMoveTo(CANCELLED)).isTrue();
        assertThat(PENDING.canMoveTo(COMPLETED)).isFalse();
        assertThat(RUNNING.canMoveTo(COMPLETED)).isTrue();
        assertThat(RUNNING.canMoveTo(FAILED)).isTrue();
        assertThat(RUNNING.canMoveTo(CANCELLED)).isTrue();
        assertThat(RUNNING.canMoveTo(RUNNING)).as("다른 인스턴스가 넘겨받음").isTrue();
        for (ReprocessProgress.Status done : new ReprocessProgress.Status[]{COMPLETED, FAILED, CANCELLED}) {
            assertThat(done.finished()).isTrue();
            assertThat(done.canMoveTo(RUNNING)).as("%s 뒤 추가 처리 없음", done).isFalse();
            assertThat(done.displayName()).isEqualTo(done.name());
        }
    }

    @Test
    @DisplayName("[ING-01.04] TC-ING-025 진행률 = 처리 건수 ÷ 대상 건수(소수 1자리), 대상 0이면 100")
    void percent() {
        assertThat(ReprocessProgress.percent(1, 3)).isEqualTo(33.3);
        assertThat(ReprocessProgress.percent(2, 3)).isEqualTo(66.7);
        assertThat(ReprocessProgress.percent(4000, 10000)).isEqualTo(40.0);
        assertThat(ReprocessProgress.percent(5, 0)).isEqualTo(100.0);
        assertThat(ReprocessProgress.percent(12, 10)).isEqualTo(100.0);
        assertThat(net.java21.data2flow.pipeline.ingest.service.ReprocessJobService.percent(1, 8)).isEqualTo(12.5);
    }
}
