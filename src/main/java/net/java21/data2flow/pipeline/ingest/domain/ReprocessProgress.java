package net.java21.data2flow.pipeline.ingest.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Set;

/**
 * 재처리 작업 상태와 진행률(ING-01.04, ING domain-model "ReprocessJob"). 화면(API-ING-14)은 대기 상태를 QUEUED로 보여 주고 DB에는
 * PENDING으로 둔다.
 *
 * <pre>PENDING → RUNNING → COMPLETED | FAILED,  PENDING/RUNNING → CANCELLED,  RUNNING → RUNNING(다른 인스턴스가 넘겨받음)</pre>
 */
public final class ReprocessProgress {

    /** 작업 상태 */
    public enum Status {
        PENDING, RUNNING, COMPLETED, FAILED, CANCELLED;

        private static final Map<Status, Set<Status>> NEXT = Map.of(
                PENDING, Set.of(RUNNING, CANCELLED),
                RUNNING, Set.of(RUNNING, COMPLETED, FAILED, CANCELLED),
                COMPLETED, Set.of(),
                FAILED, Set.of(),
                CANCELLED, Set.of());

        public boolean canMoveTo(Status next) {
            return NEXT.get(this).contains(next);
        }

        /** 끝난 상태(취소 후에는 더 처리하지 않는다) */
        public boolean finished() {
            return NEXT.get(this).isEmpty();
        }

        /** API 표시 이름(대기는 QUEUED) */
        public String displayName() {
            return this == PENDING ? "QUEUED" : name();
        }
    }

    private ReprocessProgress() {
    }

    /** 처리 건수 ÷ 대상 건수 × 100, 소수 1자리 반올림. 대상이 0이면 100 */
    public static double percent(long done, long total) {
        if (total <= 0) {
            return 100.0;
        }
        return BigDecimal.valueOf(Math.min(done, total) * 100.0 / total).setScale(1, RoundingMode.HALF_UP).doubleValue();
    }
}
