package net.java21.data2flow.pipeline.ingest.domain;

import net.java21.data2flow.contracts.message.Quality;

/**
 * 품질 코드(ING-04.01, BR-ING-03·04·06, TC-ING-054): 0 정상, 1 범위 초과, 2 미검증 측정 항목, 3 의심, 4 시각 보정, 5 예보값.
 * 여러 조건이면 4 > 1 > 3 > 2 순서로 하나를 고른다. 예보 소스는 5.
 */
public final class QualityAssigner {

    private QualityAssigner() {
    }

    public static int assign(boolean timeCorrected, boolean outOfRange, boolean suspect, boolean unverified, boolean forecast) {
        if (forecast) {
            return Quality.FORECAST;
        }
        if (timeCorrected) {
            return Quality.TIME_CORRECTED;
        }
        if (outOfRange) {
            return Quality.OUT_OF_RANGE;
        }
        if (suspect) {
            return Quality.SUSPECT;
        }
        if (unverified) {
            return Quality.UNVERIFIED;
        }
        return Quality.NORMAL;
    }
}
