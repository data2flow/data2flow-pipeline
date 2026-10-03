package net.java21.data2flow.pipeline.ingest.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * 측정 시각 보정(ING-02.05, BR-ING-04): 없거나, 수신 시각보다 5분 넘게 미래이거나, 과거 한도(기본 7일)를 넘으면 수신 시각으로 바꾸고
 * 품질 4. 가상 환경 메시지(simRunId)·엣지 버퍼 배치·가져오기는 원래 시각을 지킨다(가속·재생·장기 버퍼링).
 */
public final class MeasuredAtNormalizer {

    private final Duration futureTolerance;
    private final Duration pastLimit;

    public MeasuredAtNormalizer(Duration futureTolerance, Duration pastLimit) {
        this.futureTolerance = futureTolerance;
        this.pastLimit = pastLimit;
    }

    /**
     * @param measuredAt  디코딩된 측정 시각. 없으면 null
     * @param receivedAt  수신 시각
     * @param keepOriginal 원래 시각을 지키는 경로(가상·엣지 배치·가져오기)
     */
    public Result normalize(Instant measuredAt, Instant receivedAt, boolean keepOriginal) {
        if (measuredAt == null) {
            return new Result(receivedAt, true, "MISSING");
        }
        if (keepOriginal) {
            return new Result(measuredAt, false, null);
        }
        if (measuredAt.isAfter(receivedAt.plus(futureTolerance))) {
            return new Result(receivedAt, true, "FUTURE");
        }
        if (measuredAt.isBefore(receivedAt.minus(pastLimit))) {
            return new Result(receivedAt, true, "TOO_OLD");
        }
        return new Result(measuredAt, false, null);
    }

    /**
     * @param measuredAt 보정 후 측정 시각
     * @param corrected  보정했는지(품질 4)
     * @param reason     MISSING, FUTURE, TOO_OLD. 보정하지 않았으면 null
     */
    public record Result(Instant measuredAt, boolean corrected, String reason) {
    }
}
