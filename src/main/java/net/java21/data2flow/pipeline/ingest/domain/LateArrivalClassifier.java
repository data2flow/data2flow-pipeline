package net.java21.data2flow.pipeline.ingest.domain;

import java.time.Duration;
import java.time.Instant;

/** 늦은 도착(ING-06.03): 측정 시각이 수신 시각보다 1시간 이상 과거면 late(telemetry.flags 비트 1, 실시간 판정 제외 BR-ING-11) */
public final class LateArrivalClassifier {

    private final Duration threshold;

    public LateArrivalClassifier(Duration threshold) {
        this.threshold = threshold;
    }

    public boolean isLate(Instant measuredAt, Instant receivedAt) {
        return !measuredAt.isAfter(receivedAt.minus(threshold));
    }
}
