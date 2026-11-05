package net.java21.data2flow.pipeline.telemetry.repository;

import java.time.Instant;

/** {@code telemetry} 한 행(측정 항목마다 한 행, BR-TSD-24). flags 비트: 1 late, 2 imported, 4 reprocessed, 8 state_change, 16 derived */
public record TelemetryRow(long deviceId, String metricKey, Instant time, long organizationId, double value, int quality,
                           int flags, boolean virtual, Instant receivedAt, Long rawMessageId) {

    public static final int FLAG_LATE = 1;
    public static final int FLAG_IMPORTED = 2;
    public static final int FLAG_REPROCESSED = 4;
    public static final int FLAG_STATE_CHANGE = 8;
    /** 가상 센서 계산값(API-ING-25, ENE-02.02) */
    public static final int FLAG_DERIVED = 16;
}
