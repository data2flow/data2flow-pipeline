package net.java21.data2flow.pipeline.telemetry.repository;

import java.time.Instant;

/** {@code link_qualities} 한 행(게이트웨이마다 한 행, TSD-01.02). 기기 자체 신호면 gatewayEui는 빈 문자열 */
public record LinkQualityRow(long deviceId, String gatewayEui, Instant time, long organizationId, Double rssi, Double snr,
                             Long frameCounter) {
}
