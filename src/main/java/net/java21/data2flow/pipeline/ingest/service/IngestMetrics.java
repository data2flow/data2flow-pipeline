package net.java21.data2flow.pipeline.ingest.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;

import java.time.Duration;

/**
 * 수집 지표(OPS-01.02, ING-05.03): 소스·처리 결과별 건수 {@code data2flow_ingest_messages_total}, 수신 → 발행 지연
 * {@code ingest_e2e_latency}(재처리 제외, TC-ING-067), 스크립트 실행 시간 {@code data2flow_script_execution}.
 */
public class IngestMetrics {

    private final MeterRegistry registry;

    public IngestMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void processed(long sourceId, RawMessageStatus status) {
        Counter.builder("data2flow.ingest.messages")
                .description("처리 결과별 원본 메시지 수(ING-01.02)")
                .tag("source_id", Long.toString(sourceId))
                .tag("status", status.name())
                .register(registry).increment();
    }

    /** 플랫폼 브로커 기기 서명 거부(DSC-03.03, {@code data2flow_ingest_signature_rejected_total}) */
    public void signatureRejected(long sourceId) {
        Counter.builder("data2flow.ingest.signature.rejected")
                .description("플랫폼 브로커 기기 서명 없음·불일치로 거부한 메시지 수(DSC-03.03·03.05)")
                .tag("source_id", Long.toString(sourceId))
                .register(registry).increment();
    }

    /** 수신에서 표준 메시지 발행까지(재처리 메시지는 부르지 않는다) */
    public void latency(long sourceId, Duration latency) {
        Timer.builder("ingest.e2e.latency")
                .description("수신(ingress) → data2flow.telemetry 발행 지연(ING-05.03, NFR-01.01)")
                .tag("source_id", Long.toString(sourceId))
                .register(registry).record(latency.isNegative() ? Duration.ZERO : latency);
    }

    public void script(long scriptId, int versionNo, double durationMs, boolean ok) {
        Timer.builder("data2flow.script.execution")
                .description("스크립트 실행 시간(SCR-03.05)")
                .tag("script_id", Long.toString(scriptId))
                .tag("version", Integer.toString(versionNo))
                .tag("result", ok ? "ok" : "error")
                .register(registry).record(Duration.ofNanos((long) (durationMs * 1_000_000)));
    }
}
