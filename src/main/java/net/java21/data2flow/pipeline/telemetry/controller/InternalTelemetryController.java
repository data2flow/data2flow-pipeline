package net.java21.data2flow.pipeline.telemetry.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.pipeline.telemetry.dto.DerivedPointDtos;
import net.java21.data2flow.pipeline.telemetry.service.DerivedPointService;
import net.java21.data2flow.pipeline.telemetry.service.MetricRemapService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/** 시계열 내부 API: API-TSD-51 별칭 재매핑(core-api가 호출), API-ING-25 계산값 저장(analytics가 호출) */
@RestController
public class InternalTelemetryController {

    private final MetricRemapService remaps;
    private final DerivedPointService derived;

    public InternalTelemetryController(MetricRemapService remaps, DerivedPointService derived) {
        this.remaps = remaps;
        this.derived = derived;
    }

    /** API-ING-25 {@code POST /internal/pipeline/derived-points} → 200 {@code {accepted, rejected[{index, reason}]}} */
    @PostMapping("/internal/pipeline/derived-points")
    public ApiResponse<DerivedPointDtos.Response> derivedPoints(@Valid @RequestBody DerivedPointDtos.Request request) {
        return ApiResponse.success(derived.store(request));
    }

    /** API-TSD-51 {@code POST /internal/pipeline/telemetry/remap-metric} → 202 {@code {jobId}} */
    @PostMapping("/internal/pipeline/telemetry/remap-metric")
    public ResponseEntity<ApiResponse<RemapResponse>> remap(@Valid @RequestBody RemapRequest request) {
        String jobId = remaps.start(request.organizationId(), request.alias(), request.targetKey(), request.from(), request.to());
        return ResponseEntity.accepted().body(ApiResponse.success(new RemapResponse(jobId)));
    }

    public record RemapRequest(@NotNull @Positive Long organizationId, @NotBlank String alias, @NotBlank String targetKey,
                               Instant from, Instant to) {
    }

    public record RemapResponse(String jobId) {
    }
}
