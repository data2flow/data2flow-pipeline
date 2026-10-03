package net.java21.data2flow.pipeline.ingest.controller;

import jakarta.validation.Valid;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.pipeline.ingest.dto.DiscardDlqRequest;
import net.java21.data2flow.pipeline.ingest.dto.DiscardDlqResponse;
import net.java21.data2flow.pipeline.ingest.dto.ReprocessItemsRequest;
import net.java21.data2flow.pipeline.ingest.dto.ReprocessItemsResponse;
import net.java21.data2flow.pipeline.ingest.service.FailureService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 수집 내부 API(core-api가 호출, ADR-021): API-ING-22 선택 재처리, API-ING-23 기간 재처리 작업, API-ING-24 실패 메시지 일괄 폐기.
 * 권한 판정은 외부 API를 받는 core-api가 한다(INGEST_REPROCESS).
 */
@RestController
public class InternalIngestController {

    private final FailureService failures;
    private final net.java21.data2flow.pipeline.ingest.service.ReprocessJobService jobs;

    public InternalIngestController(FailureService failures,
                                    net.java21.data2flow.pipeline.ingest.service.ReprocessJobService jobs) {
        this.failures = failures;
        this.jobs = jobs;
    }

    /** API-ING-23 기간 재처리 작업 생성(202) */
    @PostMapping("/internal/pipeline/reprocess-jobs")
    public org.springframework.http.ResponseEntity<ApiResponse<net.java21.data2flow.pipeline.ingest.dto.ReprocessJobDtos.CreateResponse>>
            createJob(@Valid @RequestBody net.java21.data2flow.pipeline.ingest.dto.ReprocessJobDtos.CreateRequest request) {
        return org.springframework.http.ResponseEntity.accepted().body(ApiResponse.success(jobs.create(request)));
    }

    /** API-ING-23 재처리 작업 취소 */
    @PostMapping("/internal/pipeline/reprocess-jobs/{job-id}/cancel")
    public ApiResponse<net.java21.data2flow.pipeline.ingest.dto.ReprocessJobDtos.CancelResponse> cancelJob(
            @org.springframework.web.bind.annotation.PathVariable("job-id") long jobId,
            @Valid @RequestBody net.java21.data2flow.pipeline.ingest.dto.ReprocessJobDtos.CancelRequest request) {
        return ApiResponse.success(jobs.cancel(jobId, request));
    }

    @PostMapping("/internal/pipeline/reprocess-items")
    public ApiResponse<ReprocessItemsResponse> reprocessItems(@Valid @RequestBody ReprocessItemsRequest request) {
        return ApiResponse.success(failures.reprocess(request));
    }

    @PostMapping("/internal/pipeline/dlq-items/discard")
    public ApiResponse<DiscardDlqResponse> discard(@Valid @RequestBody DiscardDlqRequest request) {
        return ApiResponse.success(failures.discard(request));
    }
}
