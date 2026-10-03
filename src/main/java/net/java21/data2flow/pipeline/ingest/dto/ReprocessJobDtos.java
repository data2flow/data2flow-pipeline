package net.java21.data2flow.pipeline.ingest.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.List;

/** API-ING-23 재처리 작업 요청·응답(ING-api §2) */
public final class ReprocessJobDtos {

    private ReprocessJobDtos() {
    }

    /** 생성 {@code {organizationId, requestedBy, sourceId, deviceIds?[], from, to, onlyFailed, memo?}} */
    public record CreateRequest(@NotNull @Positive Long organizationId, @NotNull Long requestedBy, @NotNull Long sourceId,
                                List<Long> deviceIds, @NotNull Instant from, @NotNull Instant to, boolean onlyFailed,
                                @Size(max = 200) String memo) {
    }

    /** 생성 202 {@code {jobId, status: QUEUED, estimatedCount}} */
    public record CreateResponse(long jobId, String status, long estimatedCount) {
    }

    /** 취소 {@code {organizationId, requestedBy}} */
    public record CancelRequest(@NotNull @Positive Long organizationId, Long requestedBy) {
    }

    /** 취소 {@code {jobId, status: CANCELLING}} */
    public record CancelResponse(long jobId, String status) {
    }
}
