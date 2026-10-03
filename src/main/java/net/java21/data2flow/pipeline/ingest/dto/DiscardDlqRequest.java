package net.java21.data2flow.pipeline.ingest.dto;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.List;

/** API-ING-24 {@code POST /internal/pipeline/dlq-items/discard} 요청(API-ING-11 처리) */
public record DiscardDlqRequest(@NotNull @Positive Long organizationId, @NotNull Long requestedBy,
                                @NotEmpty List<Long> dlqItemIds, @NotNull @Size(min = 2, max = 200) String reason) {
}
