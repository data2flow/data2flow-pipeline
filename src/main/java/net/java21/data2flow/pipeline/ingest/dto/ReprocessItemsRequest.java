package net.java21.data2flow.pipeline.ingest.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.List;

/**
 * API-ING-22 {@code POST /internal/pipeline/reprocess-items} 요청 {@code {organizationId, requestedBy, items:[{kind: DLQ|RAW, id}]}}
 * (5,000건 이하, 넘으면 ING_DLQ_BATCH_TOO_LARGE)
 */
public record ReprocessItemsRequest(@NotNull @Positive Long organizationId, @NotNull Long requestedBy,
                                    @NotNull List<@Valid Item> items) {

    public record Item(@NotNull Kind kind, @NotNull Long id) {
    }

    public enum Kind {
        DLQ, RAW
    }
}
