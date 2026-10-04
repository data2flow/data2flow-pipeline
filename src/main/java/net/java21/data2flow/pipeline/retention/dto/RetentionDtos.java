package net.java21.data2flow.pipeline.retention.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.List;

/** 보관 내부 API(API-TSD-53 정책 적용 통지, API-TSD-62 삭제 예정 미리 보기) 요청·응답 */
public final class RetentionDtos {

    private RetentionDtos() {
    }

    /** API-TSD-53 {@code {organizationId, policyVersion}} */
    public record ApplyPolicyRequest(@NotNull Long organizationId, Long policyVersion) {
    }

    /** API-TSD-53 응답 {@code {appliesAt}}(다음 야간 작업 시각) */
    public record ApplyPolicyResponse(Instant appliesAt) {
    }

    /** API-TSD-62 {@code {organizationId, items[{scope, scopeRef?, dataClass, retainDays}]}} — 변경안 */
    public record PreviewRequest(@NotNull Long organizationId, @NotNull @Size(min = 1, max = 200) @Valid List<Item> items) {
    }

    public record Item(@NotNull String scope, String scopeRef, @NotNull String dataClass, @NotNull @Min(0) Integer retainDays) {
    }

    /** API-TSD-62 응답 {@code {affectedRows, affectedBytes, byMetric[]}}(API-TSD-41 근거, BR-TSD-03) */
    public record PreviewResponse(long affectedRows, long affectedBytes, List<ByItem> byMetric) {
    }

    public record ByItem(String scope, String scopeRef, String dataClass, long rows, long bytes) {
    }
}
