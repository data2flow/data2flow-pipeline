package net.java21.data2flow.pipeline.retention.controller;

import jakarta.validation.Valid;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.pipeline.retention.dto.RetentionDtos;
import net.java21.data2flow.pipeline.retention.repository.RetentionRepository;
import net.java21.data2flow.pipeline.retention.service.RetentionPolicyCache;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 보관 내부 API(core-api가 호출, ADR-021): API-TSD-53 정책 적용 통지(캐시를 바로 다시 읽고 다음 야간 작업 시각을 돌려준다),
 * API-TSD-62 보관 기간을 줄일 때 지울 행 수·크기 미리 보기(API-TSD-41의 근거, BR-TSD-03 — 확인 토큰은 core가 관리).
 */
@RestController
public class InternalRetentionController {

    /** 데이터 종류 → (표, 시간 열) */
    private static final Map<String, String[]> TABLES = Map.of(
            "RAW_MESSAGE", new String[]{"raw_messages", "received_at"},
            "TELEMETRY", new String[]{"telemetry", "time"},
            "LINK", new String[]{"link_qualities", "time"},
            "AGG_1M", new String[]{"telemetry_1m", "bucket"},
            "AGG_1H", new String[]{"telemetry_1h", "bucket"},
            "AGG_1D", new String[]{"telemetry_1d", "bucket"});

    private final RetentionPolicyCache policies;
    private final RetentionRepository repository;
    private final Clock clock;

    public InternalRetentionController(RetentionPolicyCache policies, RetentionRepository repository, Clock clock) {
        this.policies = policies;
        this.repository = repository;
        this.clock = clock;
    }

    @PostMapping("/internal/pipeline/retention/apply-policy")
    public ApiResponse<RetentionDtos.ApplyPolicyResponse> apply(@Valid @RequestBody RetentionDtos.ApplyPolicyRequest request) {
        policies.refresh();
        return ApiResponse.success(new RetentionDtos.ApplyPolicyResponse(nextNightly(clock.instant())));
    }

    @PostMapping("/internal/pipeline/retention/preview")
    public ApiResponse<RetentionDtos.PreviewResponse> preview(@Valid @RequestBody RetentionDtos.PreviewRequest request) {
        long org = request.organizationId();
        Instant now = clock.instant();
        List<RetentionDtos.ByItem> items = new ArrayList<>();
        long rows = 0;
        long bytes = 0;
        for (RetentionDtos.Item i : request.items()) {
            String[] table = TABLES.get(i.dataClass());
            if (table == null) {
                throw new BusinessException(CommonErrorCode.INVALID_REQUEST);
            }
            if (i.retainDays() == 0) {
                items.add(new RetentionDtos.ByItem(i.scope(), i.scopeRef(), i.dataClass(), 0, 0));
                continue;
            }
            String extra = null;
            Object[] args = new Object[0];
            if ("METRIC".equalsIgnoreCase(i.scope()) && i.scopeRef() != null && !table[0].equals("raw_messages")
                    && !table[0].equals("link_qualities")) {
                extra = "metric_key = ?";
                args = new Object[]{i.scopeRef()};
            }
            long n = repository.countBefore(table[0], table[1], org, now.minus(Duration.ofDays(i.retainDays())), extra, args);
            long total = repository.countRows(table[0]);
            long size = total == 0 ? 0 : repository.sizeOf(table[0]) * n / total;
            rows += n;
            bytes += size;
            items.add(new RetentionDtos.ByItem(i.scope(), i.scopeRef(), i.dataClass(), n, size));
        }
        return ApiResponse.success(new RetentionDtos.PreviewResponse(rows, bytes, items));
    }

    /** 다음 야간 보관 정리 시각(매일 02:00 UTC) */
    static Instant nextNightly(Instant now) {
        ZonedDateTime at = now.atZone(ZoneOffset.UTC).with(LocalTime.of(2, 0));
        return (at.toInstant().isAfter(now) ? at : at.plusDays(1)).toInstant();
    }
}
