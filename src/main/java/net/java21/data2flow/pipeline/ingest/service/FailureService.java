package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.pipeline.common.PipelineErrorCode;
import net.java21.data2flow.pipeline.ingest.domain.RawMessageStatus;
import net.java21.data2flow.pipeline.ingest.dto.DiscardDlqRequest;
import net.java21.data2flow.pipeline.ingest.dto.DiscardDlqResponse;
import net.java21.data2flow.pipeline.ingest.dto.ReprocessItemsRequest;
import net.java21.data2flow.pipeline.ingest.dto.ReprocessItemsResponse;
import net.java21.data2flow.pipeline.ingest.dto.ReprocessItemsResponse.Outcome;
import net.java21.data2flow.pipeline.ingest.repository.DlqItemRepository;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRepository;
import net.java21.data2flow.pipeline.ingest.repository.RawMessageRow;
import net.java21.data2flow.pipeline.script.service.RawInputLoader;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 실패 메시지 보관함 처리(ING-07.03): 선택 재처리(API-ING-22, core API-ING-07이 호출)와 일괄 폐기(API-ING-24, API-ING-11).
 * 재처리는 항목마다 잠그고(5분, 다른 사용자가 잡은 항목은 SKIPPED·ING_DLQ_ITEM_LOCKED) 현재 디코더·스크립트로 처리한 뒤
 * 같은 원본 행을 갱신한다. 성공하면 RESOLVED, 다시 실패하면 OPEN(시도 수 증가). 폐기해도 원본은 보관 규칙(30일)을 따른다.
 * 테스트 실행(API-SCR-31)이 원본 ID로 입력을 만들 때도 여기서 원본을 찾는다.
 */
public class FailureService implements RawInputLoader {

    public static final int MAX_BATCH = 5_000;
    private static final Duration LOCK_TTL = Duration.ofMinutes(5);

    private final DlqItemRepository dlq;
    private final RawMessageRepository raws;
    private final IngestProcessor processor;
    private final Clock clock;

    public FailureService(DlqItemRepository dlq, RawMessageRepository raws, IngestProcessor processor, Clock clock) {
        this.dlq = dlq;
        this.raws = raws;
        this.processor = processor;
        this.clock = clock;
    }

    public ReprocessItemsResponse reprocess(ReprocessItemsRequest request) {
        if (request.items().size() > MAX_BATCH) {
            throw new BusinessException(PipelineErrorCode.ING_DLQ_BATCH_TOO_LARGE);
        }
        long org = request.organizationId();
        Instant now = clock.instant();
        List<Long> dlqIds = request.items().stream().filter(i -> i.kind() == ReprocessItemsRequest.Kind.DLQ)
                .map(ReprocessItemsRequest.Item::id).distinct().toList();
        Map<Long, DlqItemRepository.DlqItem> locked = new HashMap<>();
        dlq.lockForReprocess(org, dlqIds, request.requestedBy(), now, now.plus(LOCK_TTL)).forEach(i -> locked.put(i.id(), i));
        Map<Long, DlqItemRepository.DlqItem> known = new HashMap<>();
        dlq.findByIds(org, dlqIds).forEach(i -> known.put(i.id(), i));
        List<ReprocessItemsResponse.Result> results = new ArrayList<>();
        for (ReprocessItemsRequest.Item item : request.items()) {
            if (item.kind() == ReprocessItemsRequest.Kind.DLQ) {
                DlqItemRepository.DlqItem lockedItem = locked.get(item.id());
                if (lockedItem == null) {
                    DlqItemRepository.DlqItem k = known.get(item.id());
                    String code = k == null ? "RESOURCE_NOT_FOUND"
                            : "REPROCESSING".equals(k.status()) ? PipelineErrorCode.ING_DLQ_ITEM_LOCKED.code() : "DLQ_" + k.status();
                    results.add(new ReprocessItemsResponse.Result("DLQ", item.id(), Outcome.SKIPPED, code, null));
                    continue;
                }
                results.add(reprocessRaw("DLQ", item.id(), org, lockedItem.rawMessageId(), lockedItem.errorCode(), true));
            } else {
                Optional<RawMessageRow> raw = raws.findById(org, item.id());
                if (raw.isEmpty()) {
                    results.add(new ReprocessItemsResponse.Result("RAW", item.id(), Outcome.SKIPPED,
                            PipelineErrorCode.ING_RAW_MESSAGE_NOT_FOUND.code(), null));
                } else if (!raw.get().status().reprocessable()) {
                    results.add(new ReprocessItemsResponse.Result("RAW", item.id(), Outcome.SKIPPED,
                            "ING_REPROCESS_SAME_RESULT", raw.get().errorCode()));
                } else {
                    results.add(reprocessRaw("RAW", item.id(), org, item.id(), raw.get().errorCode(), false));
                }
            }
        }
        int ok = (int) results.stream().filter(r -> r.outcome() == Outcome.OK).count();
        int failed = (int) results.stream().filter(r -> r.outcome() == Outcome.FAILED).count();
        return new ReprocessItemsResponse(results, ok, failed, results.size() - ok - failed);
    }

    private ReprocessItemsResponse.Result reprocessRaw(String kind, long id, long org, long rawId, String previous,
                                                       boolean fromDlq) {
        Optional<RawMessageRow> raw = raws.findById(org, rawId);
        if (raw.isEmpty()) {
            return new ReprocessItemsResponse.Result(kind, id, Outcome.SKIPPED,
                    PipelineErrorCode.ING_RAW_MESSAGE_NOT_FOUND.code(), previous);
        }
        IngestProcessor.Outcome outcome = processor.reprocess(raw.get());
        boolean ok = outcome.status() == RawMessageStatus.OK || outcome.status() == RawMessageStatus.DUPLICATE;
        if (!ok && fromDlq && outcome.status().dlqStage() == null) {
            // 실패했지만 보관함 대상 상태가 아님(예: INVALID): 잠금을 풀어 다시 OPEN
            dlq.markReopened(org, id, outcome.errorCode() == null ? outcome.status().name() : outcome.errorCode(), null);
        }
        return new ReprocessItemsResponse.Result(kind, id, ok ? Outcome.OK : Outcome.FAILED,
                ok ? null : (outcome.errorCode() == null ? outcome.status().name() : outcome.errorCode()), previous);
    }

    public DiscardDlqResponse discard(DiscardDlqRequest request) {
        if (request.dlqItemIds().size() > MAX_BATCH) {
            throw new BusinessException(PipelineErrorCode.ING_DLQ_BATCH_TOO_LARGE);
        }
        return new DiscardDlqResponse(dlq.discard(request.organizationId(), request.dlqItemIds(), request.reason(),
                clock.instant()));
    }

    @Override
    public Optional<RawInput> load(long organizationId, long rawMessageId) {
        return raws.findById(organizationId, rawMessageId)
                .map(r -> new RawInput(r.topic(), r.payload(), r.receivedAt(), r.sourceId(), r.sourceType()));
    }
}
