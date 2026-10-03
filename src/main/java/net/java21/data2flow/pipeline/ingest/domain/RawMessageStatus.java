package net.java21.data2flow.pipeline.ingest.domain;

import java.util.EnumSet;
import java.util.Set;

/**
 * 원본 처리 상태(ING-01.02, ING domain-model "상태 전이: RawMessage.status").
 *
 * <pre>
 * RECEIVED ─▶ OK · DUPLICATE · DECODE_ERROR · SCRIPT_ERROR · UNKNOWN_DEVICE_REJECTED · INVALID · STORE_ERROR · PUBLISH_ERROR
 * DECODE_ERROR · SCRIPT_ERROR · STORE_ERROR · PUBLISH_ERROR ─(재처리)─▶ OK 또는 같은/다른 실패 상태
 * </pre>
 * 종결 상태에서 OK로 바로 가는 일반 전이는 없고 재처리 경로만 허용한다.
 */
public enum RawMessageStatus {
    RECEIVED, OK, DUPLICATE, DECODE_ERROR, SCRIPT_ERROR, UNKNOWN_DEVICE_REJECTED, INVALID, STORE_ERROR, PUBLISH_ERROR;

    private static final Set<RawMessageStatus> REPROCESSABLE = EnumSet.of(DECODE_ERROR, SCRIPT_ERROR, STORE_ERROR,
            PUBLISH_ERROR, UNKNOWN_DEVICE_REJECTED);

    /** 일반 처리 전이(RECEIVED에서 결과로) */
    public boolean canTransitionTo(RawMessageStatus next) {
        return this == RECEIVED && next != RECEIVED;
    }

    /** 재처리로 다시 처리할 수 있는 상태(INVALID·DUPLICATE·OK는 같은 결과가 나오므로 제외) */
    public boolean reprocessable() {
        return REPROCESSABLE.contains(this);
    }

    /** 재처리 전이: 재처리할 수 있는 상태에서 RECEIVED 외 아무 결과로 */
    public boolean canReprocessTo(RawMessageStatus next) {
        return reprocessable() && next != RECEIVED;
    }

    /** 실패 메시지 보관함(DLQ)에 올리는 단계. 없으면 null */
    public DlqStage dlqStage() {
        return switch (this) {
            case DECODE_ERROR -> DlqStage.DECODE;
            case SCRIPT_ERROR -> DlqStage.SCRIPT;
            case STORE_ERROR -> DlqStage.STORE;
            case PUBLISH_ERROR -> DlqStage.PUBLISH;
            default -> null;
        };
    }
}
