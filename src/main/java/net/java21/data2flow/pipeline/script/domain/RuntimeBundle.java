package net.java21.data2flow.pipeline.script.domain;

import tools.jackson.databind.JsonNode;

import java.util.List;

/**
 * 조직의 스크립트 실행 번들(API-SCR-32 {@code GET /internal/core/scripts/runtime-bundle?organizationId=}).
 * ACTIVE 버전 코드, 연결, 설정값, 실패 정책. 인스턴스 시작·EVT-SCR-01 수신·30초 폴링 때 다시 읽는다(SCR-03.04).
 *
 * @param bundleVersion 번들 버전(바뀌었을 때만 교체)
 * @param scripts       스크립트
 */
public record RuntimeBundle(long bundleVersion, List<Script> scripts) {

    public static final RuntimeBundle EMPTY = new RuntimeBundle(-1, List.of());

    public RuntimeBundle {
        scripts = scripts == null ? List.of() : List.copyOf(scripts);
    }

    /**
     * @param scriptId      스크립트 ID
     * @param kind          DECODE / TRANSFORM
     * @param versionId     ACTIVE 버전 ID
     * @param versionNo     ACTIVE 버전 번호
     * @param code          코드
     * @param config        설정값(ctx.config)
     * @param failurePolicy 스크립트 기본 실패 정책
     * @param enabled       스크립트 활성 여부(DISABLED·AUTO_DISABLED면 false, 건너뜀)
     * @param bindings      연결(SOURCE·MODEL·DEVICE)
     */
    public record Script(long scriptId, ScriptKind kind, long versionId, int versionNo, String code, JsonNode config,
                         FailurePolicy failurePolicy, boolean enabled, List<Binding> bindings) {

        public Script {
            bindings = bindings == null ? List.of() : List.copyOf(bindings);
        }
    }

    /**
     * @param targetType    SOURCE, MODEL, DEVICE
     * @param targetId      대상 ID
     * @param enabled       연결 활성
     * @param failurePolicy 연결별 실패 정책(API-SCR-09). 없으면 스크립트 기본값
     */
    public record Binding(String targetType, long targetId, boolean enabled, FailurePolicy failurePolicy) {
    }
}
