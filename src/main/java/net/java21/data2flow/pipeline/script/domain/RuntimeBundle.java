package net.java21.data2flow.pipeline.script.domain;

import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 조직의 스크립트 실행 번들(API-SCR-32 {@code GET /internal/core/scripts/runtime-bundle?organizationId=}).
 * ACTIVE 버전 코드, 연결, 설정값, 실패 정책, 공유 모듈 버전(SCR-04.01), 수식 항목(SCR-01.06). 인스턴스 시작·EVT-SCR-01 수신·30초 폴링 때
 * 다시 읽는다(SCR-03.04). 재처리 작업은 만들 때의 번들을 JSON으로 고정해 둔다(BR-ING-12).
 *
 * @param bundleVersion 번들 버전(바뀌었을 때만 교체)
 * @param scripts       스크립트
 * @param modules       공유 모듈 버전(배포된 버전은 불변, BR-SCR-13)
 * @param formulas      수식 파생 항목(TRANSFORM 마지막에 실행, BR-SCR-19)
 */
public record RuntimeBundle(long bundleVersion, List<Script> scripts, List<Module> modules, List<Formula> formulas) {

    public static final RuntimeBundle EMPTY = new RuntimeBundle(-1, List.of());

    public RuntimeBundle {
        scripts = scripts == null ? List.of() : List.copyOf(scripts);
        modules = modules == null ? List.of() : List.copyOf(modules);
        formulas = formulas == null ? List.of() : List.copyOf(formulas);
    }

    public RuntimeBundle(long bundleVersion, List<Script> scripts) {
        this(bundleVersion, scripts, List.of(), List.of());
    }

    /** {@code name@version}으로 모듈 찾기 */
    public Optional<Module> module(String ref) {
        return modules.stream().filter(m -> m.ref().equals(ref)).findFirst();
    }

    /**
     * @param scriptId        스크립트 ID
     * @param kind            DECODE / TRANSFORM
     * @param versionId       ACTIVE 버전 ID
     * @param versionNo       ACTIVE 버전 번호
     * @param code            코드
     * @param config          설정값(ctx.config, SCR-04.02 — 새 버전 없이 바뀐다)
     * @param failurePolicy   스크립트 기본 실패 정책
     * @param enabled         스크립트 활성 여부(DISABLED·AUTO_DISABLED면 false, 건너뜀)
     * @param bindings        연결(SOURCE·MODEL·DEVICE)
     * @param moduleRefs      가져다 쓰는 모듈 {@code name@version}(SCR-04.01)
     * @param configRevision  설정값 판(바뀔 때마다 오름, 처리 기록 {@code configRevision})
     * @param logCaptureUntil 운영 로그 수집 끝 시각(SCR-05.02). 없으면 수집 안 함
     */
    public record Script(long scriptId, ScriptKind kind, long versionId, int versionNo, String code, JsonNode config,
                         FailurePolicy failurePolicy, boolean enabled, List<Binding> bindings, List<String> moduleRefs,
                         long configRevision, Instant logCaptureUntil) {

        public Script {
            bindings = bindings == null ? List.of() : List.copyOf(bindings);
            moduleRefs = moduleRefs == null ? List.of() : List.copyOf(moduleRefs);
        }

        public Script(long scriptId, ScriptKind kind, long versionId, int versionNo, String code, JsonNode config,
                      FailurePolicy failurePolicy, boolean enabled, List<Binding> bindings) {
            this(scriptId, kind, versionId, versionNo, code, config, failurePolicy, enabled, bindings, List.of(), 0, null);
        }

        /** 운영 로그 수집 중인가(BR-SCR-17: 켠 뒤 30분) */
        public boolean capturingLogs(Instant now) {
            return logCaptureUntil != null && now.isBefore(logCaptureUntil);
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

    /**
     * 공유 모듈의 배포된 버전 하나(SCR-04.01).
     *
     * @param name      이름(소문자·숫자·{@code -} 3~40자)
     * @param versionNo 버전 번호
     * @param code      코드({@code export function}·{@code exports.x = …}·{@code module.exports = …})
     */
    public record Module(String name, int versionNo, String code) {

        public String ref() {
            return name + "@" + versionNo;
        }
    }

    /**
     * 수식 파생 항목(SCR-01.06).
     *
     * @param id         ID
     * @param resultKey  결과 측정 키(파생, BR-SCR-07)
     * @param unit       단위. 없으면 null
     * @param expression 수식(예: {@code thi(temperature, humidity)}). pipeline이 직접 컴파일한다
     * @param compiledJs core가 미리 컴파일한 식(API-SCR-37 결과). {@code expression}이 없을 때만 쓴다
     * @param targetType MODEL, DEVICE, SPACE
     * @param targetId   대상 ID
     */
    public record Formula(long id, String resultKey, String unit, String expression, String compiledJs, String targetType,
                          long targetId) {
    }
}
