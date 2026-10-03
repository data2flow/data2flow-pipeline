package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.device.service.CoreDirectory;
import net.java21.data2flow.pipeline.script.domain.FailurePolicy;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 조직별 스크립트 실행 계획(SCR-03.04 라이브 리로드, BR-SCR-09). core의 실행 번들(API-SCR-32)을 읽어 두고,
 * EVT-SCR-01({@code entityType=SCRIPT}) 수신·30초 폴링·재연결 때 다시 읽는다. 새 번들은 참조 하나를 원자적으로 바꾸며,
 * 처리 단계는 메시지를 시작할 때 계획을 한 번 집어 끝까지 그 계획을 쓰므로 <b>한 메시지는 한 버전으로만</b> 처리된다.
 * 적용한 ACTIVE 버전이 바뀐 스크립트는 core에 인스턴스별 적용 보고(API-SCR-34)를 보낸다.
 */
public class ScriptRuntimeRegistry implements ScriptConfigLookup {

    private static final Logger log = LoggerFactory.getLogger(ScriptRuntimeRegistry.class);

    private final CoreDirectory core;
    private final Clock clock;
    private final String instanceId;
    private final Map<Long, RuntimeBundle> bundles = new ConcurrentHashMap<>();

    public ScriptRuntimeRegistry(CoreDirectory core, Clock clock, String instanceId) {
        this.core = core;
        this.clock = clock;
        this.instanceId = instanceId;
    }

    /** 조직의 현재 계획. 처음이면 core에서 읽는다(core 장애면 CoreUnavailableException) */
    public Plan plan(long organizationId) {
        RuntimeBundle bundle = bundles.get(organizationId);
        if (bundle == null) {
            bundle = reload(organizationId);
        }
        return new Plan(bundle);
    }

    /** 다시 읽어 바뀌었으면 교체하고 적용 보고. 새 번들을 돌려준다 */
    public RuntimeBundle reload(long organizationId) {
        RuntimeBundle fresh = core.runtimeBundle(organizationId);
        RuntimeBundle old = bundles.put(organizationId, fresh);
        if (old == null || old.bundleVersion() != fresh.bundleVersion()) {
            acknowledge(old, fresh);
        }
        return fresh;
    }

    /** 30초 폴링(TC-SCR-054: 이벤트를 놓쳐도 따라잡음) */
    public void refreshAll() {
        for (Long org : Set.copyOf(bundles.keySet())) {
            try {
                RuntimeBundle current = bundles.get(org);
                RuntimeBundle fresh = core.runtimeBundle(org);
                if (current == null || current.bundleVersion() != fresh.bundleVersion()) {
                    bundles.put(org, fresh);
                    acknowledge(current, fresh);
                }
            } catch (RuntimeException e) {
                log.warn("스크립트 번들을 읽지 못했습니다(org={}): {}", org, e.getMessage());
            }
        }
    }

    /** 재연결: 아는 조직 모두 다시 읽게 한다 */
    public void invalidateAll() {
        refreshAll();
    }

    @Override
    public Optional<JsonNode> config(long organizationId, long scriptId) {
        RuntimeBundle bundle = bundles.get(organizationId);
        if (bundle == null) {
            return Optional.empty();
        }
        return bundle.scripts().stream().filter(s -> s.scriptId() == scriptId).findFirst().map(RuntimeBundle.Script::config);
    }

    private void acknowledge(RuntimeBundle old, RuntimeBundle fresh) {
        for (RuntimeBundle.Script s : fresh.scripts()) {
            boolean changed = old == null || old.scripts().stream()
                    .noneMatch(o -> o.scriptId() == s.scriptId() && o.versionId() == s.versionId());
            if (changed) {
                try {
                    core.deployAck(instanceId, s.scriptId(), s.versionId(), clock.instant());
                } catch (RuntimeException e) {
                    log.warn("스크립트 적용 보고 실패(script={}, version={}): {}", s.scriptId(), s.versionId(), e.getMessage());
                }
            }
        }
    }

    /** 한 메시지를 처리하는 동안 쓰는 고정된 계획 */
    public record Plan(RuntimeBundle bundle) {

        public Optional<RuntimeBundle.Script> decodeScript(long scriptId) {
            return bundle.scripts().stream()
                    .filter(s -> s.scriptId() == scriptId && s.kind() == ScriptKind.DECODE && s.enabled())
                    .findFirst();
        }

        /** SOURCE에 연결된 활성 DECODE 스크립트 */
        public Optional<RuntimeBundle.Script> decodeScriptForSource(long sourceId) {
            return bundle.scripts().stream()
                    .filter(s -> s.kind() == ScriptKind.DECODE && s.enabled())
                    .filter(s -> s.bindings().stream().anyMatch(b -> "SOURCE".equalsIgnoreCase(b.targetType())
                            && b.targetId() == sourceId && b.enabled()))
                    .findFirst();
        }

        /** 실행 순서 모델 → 기기(BR-SCR-03). 연결별 실패 정책이 있으면 그것, 없으면 스크립트 기본값 */
        public List<Step> transforms(Long modelId, long deviceId) {
            List<Step> steps = new ArrayList<>();
            if (modelId != null) {
                collect(steps, "MODEL", modelId);
            }
            collect(steps, "DEVICE", deviceId);
            return steps;
        }

        private void collect(List<Step> steps, String type, long targetId) {
            for (RuntimeBundle.Script s : bundle.scripts()) {
                if (s.kind() != ScriptKind.TRANSFORM || !s.enabled()) {
                    continue;
                }
                for (RuntimeBundle.Binding b : s.bindings()) {
                    if (type.equalsIgnoreCase(b.targetType()) && b.targetId() == targetId && b.enabled()) {
                        steps.add(new Step(s, type, b.failurePolicy() != null ? b.failurePolicy() : s.failurePolicy()));
                    }
                }
            }
        }
    }

    /** TRANSFORM 한 단계 */
    public record Step(RuntimeBundle.Script script, String scope, FailurePolicy failurePolicy) {
    }
}
