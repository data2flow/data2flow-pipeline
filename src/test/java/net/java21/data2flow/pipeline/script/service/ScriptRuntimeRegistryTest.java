package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.device.service.FakeCoreDirectory;
import net.java21.data2flow.pipeline.script.domain.FailurePolicy;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-01.02 TC-SCR-005(순서) · SCR-03.04 TC-SCR-051·054(번들 교체·적용 보고·폴링) · SCR-02.03(연결별 실패 정책) */
class ScriptRuntimeRegistryTest {

    private static RuntimeBundle.Script transform(long id, long versionId, String type, long target, FailurePolicy policy,
                                                  FailurePolicy bindingPolicy) {
        return new RuntimeBundle.Script(id, ScriptKind.TRANSFORM, versionId, (int) versionId, "x",
                MessageCodec.newMapper().createObjectNode(), policy, true,
                List.of(new RuntimeBundle.Binding(type, target, true, bindingPolicy)));
    }

    @Test
    @DisplayName("[SCR-01.02][AT-SCR-04.1] TC-SCR-005 실행 순서는 모델 → 기기, 연결별 실패 정책이 스크립트 기본값보다 우선")
    void order() {
        FakeCoreDirectory core = new FakeCoreDirectory();
        core.bundle = new RuntimeBundle(1, List.of(
                transform(2, 20, "DEVICE", 17, FailurePolicy.FAIL_OPEN, FailurePolicy.FAIL_CLOSED),
                transform(1, 10, "MODEL", 6, FailurePolicy.FAIL_OPEN, null),
                transform(3, 30, "DEVICE", 99, FailurePolicy.FAIL_OPEN, null)));
        ScriptRuntimeRegistry registry = new ScriptRuntimeRegistry(core, MutableClock.atUtc("2026-10-03T00:00:00Z"), "p-0");

        List<ScriptRuntimeRegistry.Step> steps = registry.plan(1).transforms(6L, 17);

        assertThat(steps).extracting(s -> s.script().scriptId()).containsExactly(1L, 2L);
        assertThat(steps).extracting(ScriptRuntimeRegistry.Step::failurePolicy)
                .containsExactly(FailurePolicy.FAIL_OPEN, FailurePolicy.FAIL_CLOSED);
        assertThat(registry.plan(1).transforms(null, 99)).hasSize(1);
        assertThat(core.acks).containsExactlyInAnyOrder("p-0:2:20", "p-0:1:10", "p-0:3:30");
    }

    @Test
    @DisplayName("[SCR-03.04][AT-SCR-03.1] TC-SCR-051·054 새 번들 버전이면 원자 교체하고 바뀐 스크립트만 적용 보고, 폴링으로 놓친 배포를 따라잡는다")
    void reloadAndPoll() {
        FakeCoreDirectory core = new FakeCoreDirectory();
        core.bundle = new RuntimeBundle(1, List.of(transform(1, 10, "MODEL", 6, FailurePolicy.FAIL_OPEN, null)));
        ScriptRuntimeRegistry registry = new ScriptRuntimeRegistry(core, MutableClock.atUtc("2026-10-03T00:00:00Z"), "p-0");
        ScriptRuntimeRegistry.Plan before = registry.plan(1);

        core.bundle = new RuntimeBundle(2, List.of(transform(1, 11, "MODEL", 6, FailurePolicy.FAIL_OPEN, null)));
        registry.refreshAll();

        assertThat(before.transforms(6L, 1).getFirst().script().versionId()).as("이미 집은 계획은 그대로").isEqualTo(10);
        assertThat(registry.plan(1).transforms(6L, 1).getFirst().script().versionId()).isEqualTo(11);
        assertThat(core.acks).containsExactly("p-0:1:10", "p-0:1:11");
        registry.refreshAll();
        assertThat(core.acks).hasSize(2);
        assertThat(registry.config(1, 1)).isPresent();
        assertThat(registry.config(1, 9)).isEmpty();
        assertThat(registry.config(5, 1)).isEmpty();
        core.down = true;
        registry.invalidateAll();
        assertThat(registry.plan(1).bundle().bundleVersion()).isEqualTo(2);
    }
}
