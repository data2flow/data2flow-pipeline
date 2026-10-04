package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.domain.ScriptProblem;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import net.java21.data2flow.script.sandbox.ScriptOutcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SCR-04.01 공유 모듈과 버전 지정 가져오기 */
class ScriptModuleLinkerTest {

    private static final RuntimeBundle.Module V1 = new RuntimeBundle.Module("milesight", 1, """
            export function parseChannels(hex) {
              return { temperature: parseInt(hex.substring(0, 2), 16) / 2 };
            }
            export const VERSION = 1;
            """);
    private static final RuntimeBundle.Module V2 = new RuntimeBundle.Module("milesight", 2, """
            exports.parseChannels = function (hex) { return { temperature: -1 }; };
            exports.VERSION = 2;
            """);
    private static final RuntimeBundle.Module EVIL = new RuntimeBundle.Module("evil-mod", 1, """
            export function steal() { return fetch('http://x'); }
            """);
    private static final RuntimeBundle.Module CYCLE_A = new RuntimeBundle.Module("cyc-a", 1,
            "import { b } from 'module:cyc-b@1';\nexport const a = 1;");
    private static final RuntimeBundle.Module CYCLE_B = new RuntimeBundle.Module("cyc-b", 1,
            "import { a } from 'module:cyc-a@1';\nexport const b = 2;");
    private static final RuntimeBundle BUNDLE = new RuntimeBundle(1, List.of(), List.of(V1, V2, EVIL, CYCLE_A, CYCLE_B),
            List.of());

    private final ScriptRunner runner = new ScriptRunner(ScriptSandboxHarness.sandbox(), null);
    private final ScriptStaticChecker checker = new ScriptStaticChecker(ScriptSandboxHarness.sandbox());

    private ScriptOutcome transform(String code, List<String> refs) {
        ObjectNode ctx = MessageCodec.newMapper().createObjectNode();
        return runner.runUnsaved(BUNDLE, code, ScriptKind.TRANSFORM, refs, ScriptSandboxHarness.NORMAL_INPUT, ctx,
                ScriptRunner.Execution.test(1, Instant.parse("2026-10-03T00:00:00Z")));
    }

    @Test
    @DisplayName("[SCR-04.01][AT-SCR-08.1] TC-SCR-067 import { parseChannels } from 'module:milesight@1' 고정 버전 연결, ctx.modules로도 쓴다")
    void pinnedImport() {
        ScriptOutcome v1 = transform("""
                import { parseChannels, VERSION as V } from 'module:milesight@1';
                function transform(msg, ctx) {
                  msg.metrics[0].value = parseChannels('2d').temperature + V;
                  msg.metrics.push({key: 'v2', value: ctx.modules['milesight@2'].VERSION});
                  return msg;
                }""", List.of("milesight@2"));

        assertThat(v1.ok()).as(String.valueOf(v1.failure())).isTrue();
        assertThat(v1.output().get("metrics").get(0).get("value").asDouble()).isEqualTo(23.5);
        assertThat(v1.output().get("metrics").get(1).get("value").asInt()).isEqualTo(2);
    }

    @Test
    @DisplayName("[SCR-04.01] TC-SCR-067 없는 버전 → SCRIPT_MODULE_NOT_FOUND(검사 오류·실행 오류 모두), 줄·열 위치")
    void missingVersion() {
        String code = "function transform(msg, ctx) { return msg; }\nimport { parseChannels } from 'module:milesight@9';";
        List<ScriptProblem> problems = checker.check(ScriptKind.TRANSFORM, code, List.of(), BUNDLE::module);
        assertThat(problems).extracting(ScriptProblem::code).contains(ScriptLinker.MODULE_NOT_FOUND);
        assertThat(problems).filteredOn(p -> p.code().equals(ScriptLinker.MODULE_NOT_FOUND)).first()
                .satisfies(p -> assertThat(p.line()).isEqualTo(2));
        ScriptOutcome run = transform(code, List.of());
        assertThat(run.ok()).isFalse();
        assertThat(run.failure().message()).startsWith(ScriptLinker.MODULE_NOT_FOUND);
    }

    @Test
    @DisplayName("[SCR-04.01] TC-SCR-067 외부 패키지(lodash)·버전 없는 모듈 거부, 모듈 안의 금지 API는 같은 검사 오류, 순환 거부")
    void rejects() {
        assertThat(checker.check(ScriptKind.TRANSFORM, "import _ from 'lodash';\nfunction transform(msg, ctx) { return msg; }",
                List.of(), BUNDLE::module)).extracting(ScriptProblem::code).contains(ScriptLinker.MODULE_NOT_FOUND);
        assertThatThrownBy(() -> ScriptLinker.link("import { x } from 'module:milesight';", "transform", List.of(),
                BUNDLE::module)).isInstanceOf(ScriptLinker.LinkException.class);
        assertThat(checker.check(ScriptKind.TRANSFORM,
                "import { steal } from 'module:evil-mod@1';\nfunction transform(msg, ctx) { return msg; }", List.of(),
                BUNDLE::module)).extracting(ScriptProblem::code).contains("SCRIPT_FORBIDDEN_API")
                .doesNotContain(ScriptLinker.MODULE_NOT_FOUND);
        assertThatThrownBy(() -> ScriptLinker.link("import { a } from 'module:cyc-a@1';\nfunction transform(m, c) { return m; }",
                "transform", List.of(), BUNDLE::module)).isInstanceOf(ScriptLinker.LinkException.class)
                .hasMessageContaining("순환");
        assertThat(checker.check(ScriptKind.TRANSFORM, """
                import { parseChannels } from 'module:milesight@1';
                function transform(msg, ctx) { return msg; }""", List.of(), BUNDLE::module)).isEmpty();
        assertThatThrownBy(() -> ScriptLinker.link("function transform(m, c) { return m; }", "transform", List.of("bad"),
                BUNDLE::module)).isInstanceOf(ScriptLinker.LinkException.class);
    }

    @Test
    @DisplayName("[SCR-01.02][BR-SCR-19] ctx.window(key, '10m'): 기준 시각 앞 10분 값만, 사용자는 ctx.runtime을 볼 수 없다")
    void window() {
        long base = Instant.parse("2026-10-03T00:00:00Z").toEpochMilli();
        Map<String, List<double[]>> window = Map.of("co2", List.of(new double[]{base - 601_000, 1000},
                new double[]{base - 600_000, 500}, new double[]{base - 60_000, 700}));
        ObjectNode ctx = MessageCodec.newMapper().createObjectNode();
        ScriptOutcome out = runner.runUnsaved(BUNDLE, """
                function transform(msg, ctx) {
                  const w = ctx.window('co2', '10m');
                  msg.metrics.push({key: 'n', value: w.length});
                  msg.metrics.push({key: 's', value: w.reduce((a, b) => a + b, 0)});
                  msg.metrics.push({key: 'rt', value: ctx.runtime === undefined ? 0 : 1});
                  return msg;
                }""", ScriptKind.TRANSFORM, List.of(), ScriptSandboxHarness.NORMAL_INPUT, ctx,
                new ScriptRunner.Execution(1, 7L, null, Instant.parse("2026-10-03T00:00:05Z"),
                        Instant.parse("2026-10-03T00:00:00Z"), window, false));

        assertThat(out.ok()).as(String.valueOf(out.failure())).isTrue();
        assertThat(out.output().get("metrics").get(1).get("value").asInt()).isEqualTo(2);
        assertThat(out.output().get("metrics").get(2).get("value").asInt()).isEqualTo(1200);
        assertThat(out.output().get("metrics").get(3).get("value").asInt()).isZero();
        assertThat(ScriptRunner.windowKeys("ctx.window('co2', '10m'); ctx.window(\"temp\"); ctx.window('co2','1h')"))
                .containsEntry("co2", java.time.Duration.ofHours(1)).containsEntry("temp", java.time.Duration.ofHours(24));
    }
}
