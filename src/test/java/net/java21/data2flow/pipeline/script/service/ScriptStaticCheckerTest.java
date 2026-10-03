package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import net.java21.data2flow.pipeline.script.domain.ScriptProblem;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-03.01·04.05 · BR-SCR-05 · API-SCR-30: 정적 검사(문법, 금지 식별자 줄·열, globalThis 수정, 진입 함수, 반환 누락 경고) */
class ScriptStaticCheckerTest {

    private final ScriptStaticChecker checker = new ScriptStaticChecker(ScriptSandboxHarness.sandbox());

    @Test
    @DisplayName("[SCR-03.01][AT-SCR-01.2] 금지된 API는 \"금지된 API: require (3:9)\"처럼 줄·열과 함께 ERROR")
    void forbiddenWithPosition() {
        List<ScriptProblem> problems = checker.check(ScriptKind.TRANSFORM, """
                function transform(msg, ctx) {
                  // fetch 는 주석이라 괜찮다
                  const x = require('fs');
                  const s = "setTimeout은 글자라 괜찮다";
                  msg.process = 1; const o = {fetch: 1};
                  return msg;
                }
                """);

        assertThat(problems).extracting(ScriptProblem::message).containsExactly("금지된 API: require (3:13)");
        assertThat(ScriptStaticChecker.hasErrors(problems)).isTrue();
    }

    @Test
    @DisplayName("[SCR-04.05] 문법 오류·진입 함수 누락·globalThis 수정은 ERROR, 반환 누락은 WARNING(배포 가능)")
    void otherProblems() {
        assertThat(checker.check(ScriptKind.TRANSFORM, "function transform(msg, ctx) {"))
                .extracting(ScriptProblem::code).contains("SYNTAX_ERROR");
        assertThat(checker.check(ScriptKind.DECODE, "function transform(msg, ctx) { return msg; }"))
                .extracting(ScriptProblem::code).containsExactly("ENTRY_MISSING");
        assertThat(checker.check(ScriptKind.TRANSFORM, "globalThis.x = 1;\nfunction transform(m, c) { return m; }"))
                .extracting(ScriptProblem::code).containsExactly("SCRIPT_FORBIDDEN_API");
        List<ScriptProblem> warning = checker.check(ScriptKind.TRANSFORM, "function transform(msg, ctx) { msg.x = 1; }");
        assertThat(warning).extracting(ScriptProblem::severity).containsExactly(ScriptProblem.Severity.WARNING);
        assertThat(warning.getFirst().message()).isEqualTo("모든 경로에서 msg 또는 null을 반환해야 합니다");
        assertThat(ScriptStaticChecker.hasErrors(warning)).isFalse();
        assertThat(checker.check(ScriptKind.TRANSFORM, "const transform = (msg) => msg;")).isEmpty();
        assertThat(checker.check(ScriptKind.DECODE, "")).extracting(ScriptProblem::code).containsExactly("ENTRY_MISSING");
        assertThat(checker.check(ScriptKind.DECODE, "x".repeat(70_000))).extracting(ScriptProblem::code)
                .containsExactly("SCRIPT_CODE_TOO_LARGE");
        assertThat(checker.check(ScriptKind.TRANSFORM, "function transform(m, c) { const a = [...m.metrics]; return m; }"))
                .isEmpty();
    }
}
