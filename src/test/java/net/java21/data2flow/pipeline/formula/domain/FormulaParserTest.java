package net.java21.data2flow.pipeline.formula.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SCR-01.06 수식형 파생 항목 */
class FormulaParserTest {

    private static final Set<String> KNOWN = Set.of("temperature", "humidity", "co2");

    private static FormulaCompiler.Compiled compile(String expr) {
        return FormulaCompiler.compile(expr, KNOWN::contains);
    }

    @Test
    @DisplayName("[SCR-01.06][AT-SCR-06.3] TC-SCR-015 thi(temperature, humidity)·temperature * 1.8 + 32·rolling_mean(co2, 10m) 파싱")
    void parses() {
        FormulaCompiler.Compiled thi = compile("thi(temperature, humidity)");
        assertThat(thi.js()).isEqualTo("U.thi(__m(\"temperature\"), __m(\"humidity\"))");
        assertThat(thi.inputs()).containsExactly("temperature", "humidity");

        assertThat(compile("temperature * 1.8 + 32").js()).isEqualTo("__m(\"temperature\") * 1.8 + 32");
        FormulaCompiler.Compiled rolling = compile("rolling_mean(co2, 10m)");
        assertThat(rolling.js()).isEqualTo("__roll(\"co2\", 600000, \"mean\")");
        assertThat(rolling.windows()).containsEntry("co2", Duration.ofMinutes(10));
        assertThat(compile("-(temperature - 2) ^ 2 % 7 / max(1, humidity, 3)").js())
                .isEqualTo("(-Math.pow((__m(\"temperature\") - 2), 2)) % 7 / Math.max(1, __m(\"humidity\"), 3)");
        assertThat(compile("round(dew_point(temperature, humidity), 1) + rolling_max(co2, 1h) - rolling_count(co2, 90s)")
                .windows()).containsEntry("co2", Duration.ofHours(1));
    }

    @Test
    @DisplayName("[SCR-01.06] TC-SCR-015 오타 temprature * 2 → SCRIPT_FORMULA_INVALID \"알 수 없는 측정 항목: temprature\"(열 1)")
    void unknownMetric() {
        assertThatThrownBy(() -> compile("temprature * 2")).isInstanceOf(FormulaCompiler.FormulaException.class)
                .hasMessage("알 수 없는 측정 항목: temprature")
                .satisfies(e -> {
                    assertThat(((FormulaCompiler.FormulaException) e).col()).isEqualTo(1);
                    assertThat(((FormulaCompiler.FormulaException) e).line()).isEqualTo(1);
                });
    }

    @ParameterizedTest(name = "[SCR-01.06] TC-SCR-015 거부: {0} → {1}")
    @CsvSource(delimiter = '|', value = {
            "(temperature * 2|괄호가 맞지 않습니다",
            "temperature * 2)|괄호가 맞지 않습니다",
            "foo(temperature)|알 수 없는 함수: foo",
            "rolling_mean(co2, 0m)|창 길이는 1분~24시간입니다: 0m",
            "rolling_mean(co2, 25h)|창 길이는 1분~24시간입니다: 25h",
            "rolling_mean(co2, ten)|기간 형식이 아닙니다(예: 10m): ten",
            "rolling_mean(co2 10m)|rolling 함수는 (측정 항목, 기간) 두 인자입니다",
            "thi(temperature)|thi 함수의 인자 수가 맞지 않습니다(2개)",
            "temperature +|수식이 끝나지 않았습니다",
            "temperature # 2|알 수 없는 글자입니다: #",
            "1..2|숫자 형식이 아닙니다: 1..2",
            "rolling_mean(cox, 10m)|알 수 없는 측정 항목: cox"})
    void rejects(String expr, String message) {
        assertThatThrownBy(() -> compile(expr)).isInstanceOf(FormulaCompiler.FormulaException.class).hasMessage(message);
    }

    @Test
    @DisplayName("[SCR-01.06] 빈 수식·500자 초과 거부")
    void limits() {
        assertThatThrownBy(() -> compile(" ")).isInstanceOf(FormulaCompiler.FormulaException.class);
        assertThatThrownBy(() -> compile("1+".repeat(300) + "1")).isInstanceOf(FormulaCompiler.FormulaException.class);
    }
}
