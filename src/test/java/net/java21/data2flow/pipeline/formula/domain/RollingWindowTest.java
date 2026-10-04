package net.java21.data2flow.pipeline.formula.domain;

import net.java21.data2flow.pipeline.formula.service.FormulaEngine;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.service.ScriptRunner;
import net.java21.data2flow.pipeline.support.MutableClock;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import net.java21.data2flow.pipeline.telemetry.service.RecentValues;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-01.06 수식 실행과 rolling 창(BR-SCR-19) */
class RollingWindowTest {

    private static final RuntimeBundle.Formula ROLLING = new RuntimeBundle.Formula(1, "co2_10m", "ppm",
            "rolling_mean(co2, 10m)", null, "DEVICE", 7);
    private static final RuntimeBundle.Formula THI = new RuntimeBundle.Formula(2, "thi", null, "thi(temperature, humidity)",
            null, "MODEL", 3);
    private static final RuntimeBundle.Formula BROKEN = new RuntimeBundle.Formula(3, "bad", null, "temperature / 0", null,
            "DEVICE", 7);
    private static final RuntimeBundle.Formula PRECOMPILED = new RuntimeBundle.Formula(4, "f", "℉", null,
            "__m(\"temperature\") * 1.8 + 32", "SPACE", 11);

    private final FormulaEngine engine = new FormulaEngine(new ScriptRunner(ScriptSandboxHarness.sandbox(), null));
    private final MutableClock clock = MutableClock.atUtc("2026-10-03T00:00:00Z");

    @org.junit.jupiter.api.BeforeEach
    void warm() {
        engine.warm(new RuntimeBundle(1, List.of(), List.of(), List.of(ROLLING, THI, BROKEN, PRECOMPILED)), 3, clock.instant());
    }

    private FormulaEngine.Result eval(List<RuntimeBundle.Formula> formulas, Map<String, Double> values,
                                      Map<String, List<double[]>> window, Instant at) {
        return engine.evaluate(RuntimeBundle.EMPTY, formulas, values,
                new ScriptRunner.Execution(1, 7L, null, clock.instant(), at, window, false));
    }

    @Test
    @DisplayName("[SCR-01.06][AT-SCR-06.2] TC-SCR-016 rolling_mean(co2, 10m): 직전 10분 값만(10분 1초 전 제외) + 현재 값, 창이 비면 현재 값")
    void rollingMean() {
        RecentValues recent = new RecentValues((device, since) -> Map.of());
        Instant t0 = clock.instant();
        // 60초 간격 12건: 400, 410, … 510. 늦은 값(1000)은 창에 넣지 않는다
        for (int i = 0; i < 12; i++) {
            Instant at = t0.plus(Duration.ofSeconds(60L * i));
            if (i == 0) {
                assertThat(eval(List.of(ROLLING), Map.of("co2", 400.0), recent.window(7, Set.of("co2"), at), at).derived())
                        .singleElement().satisfies(d -> assertThat(d.value()).as("창이 비면 현재 값").isEqualTo(400.0));
            }
            recent.record(7, at, Map.of("co2", 400.0 + 10 * i), false);
        }
        recent.record(7, t0.plus(Duration.ofMinutes(5)).plusSeconds(1), Map.of("co2", 1000.0), true);
        Instant now = t0.plus(Duration.ofMinutes(12)).plusSeconds(1);
        // 창 [00:02:01, 00:12:01): 00:03 ~ 00:11 의 9건(430..510)
        FormulaEngine.Result r = eval(List.of(ROLLING), Map.of("co2", 600.0), recent.window(7, Set.of("co2"), now), now);

        double expected = (430 + 440 + 450 + 460 + 470 + 480 + 490 + 500 + 510 + 600) / 10.0;
        assertThat(r.derived()).singleElement().satisfies(d -> {
            assertThat(d.key()).isEqualTo("co2_10m");
            assertThat(d.value()).isEqualTo(expected);
            assertThat(d.unit()).isEqualTo("ppm");
        });
        Instant exact = t0.plus(Duration.ofMinutes(12));
        // 정확히 10분 전(00:02)은 포함
        assertThat(eval(List.of(ROLLING), Map.of("co2", 600.0), recent.window(7, Set.of("co2"), exact), exact).derived()
                .getFirst().value()).isEqualTo((420 + 430 + 440 + 450 + 460 + 470 + 480 + 490 + 500 + 510 + 600) / 11.0);
    }

    @Test
    @DisplayName("[SCR-01.06] 입력이 없는 수식은 건너뛰고, 계산 오류(무한대)는 그 수식만 빠진다, 미리 컴파일한 식도 실행")
    void skipsAndErrors() {
        FormulaEngine.Result r = eval(List.of(THI, BROKEN, PRECOMPILED), Map.of("temperature", 28.0, "humidity", 70.0),
                Map.of(), clock.instant());

        assertThat(r.derived()).as(String.valueOf(r.errors())).extracting(FormulaEngine.Derived::key).containsExactly("thi", "f");
        assertThat(r.derived().getFirst().value()).isEqualTo(78.4);
        assertThat(r.derived().get(1).value()).isEqualTo(82.4, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(r.errors()).singleElement().asString().startsWith("bad:");
        assertThat(eval(List.of(THI), Map.of("temperature", 28.0), Map.of(), clock.instant()).derived()).isEmpty();
        assertThat(engine.evaluate(RuntimeBundle.EMPTY, List.of(), Map.of(), ScriptRunner.Execution.test(1, clock.instant()))
                .derived()).isEmpty();
    }

    @Test
    @DisplayName("[SCR-01.06] 대상 연결: 모델 → 기기 → 공간 순서, 맞지 않는 대상은 빠진다")
    void applicable() {
        RuntimeBundle bundle = new RuntimeBundle(1, List.of(), List.of(), List.of(PRECOMPILED, ROLLING, THI));
        assertThat(FormulaEngine.applicable(bundle, 3L, 7, 11L)).extracting(RuntimeBundle.Formula::id).containsExactly(2L, 1L, 4L);
        assertThat(FormulaEngine.applicable(bundle, null, 8, null)).isEmpty();
    }
}
