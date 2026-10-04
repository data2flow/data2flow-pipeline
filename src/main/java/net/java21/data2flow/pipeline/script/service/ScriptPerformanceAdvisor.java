package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.repository.ScriptOpsRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 성능·오류 경고(SCR-05.03, SCR-03.05, BR-SCR-11). 조회 구간의 마지막 버전 지표로 판단한다.
 *
 * <ul>
 *   <li>{@code SLOW}: p95 실행 시간이 20ms를 넘으면. 원인 후보는 입력이 32KB를 넘었으면 {@code LARGE_INPUT}, 코드에 반복문이 있으면
 *       {@code LARGE_LOOP}(샌드박스가 실행 문장 수를 알려 주지 않으므로 코드로 짐작), 둘 다 아니면 {@code CPU}.
 *       p95가 20ms 이하로 내려오면 경고가 없다(배지가 사라짐).</li>
 *   <li>{@code ERROR_RATE}: 오류율이 10% 이상이면(BR-SCR-11의 WARNING 기준).</li>
 * </ul>
 */
public final class ScriptPerformanceAdvisor {

    public static final double SLOW_P95_MS = 20.0;
    public static final int LARGE_INPUT_BYTES = 32 * 1024;
    public static final double ERROR_RATE_WARN = 0.10;
    private static final Pattern LOOP = Pattern.compile("\\b(for|while|do)\\b\\s*[({]|\\.(forEach|map|reduce|filter)\\s*\\(");

    private ScriptPerformanceAdvisor() {
    }

    public static List<Warning> warnings(List<ScriptOpsRepository.StatRow> points, String code) {
        List<Warning> out = new ArrayList<>();
        if (points.isEmpty()) {
            return out;
        }
        long latestVersion = points.stream().max((a, b) -> a.minute().compareTo(b.minute())).get().versionId();
        List<ScriptOpsRepository.StatRow> rows = points.stream().filter(p -> p.versionId() == latestVersion).toList();
        double p95 = rows.stream().mapToDouble(ScriptOpsRepository.StatRow::p95Ms).max().orElse(0);
        int maxInput = rows.stream().mapToInt(ScriptOpsRepository.StatRow::maxInputBytes).max().orElse(0);
        long processed = rows.stream().mapToLong(ScriptOpsRepository.StatRow::processed).sum();
        long errors = rows.stream().mapToLong(ScriptOpsRepository.StatRow::errors).sum();
        if (p95 > SLOW_P95_MS) {
            List<String> hints = new ArrayList<>();
            if (maxInput > LARGE_INPUT_BYTES) {
                hints.add("LARGE_INPUT");
            }
            if (code != null && LOOP.matcher(code).find()) {
                hints.add("LARGE_LOOP");
            }
            if (hints.isEmpty()) {
                hints.add("CPU");
            }
            out.add(new Warning("SLOW", Math.round(p95 * 10) / 10.0, hints));
        }
        if (processed > 0 && (double) errors / processed >= ERROR_RATE_WARN) {
            out.add(new Warning("ERROR_RATE", Math.round(errors * 1000.0 / processed) / 1000.0, List.of()));
        }
        return out;
    }

    /** 경고 {@code {type: ERROR_RATE|SLOW, value, hints[]}}(API-SCR-12) */
    public record Warning(String type, double value, List<String> hints) {
    }
}
