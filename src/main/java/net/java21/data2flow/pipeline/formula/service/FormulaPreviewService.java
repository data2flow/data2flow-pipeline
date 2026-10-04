package net.java21.data2flow.pipeline.formula.service;

import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.pipeline.formula.domain.FormulaCompiler;
import net.java21.data2flow.pipeline.formula.dto.FormulaDtos;
import net.java21.data2flow.pipeline.metric.domain.MetricCatalog;
import net.java21.data2flow.pipeline.metric.service.MetricCatalogService;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.service.ScriptRunner;
import net.java21.data2flow.pipeline.telemetry.repository.TelemetryRepository;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * 수식 검사·미리 보기(SCR-01.06, API-SCR-20·21의 원천). 검사는 문법·측정 키(조직 측정 항목·별칭)·함수·창을 보고, 미리 보기는 기기의
 * 지난 N시간(1~24) 원본에 운영과 같은 샌드박스로 수식을 적용한다(저장·발행 없음). 창 함수는 그 시각 앞의 값만 쓴다.
 */
public class FormulaPreviewService {

    private static final int MAX_POINTS = 1_440;

    private final FormulaEngine engine;
    private final MetricCatalogService catalogs;
    private final TelemetryRepository telemetry;
    private final Clock clock;

    public FormulaPreviewService(FormulaEngine engine, MetricCatalogService catalogs, TelemetryRepository telemetry, Clock clock) {
        this.engine = engine;
        this.catalogs = catalogs;
        this.telemetry = telemetry;
        this.clock = clock;
    }

    public FormulaDtos.CompileResponse compile(FormulaDtos.CompileRequest request) {
        try {
            FormulaCompiler.Compiled c = FormulaCompiler.compile(request.expression(),
                    known(request.organizationId(), request.knownKeys()));
            Map<String, String> windows = new LinkedHashMap<>();
            c.windows().forEach((k, d) -> windows.put(k, d.toString()));
            return new FormulaDtos.CompileResponse(true, c.js(), c.inputs(), windows, null);
        } catch (FormulaCompiler.FormulaException e) {
            return new FormulaDtos.CompileResponse(false, null, List.of(), Map.of(),
                    new FormulaDtos.Error(FormulaCompiler.ERROR_CODE, e.line(), e.col(), e.getMessage()));
        }
    }

    public FormulaDtos.PreviewResponse preview(FormulaDtos.PreviewRequest request) {
        long org = request.organizationId();
        FormulaCompiler.Compiled c;
        try {
            c = FormulaCompiler.compile(request.expression(), known(org, null));
        } catch (FormulaCompiler.FormulaException e) {
            throw new BusinessException(net.java21.data2flow.pipeline.common.PipelineErrorCode.SCRIPT_FORMULA_INVALID,
                    e.getMessage());
        }
        Instant to = clock.instant();
        Instant from = to.minus(Duration.ofHours(request.hours()));
        Duration lookback = c.windows().values().stream().max(Duration::compareTo).orElse(Duration.ZERO);
        Map<Instant, Map<String, Double>> rows = new TreeMap<>();
        Map<String, List<double[]>> history = new LinkedHashMap<>();
        for (TelemetryRepository.Point p : telemetry.findPoints(org, request.deviceId(), c.inputs(), from.minus(lookback), to)) {
            history.computeIfAbsent(p.metricKey(), k -> new ArrayList<>()).add(new double[]{p.time().toEpochMilli(), p.value()});
            if (!p.time().isBefore(from)) {
                rows.computeIfAbsent(p.time(), t -> new LinkedHashMap<>()).put(p.metricKey(), p.value());
            }
        }
        Map<String, List<FormulaDtos.Point>> inputs = new LinkedHashMap<>();
        for (String key : c.inputs()) {
            List<FormulaDtos.Point> points = new ArrayList<>();
            rows.forEach((t, values) -> {
                if (values.containsKey(key)) {
                    points.add(new FormulaDtos.Point(t, values.get(key)));
                }
            });
            inputs.put(key, points);
        }
        RuntimeBundle.Formula formula = new RuntimeBundle.Formula(0, "preview", null, request.expression(), null, "DEVICE",
                request.deviceId());
        List<FormulaDtos.Point> series = new ArrayList<>();
        int n = 0;
        for (Map.Entry<Instant, Map<String, Double>> row : rows.entrySet()) {
            if (++n > MAX_POINTS) {
                break;
            }
            Instant t = row.getKey();
            Map<String, List<double[]>> window = new LinkedHashMap<>();
            c.windows().forEach((key, d) -> window.put(key, history.getOrDefault(key, List.of()).stream()
                    .filter(p -> p[0] >= t.minus(d).toEpochMilli() && p[0] < t.toEpochMilli()).toList()));
            FormulaEngine.Result r = engine.evaluate(RuntimeBundle.EMPTY, List.of(formula), row.getValue(),
                    new ScriptRunner.Execution(org, request.deviceId(), null, clock.instant(), t, window, false));
            r.derived().forEach(x -> series.add(new FormulaDtos.Point(t, x.value())));
        }
        return new FormulaDtos.PreviewResponse(series, inputs);
    }

    private Predicate<String> known(Long organizationId, List<String> knownKeys) {
        if (knownKeys != null && !knownKeys.isEmpty()) {
            return knownKeys::contains;
        }
        if (organizationId == null) {
            return k -> true;
        }
        MetricCatalog catalog = catalogs.catalog(organizationId);
        return k -> catalog.definition(catalog.canonicalKey(k)) != null;
    }
}
