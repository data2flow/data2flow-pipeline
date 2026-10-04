package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.formula.service.FormulaEngine;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import net.java21.data2flow.pipeline.script.domain.ScriptKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 새 번들 예열(SCR-03.04, EVT-SCR-01 처리: "다시 읽어 컴파일·예열한 뒤 실행 계획을 원자 교체"). 바뀐 스크립트 버전과 바뀐 수식 묶음만
 * 시험 입력으로 몇 번 실행해 둔다(지표·오류에는 넣지 않음). 해석 실행에서는 처음 보는 코드의 첫 실행이 느려 운영 첫 메시지가 50ms를
 * 넘을 수 있기 때문이다. 예열 실패는 무시한다(실제 오류는 운영 실행이 기록).
 */
public class ScriptWarmer {

    private static final Logger log = LoggerFactory.getLogger(ScriptWarmer.class);
    private static final String SAMPLE_MESSAGE = "{\"v\":1,\"deviceId\":0,\"measuredAt\":\"2026-01-01T00:00:00Z\","
            + "\"metrics\":[{\"key\":\"temperature\",\"value\":22.0,\"quality\":0},{\"key\":\"humidity\",\"value\":40,\"quality\":0}],"
            + "\"meta\":{}}";
    private static final String SAMPLE_DECODE = "{\"topic\":\"warm\",\"payload\":{\"temperature\":22.0},"
            + "\"payloadEncoding\":\"JSON\",\"receivedAt\":\"2026-01-01T00:00:00Z\",\"source\":{\"code\":\"warm\",\"config\":{}}}";

    private final ScriptRunner runner;
    private final FormulaEngine formulas;
    private final int rounds;
    private final Clock clock;
    private final JsonMapper mapper = MessageCodec.newMapper();

    public ScriptWarmer(ScriptRunner runner, FormulaEngine formulas, int rounds, Clock clock) {
        this.runner = runner;
        this.formulas = formulas;
        this.rounds = rounds;
        this.clock = clock;
    }

    /** 처음 읽는 번들을 데울 때의 시간 상한(그 조직의 첫 메시지가 너무 오래 기다리지 않게) */
    private static final long FIRST_LOAD_BUDGET_NANOS = 5_000_000_000L;

    /** {@code old}와 비교해 바뀐 것만. 처음 읽는 번들({@code old == null})은 5초 안에서 모두 */
    public void warm(RuntimeBundle old, RuntimeBundle fresh) {
        long started = System.nanoTime();
        RuntimeBundle previous = old == null ? RuntimeBundle.EMPTY : old;
        Set<Long> known = new HashSet<>();
        previous.scripts().forEach(s -> known.add(s.versionId()));
        int warmed = 0;
        for (RuntimeBundle.Script s : fresh.scripts()) {
            if (!s.enabled() || known.contains(s.versionId()) && Objects.equals(previous.modules(), fresh.modules())) {
                continue;
            }
            if (old == null && System.nanoTime() - started > FIRST_LOAD_BUDGET_NANOS) {
                break;
            }
            for (int i = 0; i < rounds; i++) {
                try {
                    ObjectNode ctx = mapper.createObjectNode();
                    ctx.set("config", s.config());
                    runner.run(fresh, s, s.kind(), s.kind() == ScriptKind.DECODE ? SAMPLE_DECODE : SAMPLE_MESSAGE, ctx,
                            ScriptRunner.Execution.test(0, clock.instant()));
                } catch (RuntimeException e) {
                    log.debug("예열 실패(script={}): {}", s.scriptId(), e.getMessage());
                }
            }
            warmed++;
        }
        if (!Objects.equals(previous.formulas(), fresh.formulas())) {
            List<RuntimeBundle.Formula> changedKeys = fresh.formulas().stream().filter(f -> !previous.formulas().contains(f)).toList();
            // 바뀐 수식이 든 대상 묶음 전체(같은 프로그램)
            Set<String> targets = new HashSet<>();
            changedKeys.forEach(f -> targets.add(f.targetType() + ":" + f.targetId()));
            List<RuntimeBundle.Formula> changed = fresh.formulas().stream()
                    .filter(f -> targets.contains(f.targetType() + ":" + f.targetId())).toList();
            formulas.warm(new RuntimeBundle(fresh.bundleVersion(), List.of(), List.of(), changed), rounds, clock.instant());
        }
        if (warmed > 0) {
            log.info("스크립트 {}개 예열 {}ms", warmed, (System.nanoTime() - started) / 1_000_000);
        }
    }
}
