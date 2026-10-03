package net.java21.data2flow.pipeline.common;

import net.java21.data2flow.pipeline.script.domain.ScriptLimits;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.time.ZoneId;

/**
 * pipeline 설정({@code data2flow.pipeline.*}). 기본값은 스펙 수치(BR-ING-07·09·10·14, BR-SCR-02, BR-DEV-08, BR-TSD-01·04)이고
 * 환경별 차이는 application-{local,staging,prod}.yml, 비밀값은 환경변수로 넣는다.
 *
 * @param flywayMode   {@code validate}(기본·prod·local) 또는 {@code migrate}(staging 배포·테스트). ADR-030
 * @param instanceId   인스턴스 이름(파드 이름). 처리 기록·스크립트 적용 보고에 남는다
 * @param developer    로컬 개발자 이름(소비자 그룹 접미사, deployment.md §8.2). 운영·staging은 비움
 * @param jobsEnabled  스케줄 작업(파티션·집계·오프라인 판정·정리) 실행 여부. 로컬은 공용 DB라 끈다
 * @param consumerEnabled {@code data2flow.raw} 소비 여부
 */
@ConfigurationProperties("data2flow.pipeline")
public record PipelineProperties(
        @DefaultValue("validate") String flywayMode,
        @DefaultValue("data2flow-pipeline-local") String instanceId,
        String developer,
        @DefaultValue("true") boolean jobsEnabled,
        @DefaultValue("true") boolean consumerEnabled,
        @DefaultValue Stream stream,
        @DefaultValue Core core,
        @DefaultValue Script script,
        @DefaultValue Ingest ingest,
        @DefaultValue Partition partition,
        @DefaultValue Aggregation aggregation,
        @DefaultValue Offline offline,
        @DefaultValue Lag lag) {

    /**
     * RabbitMQ Stream 연결. 호스트·계정·vhost는 {@code spring.rabbitmq.*}와 같은 값을 쓴다.
     *
     * @param port               Stream 포트(5552)
     * @param partitions         Super Stream이 없을 때 만들 파티션 수(운영 12, 테스트 3)
     * @param fixedAddress       브로커가 알려 주는 주소 대신 설정한 호스트·포트로만 접속(단일 노드·테스트)
     * @param publishTimeout     발행 확인(confirm) 대기 한도
     */
    public record Stream(@DefaultValue("5552") int port, @DefaultValue("12") int partitions,
                         @DefaultValue("true") boolean fixedAddress,
                         @DefaultValue("10s") Duration publishTimeout) {
    }

    /**
     * core-api 내부 API(ADR-021: 토큰 없음, {@code X-CALLER-SERVICE}).
     *
     * @param baseUrl        예: {@code http://data2flow-core-api}
     * @param connectTimeout 연결 한도
     * @param readTimeout    응답 한도
     */
    public record Core(@DefaultValue("http://data2flow-core-api") String baseUrl,
                       @DefaultValue("2s") Duration connectTimeout,
                       @DefaultValue("5s") Duration readTimeout,
                       @DefaultValue("60s") Duration cacheTtl,
                       @DefaultValue("30s") Duration negativeCacheTtl,
                       @DefaultValue("30s") Duration bundlePollInterval) {
    }

    /**
     * 스크립트 실행 제한(SCR-02.02, BR-SCR-02)과 시작 예열.
     *
     * @param warmUpRounds       예열 최소 반복 수
     * @param warmUpMaxRounds    예열 최대 반복 수
     * @param warmUpStableRounds 목표 아래가 이어져야 끝내는 반복 수
     * @param warmUpTarget       대표 스크립트 1회의 감시 구간 CPU 시간 목표(기본 10ms = 한도 50ms의 1/5)
     * @param warmUpMaxTime      예열 최대 시간(시작 지연 상한)
     */
    public record Script(@DefaultValue("50ms") Duration cpuTime,
                         @DefaultValue("1s") Duration wallTime,
                         @DefaultValue("1000000") long statementLimit,
                         @DefaultValue("65536") int maxOutputBytes,
                         @DefaultValue("1024") int maxLogBytes,
                         @DefaultValue("100") int maxLogEntries,
                         @DefaultValue("1048576") int maxStringLength,
                         @DefaultValue("1048576") int maxArrayLength,
                         @DefaultValue("65536") int maxCodeBytes,
                         @DefaultValue("32") int maxOutputDepth,
                         @DefaultValue("5") int warmUpRounds,
                         @DefaultValue("80") int warmUpMaxRounds,
                         @DefaultValue("3") int warmUpStableRounds,
                         @DefaultValue("10ms") Duration warmUpTarget,
                         @DefaultValue("30s") Duration warmUpMaxTime) {

        public ScriptLimits toLimits() {
            return new ScriptLimits(cpuTime, wallTime, statementLimit, maxOutputBytes, maxLogBytes, maxLogEntries,
                    maxStringLength, maxArrayLength, maxCodeBytes, maxOutputDepth);
        }

        public net.java21.data2flow.pipeline.script.service.ScriptSandbox.WarmUpPolicy toWarmUpPolicy() {
            return new net.java21.data2flow.pipeline.script.service.ScriptSandbox.WarmUpPolicy(warmUpRounds, warmUpMaxRounds,
                    warmUpStableRounds, warmUpTarget, warmUpMaxTime);
        }
    }

    /**
     * 수집 처리(ING).
     *
     * @param maxPayloadBytes       payload 한도(256KB, BR-ING-10)
     * @param maxMetrics            측정 항목 수 한도(100)
     * @param maxStringBytes        문자열 값 한도(1KB)
     * @param dedupWindow           중복 판정 창(10분, BR-ING-07)
     * @param futureTolerance       측정 시각 미래 허용(5분, BR-ING-04)
     * @param pastLimit             측정 시각 과거 한도(7일, BR-ING-04 조직 설정 기본값)
     * @param lateThreshold         늦은 도착 기준(1시간, ING-06.03)
     * @param autoRegisterHourlyLimit 소스당 시간당 자동 등록 한도(BR-ING-09, 소스 설정이 없을 때)
     * @param retryInitial          DB·core 일시 장애 재시도 첫 대기
     * @param retryMax              재시도 최대 대기
     */
    public record Ingest(@DefaultValue("262144") int maxPayloadBytes,
                         @DefaultValue("100") int maxMetrics,
                         @DefaultValue("1024") int maxStringBytes,
                         @DefaultValue("10m") Duration dedupWindow,
                         @DefaultValue("5m") Duration futureTolerance,
                         @DefaultValue("7d") Duration pastLimit,
                         @DefaultValue("1h") Duration lateThreshold,
                         @DefaultValue("100") int autoRegisterHourlyLimit,
                         @DefaultValue("200ms") Duration retryInitial,
                         @DefaultValue("30s") Duration retryMax,
                         @DefaultValue("5") int storeAttempts) {
    }

    /**
     * 월·일 파티션 관리(ADR-019, BR-TSD-01·02, BR-ING-14). pg_partman 없이 우리 스케줄러가 만든다.
     *
     * @param monthsAhead       월 파티션을 미리 만들 개월 수(3)
     * @param rawDaysAhead      원본 일 파티션을 미리 만들 일 수(7)
     * @param rawRetention      원본 보관(30일)
     * @param dlqRetention      DLQ 보관(14일)
     * @param lockTimeout       DETACH lock_timeout(5초)
     */
    public record Partition(@DefaultValue("3") int monthsAhead,
                            @DefaultValue("7") int rawDaysAhead,
                            @DefaultValue("30d") Duration rawRetention,
                            @DefaultValue("14d") Duration dlqRetention,
                            @DefaultValue("5s") Duration lockTimeout) {
    }

    /**
     * 1m·1h·1d 집계(TSD-02.02, BR-TSD-04·05·06).
     *
     * @param grace       분 집계 확정 전 기다림(수신 지연 흡수)
     * @param defaultZone 1d 구간의 기본 사이트 시간대(BR-TSD-05)
     * @param backfill    워터마크가 없을 때 처음 계산할 과거 범위
     */
    public record Aggregation(@DefaultValue("30s") Duration grace,
                              @DefaultValue("Asia/Seoul") ZoneId defaultZone,
                              @DefaultValue("2h") Duration backfill) {
    }

    /** 오프라인 판정 기본값(BR-DEV-08: 주기 300초, 배수 3) */
    public record Offline(@DefaultValue("300") int defaultIntervalSec,
                          @DefaultValue("3") double defaultMultiplier,
                          @DefaultValue("3") double gapFactor) {
    }

    /** 처리 지연 경보(ING-07.04, BR-ING-16) */
    public record Lag(@DefaultValue("60s") Duration warn, @DefaultValue("300s") Duration critical) {
    }
}
