# data2flow-pipeline

수집 경로(M2)의 처리 서비스입니다. `data2flow.raw`를 읽어 디코딩, DECODE·TRANSFORM 스크립트(GraalJS 커뮤니티판 샌드박스), 기기 식별·자동 등록(PENDING), 검증·중복 제거, 시계열 저장(월 파티션), 1m/1h/1d 집계, 오프라인 판정을 하고 `data2flow.telemetry`와 도메인 이벤트를 냅니다.

- 관련 스펙: ING, SCR, TSD, DEV-02.05·02.08·04.03·05.01, NFR-02 (정본은 비공개 저장소 `data2flow-docs`)
- 패키지: `net.java21.data2flow.pipeline` · Spring Boot 4.1.1 · Java 21 · Maven Wrapper
- 포트: API 8080(내부 API만, 외부 경로 없음), actuator 8081(프로브·지표 전용)
- 스키마: `data2flow_pipeline`(15개 표, Flyway `V202610041000__pipeline_init.sql`). staging 배포만 migrate, prod·local은 validate(ADR-030)

## 빌드와 실행

```bash
./mvnw verify                          # 단위·통합(Testcontainers PostgreSQL 18·RabbitMQ 3.13 Stream) + 커버리지 80%
./mvnw verify -Dnfr.db-outage=PT30S    # DB 중단 시험(NFR-02.02)을 짧게(기본 5분)
./mvnw spring-boot:run                 # 로컬 실행(프로필 local: 공유 스케줄 작업 끔, 그룹 pipeline-<DATA2FLOW_DEV_NAME>)
```

공통 라이브러리 `data2flow-contracts`는 GitHub Packages에 있어서 읽기에도 토큰이 필요합니다. `~/.m2/settings.xml`에 서버 `github`(사용자 이름 + `read:packages` 권한 토큰)를 넣거나, `data2flow-contracts`를 받아 `./mvnw install`로 로컬 저장소에 설치합니다.

## 메시지와 내부 API

| 구분 | 이름 |
|---|---|
| 읽음 | Super Stream `data2flow.raw`(그룹 `pipeline`, SAC, DB 커밋·발행 확인 뒤 오프셋 저장), fanout `data2flow.config`(`ConfigChangedMessage`, 인스턴스별 임시 큐) |
| 냄 | Super Stream `data2flow.telemetry`(라우팅 키 deviceId), topic `data2flow.events`: `device.connectivity.changed`, `ingest.alert.raised/cleared`, `ingest.gap.detected`, `aggregates.recomputed`, `partition.warning`. 읽을 수 없는 원본은 `data2flow.dlx` → `pipeline.raw.dlq` |
| 부름(core) | API-ING-21, API-DEV-120·121·122·123·124·125·130, API-SCR-32·34 |
| 받음 | API-SCR-30 `POST /internal/pipeline/scripts/check`, API-SCR-31 `…/scripts/test-run`, API-ING-22 `…/reprocess-items`, API-ING-23 `…/reprocess-jobs`(·`/{job-id}/cancel`), API-ING-24 `…/dlq-items/discard`, API-TSD-51 `…/telemetry/remap-metric` |

## 플랫폼 브로커 기기 서명 (DSC-03.03·03.05, ADR-042)

ingress가 `RawEnvelope.signatureStatus`(VERIFIED·UNSIGNED·INVALID)를 채우고, pipeline은 기기 상태와 함께 판정합니다.

| 경우 | 결과 |
|---|---|
| `INVALID`(서명 키가 있는데 서명 없음·불일치) | 디코딩 없이 원본만 `INVALID` + `DEVICE_SIGNATURE_INVALID`, 자동 등록·저장 0, 지표 `data2flow_ingest_signature_rejected_total` |
| 승인 대기(PENDING) 기기 | 서명과 무관하게 저장하되 quality 2(격리) |
| 승인된 기기 + `VERIFIED` | 정상 처리(quality 0 등) |
| 승인된 기기 + `UNSIGNED`·없음 | `INVALID` + `DEVICE_SIGNATURE_INVALID`(ingress 키 캐시가 늦어도 승인 뒤 서명 없는 메시지를 막음) |
| 재처리(API-ING-22·23) | 서명 거부 원본은 그대로 둔다. 그 밖의 플랫폼 브로커 원본은 서명 결과를 보관하지 않으므로 quality 2(미검증)로 다시 저장 |

## 작업 규칙

스펙 ID에서 시작하고(인수 테스트 → 테스트 케이스 → 구현), 브랜치·PR·테스트 이름에 스펙 ID를 남깁니다. 1.0 전에는 `main` + `feat/<스펙ID>-<요약>`, 1.0 뒤에는 버전 브랜치 `feature/vX.Y`를 씁니다(ADR-039).
