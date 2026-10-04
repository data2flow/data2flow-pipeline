# data2flow-pipeline

수집 경로(M2)와 데이터 관리(M5)의 처리 서비스입니다. `data2flow.raw`를 읽어 디코딩, DECODE·TRANSFORM 스크립트(GraalJS 커뮤니티판 샌드박스, 공용 모듈 `data2flow-script-sandbox`·ADR-046)와 공유 모듈·수식 파생 항목, 기기 식별·자동 등록(PENDING), 검증·품질(의심 값)·중복 제거, 시계열 저장(월 파티션), 1m/1h/1d 집계(1d는 기기 사이트 시간대), 오프라인 판정·시계 오차 감지를 하고 `data2flow.telemetry`와 도메인 이벤트를 냅니다. 기간 재처리 작업, 일일 품질 점수, 보관 정리·정렬 재작성·Parquet 콜드 보관도 이 서비스가 합니다.

- 관련 스펙: ING, SCR, TSD, DEV-02.05·02.08·04.03·05.01, NFR-02 (정본은 비공개 저장소 `data2flow-docs`)
- 패키지: `net.java21.data2flow.pipeline` · Spring Boot 4.1.1 · Java 21 · Maven Wrapper
- 포트: API 8080(내부 API만, 외부 경로 없음), actuator 8081(프로브·지표 전용)
- 스키마: `data2flow_pipeline`(21개 표, Flyway `V202610041000__pipeline_init.sql` + M5 확장 `V202610050000__pipeline_m5.sql`). staging 배포만 migrate, prod·local은 validate(ADR-030)

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
| 냄 | Super Stream `data2flow.telemetry`(라우팅 키 deviceId, 기간 재처리 결과는 다시 내지 않음), topic `data2flow.events`: `device.connectivity.changed`, `ingest.alert.raised/cleared`, `ingest.gap.detected`, `ingest.clock-skew.suspected`, `ingest.reprocess.finished`, `aggregates.recomputed`, `retention.purged`, `partition.warning`, `device.state.reported`(EVT-ACT-07 LoRaWAN 업링크 신호: 승인된 실제 기기의 ChirpStack `event/up`마다 빈 `capabilities`·version=fCnt, action이 Class A 대기 다운링크를 보냄). 읽을 수 없는 원본은 `data2flow.dlx` → `pipeline.raw.dlq` |
| 부름(core) | API-ING-21, API-DEV-120·121·122·123·124·125·130, API-SCR-32·34, API-TSD-60(보관 정책, 없으면 기본값)·61(콜드 보관 파일 등록) |
| 받음 | API-SCR-30 `POST /internal/pipeline/scripts/check`, API-SCR-31 `…/scripts/test-run`, API-SCR-35 `…/scripts/test-cases/run`, API-SCR-36 `GET …/scripts/{script-id}/stats`, API-SCR-37 `…/formula-metrics/compile`, API-SCR-38 `…/formula-metrics/preview`, API-ING-22 `…/reprocess-items`, API-ING-23 `…/reprocess-jobs`(·`/{job-id}/cancel`), API-ING-24 `…/dlq-items/discard`, API-TSD-51 `…/telemetry/remap-metric`, API-TSD-53 `…/retention/apply-policy`, API-TSD-62 `…/retention/preview` |

## 플랫폼 브로커 기기 서명 (DSC-03.03·03.05, ADR-042)

ingress가 `RawEnvelope.signatureStatus`(VERIFIED·UNSIGNED·INVALID)를 채우고, pipeline은 기기 상태와 함께 판정합니다.

| 경우 | 결과 |
|---|---|
| `INVALID`(서명 키가 있는데 서명 없음·불일치) | 디코딩 없이 원본만 `INVALID` + `DEVICE_SIGNATURE_INVALID`, 자동 등록·저장 0, 지표 `data2flow_ingest_signature_rejected_total` |
| 승인 대기(PENDING) 기기 | 서명과 무관하게 저장하되 quality 2(격리) |
| 승인된 기기 + `VERIFIED` | 정상 처리(quality 0 등) |
| 승인된 기기 + `UNSIGNED`·없음 | `INVALID` + `DEVICE_SIGNATURE_INVALID`(ingress 키 캐시가 늦어도 승인 뒤 서명 없는 메시지를 막음) |
| 재처리(API-ING-22·23) | 서명 거부 원본은 그대로 둔다. 원본에 보관한 서명 판정(`raw_messages.signature_status`, M5)이 VERIFIED면 quality 0을 유지하고, 판정이 없는 M5 이전 원본은 quality 2(미검증)로 다시 저장(ADR-050) |

## 데이터 관리 (M5, ADR-050)

| 기능 | 동작 |
|---|---|
| 기간 재처리(ING-01.04) | 만들 때 스크립트 번들을 `pinned_bundle`에 고정, 초당 500건(실시간 지연 1분 초과면 100건), 맡은 인스턴스가 멈추면(2분) 다른 인스턴스가 넘겨받아 이어서 처리, 끝나면 `ingest.reprocess.finished` |
| 품질(ING-04.01·06.01·06.04) | 같은 값 12회·급변(유효 범위 폭 18.75%/분)은 quality 3, 기기별 일일 점수를 사이트 시간대 00:30에 확정(30분마다 작업), 시계 오차 평균 5분 초과가 30분 이어지면 이벤트 |
| 스크립트(SCR-01.06·04.01·04.02·03.05·05.0x) | `import … from 'module:이름@버전'`·`ctx.modules`·`ctx.window`, 수식 파생 항목(TRANSFORM 마지막), 설정값 즉시 반영(`configRevision`), 지표·오류 스냅샷(100건)·로그 수집(초당 10건)을 5초마다 기록, 번들이 바뀌면 예열 뒤 교체 |
| 보관(TSD-02.01·02.03·05.01·05.02·05.03) | 매일 02:00 UTC(staging은 끔): 조직·측정 항목·모델별 기간으로 1만 행씩 삭제, 더 긴 항목은 `telemetry_long`, 모든 조직 기간이 지난 월 파티션은 DETACH·DROP, 7일 지난 원본 파티션 하나 정렬 재작성, 콜드 보관을 켠 조직은 Parquet(zstd)으로 올리고 체크섬·core 등록 뒤 삭제. 상태형 `storeMode=ON_CHANGE`는 바뀔 때와 1시간 하트비트만 저장 |

콜드 보관 저장소는 `DATA2FLOW_ARCHIVE_ENDPOINT`·`DATA2FLOW_ARCHIVE_BUCKET`(prod `data2flow-prod`, staging `data2flow-stg`)·`DATA2FLOW_ARCHIVE_ACCESS_KEY`·`DATA2FLOW_ARCHIVE_SECRET_KEY`(k8s Secret)로 정합니다. 주소가 비면 콜드 보관 없이 보관 기간대로 지웁니다. 시험은 SeaweedFS 컨테이너(`chrislusf/seaweedfs`, Apache-2.0)로 합니다.

## 작업 규칙

스펙 ID에서 시작하고(인수 테스트 → 테스트 케이스 → 구현), 브랜치·PR·테스트 이름에 스펙 ID를 남깁니다. 1.0 전에는 `main` + `feat/<스펙ID>-<요약>`, 1.0 뒤에는 버전 브랜치 `feature/vX.Y`를 씁니다(ADR-039).
