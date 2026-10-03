# data2flow-pipeline

디코딩, DECODE·TRANSFORM 스크립트(GraalJS), 기기 식별·자동 등록, 검증, 저장, 집계·파티션 관리.

- 관련 스펙: ING, SCR, TSD (정본은 비공개 저장소 `data2flow-docs`)
- 패키지: `net.java21.data2flow.pipeline` · Spring Boot 4.1.1 · Java 21 · Maven Wrapper
- 포트: API 8080, actuator 8081(프로브·지표 전용)

## 빌드와 실행

```bash
./mvnw verify                 # 단위·통합 테스트 + 커버리지 80% 검사
./mvnw spring-boot:run        # 로컬 실행(프로필 local)
```

공통 라이브러리 `data2flow-contracts`는 GitHub Packages에 있어서 읽기에도 토큰이 필요합니다. `~/.m2/settings.xml`에 서버 `github`(사용자 이름 + `read:packages` 권한 토큰)를 넣거나, `data2flow-contracts`를 받아 `./mvnw install`로 로컬 저장소에 설치합니다.

## 작업 규칙

스펙 ID에서 시작하고(인수 테스트 → 테스트 케이스 → 구현), 브랜치·PR·테스트 이름에 스펙 ID를 남깁니다. 1.0 전에는 `main` + `feat/<스펙ID>-<요약>`, 1.0 뒤에는 버전 브랜치 `feature/vX.Y`를 씁니다(ADR-039).
