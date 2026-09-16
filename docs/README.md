# Distributed Runtime 문서

이 디렉터리에는 `distributed-runtime`을 이해하고 사용하는 데 필요한 문서가 있다.
처음부터 모든 문서를 읽을 필요는 없다. 아래 읽기 순서에서 자신의 목적에 맞는 문서를
고르면 된다.

가장 중요한 구분은 다음 두 가지다.

- **지금 사용할 수 있는 기능:** Milestone01에서 만든 공통 작업 규칙
- **앞으로 만들 기능:** Cloud SQL, Pub/Sub, MIG, Terraform을 사용하는 실제 실행 환경

`first.md`는 최종 모습까지 설명한다. 지금 코드에서 바로 쓸 수 있는 기능을 알고
싶다면 이 문서와 `architecture/current-contract-layer.md`부터 읽는다.

낯선 단어가 나오면 [쉬운 용어 설명](glossary.md)을 참고한다.

## 현재 상태

현재 버전은 `distributed-runtime 0.1.0`이며 다음 범위가 구현되어 있다.

- 형식이 잘못된 값을 거부하는 ID와 상태 값
- 종류와 재시도 가능 여부를 담는 오류
- 같은 데이터를 항상 같은 bytes로 만드는 V1 메시지
- 저장된 파일의 위치와 내용이 맞는지 확인하는 참조
- DB 연결 한도를 넘지 않는지 확인하는 runtime 설정
- Finite 작업을 나누고 처리하는 Python 규칙
- Continuous 작업의 구역, 소유권, 결과 전송 규칙
- workload와 sink를 등록하고 찾는 registry
- 마감 시각, 작업 중단, 안전한 종료, 구조화 로그
- 공개 API와 메시지 형식의 실수 변경을 잡는 기준 파일
- GCP 없이 Finite/Continuous 코드를 검사하는 로컬 테스트 도구 (`testing` package)

- Cloud SQL에 상태를 저장하고 읽는 코드와 DB 변경 파일
- Pub/Sub으로 메시지를 보내고 받는 코드 (emulator 검증)
- Finite/Continuous 작업을 실제로 실행하는 worker
- DB 변경과 메시지 발행을 함께 안전하게 처리하는 outbox
- 실패한 작업의 다음 실행 시각을 관리하는 retry dispatcher
- Continuous partition 소유권을 실제 DB에서 관리하는 조정 작업
- Finite 구간 합과 Continuous shard pulse 예제 (`examples/`)
- Terraform GCP 인프라 모듈 (`terraform/`, docker로 fmt/validate 검증)

다음 항목은 아직 검증되지 않았다.

- 실제 GCP credential을 사용한 배포와 전체 경로 검증 — Terraform
  모듈은 validate까지 확인됐으며, 실제 provisioning과 E2E는
  credential이 있는 환경에서 진행한다.

## 권장 읽기 순서

### 처음 사용하는 개발자

1. [빠른 시작](getting-started.md)
2. [현재 계약 계층 아키텍처](architecture/current-contract-layer.md)
3. [Finite workload](concepts/finite-workloads.md) 또는
   [Continuous workload](concepts/continuous-workloads.md)
4. 모르는 말이 있으면 [쉬운 용어 설명](glossary.md)
5. 필요한 [레퍼런스](#레퍼런스)

### 런타임 구현자

1. [현재 계약 계층 아키텍처](architecture/current-contract-layer.md)
2. [Envelope와 artifact](reference/envelopes-and-artifacts.md)
3. [Configuration](reference/configuration.md)
4. [확장 경계](development/extension-boundaries.md)
5. [테스트와 품질 게이트](development/testing-and-quality.md)
6. 구현할 기능의 [설계 문서](#설계)

### 설계와 로드맵 검토자

1. [목표 아키텍처](first.md)
2. [구현 세부 계획](implementation-plan.md)
3. [현재 계약 계층 아키텍처](architecture/current-contract-layer.md)
4. [호환성과 버전 정책](development/compatibility-and-versioning.md)

## 문서 지도

### 입문

| 문서 | 목적 |
|---|---|
| [빠른 시작](getting-started.md) | 패키지 설치, application 생성, workload 등록 |
| [현재 계약 계층](architecture/current-contract-layer.md) | 지금 구현된 구조와 미구현 경계 |
| [쉬운 용어 설명](glossary.md) | 분산 실행과 GCP 용어를 짧게 풀이 |

### 개념

| 문서 | 목적 |
|---|---|
| [Finite workload](concepts/finite-workloads.md) | 유한 작업 계획, 실행 identity, drift |
| [Continuous workload](concepts/continuous-workloads.md) | partition, lease, fencing, sink |

### 레퍼런스

| 문서 | 목적 |
|---|---|
| [Core contracts](reference/core-contracts.md) | identifier, enum, error, lifecycle, logging |
| [Envelope와 artifact](reference/envelopes-and-artifacts.md) | 전송 메시지와 대형 payload reference |
| [Configuration](reference/configuration.md) | 정책, pool, execution class, DB capacity |
| [Registry와 application](reference/registry-and-application.md) | workload identity와 등록 규칙 |

### 개발

| 문서 | 목적 |
|---|---|
| [테스트와 품질 게이트](development/testing-and-quality.md) | 로컬/CI 검증 명령과 테스트 분류 |
| [확장 경계](development/extension-boundaries.md) | GCP/worker/control 구현 시 지켜야 할 경계 |
| [호환성과 버전 정책](development/compatibility-and-versioning.md) | snapshot, SemVer, schema evolution |

### 설계

기능의 계약을 구현과 함께(또는 구현 전에) 정리한 문서다.

| 문서 | 목적 |
|---|---|
| [로컬 테스트 킷 설계](design/local-testing-kits.md) | `testing` package의 구성 요소와 사용 방법 계약 |
| [제품 예제 설계](design/product-examples.md) | `examples/`의 workload 구성과 결과 종합 경계 |
| [상태 스키마 설계](design/state-schema.md) | PostgreSQL 테이블·불변식·트랜잭션 경계 계약 |

## 문서의 기준

- 현재 기능에 대해 코드와 문서의 설명이 다르면 코드와 테스트를 기준으로 판단한다.
- 앞으로 만들 기능은 `first.md`와 `implementation-plan.md`를 기준으로 판단한다.
- 공개 API는 각 package의 `__all__`과 호환성 기준 파일로 확인한다.
- 예제는 Python 3.12 이상을 기준으로 한다.
- “검증한다”는 말은 현재 코드가 잘못된 입력을 실제로 거부할 때만 쓴다.
- “실행한다”는 말은 실제 worker 또는 adapter가 있을 때만 쓴다.

## 저장소 기준점

이 문서는 Milestone01 최종 구현인 다음 기준을 설명한다.

- branch: `feat/milestone-01-contracts`
- package version: `0.1.0`
- Python: `>=3.12`
- 실행에 필요한 외부 Python package: 없음
- 로컬 검증 결과: 188개 테스트, branch coverage 94%
