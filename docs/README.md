# Distributed Runtime 문서

이 디렉터리는 `distributed-runtime`의 설계 목표, 현재 구현, 사용 방법, 공개 계약,
검증 기준을 설명한다.

가장 중요한 구분은 다음 두 가지다.

- **현재 구현:** Milestone01에서 완료된 domain-neutral 계약 계층
- **목표 아키텍처:** Cloud SQL, Pub/Sub, MIG, Terraform을 포함한 Phase 1~5 전체 구조

`docs/first.md`에는 최종 목표까지 포함되어 있으므로, 현재 사용할 수 있는 기능을
확인하려면 반드시 이 문서와 `architecture/current-contract-layer.md`부터 읽는다.

## 현재 상태

현재 버전은 `distributed-runtime 0.1.0`이며 다음 범위가 구현되어 있다.

- typed identifier와 상태 enum
- 구조화된 오류 분류
- canonical V1 envelope
- immutable artifact reference
- typed runtime configuration과 DB connection capacity 검증
- Finite planner/handler/context 계약
- Continuous partition/lease/sink/context 계약
- workload/sink registry와 최소 application facade
- deadline, cancellation, graceful shutdown, structured logging
- API/envelope/config compatibility snapshot

다음 항목은 아직 구현되지 않았다.

- Cloud SQL repository와 migration
- Pub/Sub publisher/subscriber
- finite/continuous worker loop
- transactional outbox와 retry dispatcher
- continuous reconciler와 실제 lease transaction
- GCP client adapter
- Terraform 및 실제 GCP E2E

## 권장 읽기 순서

### 처음 사용하는 개발자

1. [빠른 시작](getting-started.md)
2. [현재 계약 계층 아키텍처](architecture/current-contract-layer.md)
3. [Finite workload](concepts/finite-workloads.md) 또는
   [Continuous workload](concepts/continuous-workloads.md)
4. 필요한 [레퍼런스](#레퍼런스)

### 런타임 구현자

1. [현재 계약 계층 아키텍처](architecture/current-contract-layer.md)
2. [Envelope와 artifact](reference/envelopes-and-artifacts.md)
3. [Configuration](reference/configuration.md)
4. [확장 경계](development/extension-boundaries.md)
5. [테스트와 품질 게이트](development/testing-and-quality.md)

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

## 문서의 기준

- 코드가 문서와 다르면 현재 구현에 대해서는 코드와 테스트가 우선한다.
- 장래 설계는 `first.md`와 `implementation-plan.md`가 기준이다.
- public API는 각 package의 `__all__`과 compatibility snapshot으로 확인한다.
- 예제는 Python 3.12 이상을 기준으로 한다.
- 문서에서 “검증한다”는 표현은 생성자나 helper가 현재 실제로 검증하는 경우에만 쓴다.
- 문서에서 “실행한다”는 표현은 실제 worker/adapter가 존재할 때만 쓴다.

## 저장소 기준점

이 문서는 Milestone01 최종 구현인 다음 기준을 설명한다.

- branch: `feat/milestone-01-contracts`
- package version: `0.1.0`
- Python: `>=3.12`
- runtime dependencies: 없음
- local verification: 164 tests, 95% branch coverage
