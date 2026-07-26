# GCP Distributed Workload Runtime 구현 세부 계획

> 이 문서는 Phase 1~5의 **구현 로드맵**이다.
> 현재 Milestone01 구현의 사용법과 정확한 공개 계약은
> [`docs/README.md`](README.md)에서 시작한다.
> 낯선 용어는 [`쉬운 용어 설명`](glossary.md)을 참고한다.

## 먼저 읽는 요약

최종 목표는 다음과 같다.

1. 공통 Python 계약과 로컬 테스트 도구를 만든다.
2. Cloud SQL에 실행 상태와 partition 소유권을 저장한다.
3. Pub/Sub과 MIG를 연결해 Finite 작업을 실제로 실행한다.
4. lease와 fencing을 사용해 Continuous 작업을 실제로 실행한다.
5. 제품용 예제를 만들고 실제 GCP에 배포한다.
6. 장애, 버전 변경, 보안, 정리 작업까지 검증한다.

완료라고 판단하려면 Finite 예제와 Continuous 예제를 실제 GCP에서 처음부터 끝까지
실행해야 한다. 단순히 unit test가 통과하거나 Terraform 파일이 만들어진 것만으로는
완료가 아니다.

## 1. 목표와 범위

`docs/first.md`의 Phase 1~5를 모두 구현한다.

첫 릴리스는 한 사람이 관리하는 GCP 프로젝트 하나와 region 하나에서 운영한다.
외부에 공개하지 않는 내부 runtime으로 시작한다. 특정 제품에 묶이지 않은 Finite와
Continuous 예제를 실제 GCP에 배포해 전체 경로가 동작하는지 확인한다.

첫 릴리스에서 만들지 않는 것:

- 멀티클라우드와 Kubernetes 공통 계층
- 범용 DAG engine과 임의 코드 원격 실행
- 외부 부수 효과까지 포함한 exactly-once 보장
- 특정 제품에만 쓰이는 workload
- 여러 project, region, team을 나누는 기능
- public PyPI 배포

## 2. 핵심 결정

| 영역 | 결정 |
|---|---|
| Python | Python 3.12 이상, `distributed-runtime` package 하나 |
| 상태 저장 | Cloud SQL PostgreSQL을 최종 기준으로 사용 |
| Finite | Pub/Sub 메시지를 revision별 MIG worker가 처리 |
| Continuous | 최소 2개 MIG worker와 DB 시각 기반 lease/fencing |
| Control API | 일반 사용자용과 관리자용 Cloud Run service 분리 |
| 상태 조정 | Cloud Scheduler가 단일 task Cloud Run Job 실행 |
| 재시도 | Cloud SQL이 다음 실행 시각 관리, Pub/Sub DLQ는 인프라 안전망 |
| 결과 전송 | stable ID와 fencing token을 확인하는 outbox |
| 새 version 배포 | revision별 subscription과 worker capability 확인 |
| Infrastructure | Terraform, private Artifact Registry, 전용 service account |

```text
Private consumer → Client API → Cloud SQL
                              ├→ revision-filtered planner topic → planner MIG
                              └→ revision-filtered execution topic → finite MIG

Admin API → deployment/revision/drain/sink-resume
Scheduler → reconciler Job → Cloud SQL leases → continuous MIG
Emission outbox → dispatcher → external sink
```

## 3. 반드시 지켜야 할 정확성 규칙

### 재시도해도 바뀌지 않는 계획

1. 요청을 받을 때 입력과 planner/handler revision을 DB에 고정한다.
2. planner를 다시 실행해도 unit 순서, key, payload hash가 같아야 한다.
3. 이전 결과와 다르면 `PlanningDriftError`로 중단한다.
4. 전체 계획을 DB에 저장하기 전에는 unit 메시지를 보내지 않는다.

### Finite

1. outbox 메시지는 중복 발행될 수 있다고 가정한다.
2. 실행 중인 unit에는 owner, generation, 만료 시각이 있다.
3. 최신 generation을 가진 worker만 결과와 종료 상태를 저장할 수 있다.
4. 결과를 DB에 저장한 뒤에만 Pub/Sub 메시지에 ack한다.
5. 외부 API 호출의 중복 방지는 제품 handler가 맡는다.

### Continuous

1. lease 만료는 DB 시각으로 계산하고 fencing token은 계속 증가시킨다.
2. 연장, 반납, 진행 위치, 결과 저장에는 owner와 token을 확인한다.
3. 결과 중복 방지 ID는 fencing token이 바뀌어도 같아야 한다.
4. 다음 worker가 같은 결과를 다시 만들어도 같은 outbox 행으로 모인다.
5. 외부로 바로 보내는 custom sink는 runtime이 완전히 보호할 수 없다.

### Revision/migration

1. 요청을 받은 뒤 planner와 handler revision을 바꾸지 않는다.
2. 새 active revision은 이후에 들어온 요청에만 적용한다.
3. Finite 메시지는 revision 하나의 subscription에만 전달한다.
4. Continuous partition은 필요한 기능을 지원하는 worker에만 배정한다.
5. DB migration은 새 구조 추가 → 기존 데이터 채우기 → 지원 worker 확인 → 옛 구조
   제거 순서로 진행한다.
6. 옛 구조를 지우기 전에 이전 메시지, 실행 권한, lease, 구버전 worker가 없어야 한다.

## 4. 운영 기본값

### Finite

- 실행 시도 timeout은 기본 300초, workload가 요청할 수 있는 최대값은 3,600초다.
- 처음 실행을 포함해 최대 5번 시도한다.
- 재시도 대기는 5초부터 늘리되 최대 300초다. 여러 worker가 동시에 다시 시작하지
  않도록 실제 대기 시간은 범위 안에서 무작위로 정한다.
- Pub/Sub 인프라 DLQ 기준은 delivery attempt 20회로 시작한다.
- worker의 실행 권한은 timeout보다 120초 더 오래 유지한다.

### Continuous

- worker는 15초마다 heartbeat를 보낸다.
- lease는 60초 동안 유효하며 만료 30초 전부터 연장한다.
- 상태 조정 작업은 1분마다 실행한다.
- 종료할 때 진행 중인 일을 정리할 시간은 120초다.
- 응답 없는 worker의 partition은 150초 뒤 다른 worker가 인수할 수 있다.
- MIG worker는 최소 2개를 유지한다.

### Rebalance

- 현재 활성 상태이고 최근 heartbeat가 있으며 필요한 기능을 지원하는 worker만
  후보로 사용한다. 종료 중인 worker는 제외한다.
- 현재 맡은 partition의 전체 무게가 가장 작은 worker에 먼저 배정한다.
- worker 사이의 무게 차이가 2보다 작으면 기존 owner를 유지한다.
- 조정 작업 한 번에 최대 10개 또는 전체 partition의 5%만 이동한다.
- 한 번 이동한 partition은 10분 동안 다시 이동하지 않는다.
- 안정된 상태에서는 worker 사이의 전체 무게 차이를 1 이하로 맞춘다.

### Sink 장애

- 재시도 대기는 5초부터 늘리되 최대 300초다.
- 100번 실패하거나 24시간이 지나면 `FAILED` 상태로 바꾼다.
- 대기 중인 event가 10,000개 또는 256 MiB에 도달하면 새 처리를 제한한다.
- 제한 상태가 60초 넘게 계속되면 partition을 `BLOCKED_SINK`로 표시한다.
- 운영자가 `:resume-sink`를 호출하면 같은 event ID를 유지한 채 다시 전송한다.

## 5. Repository 목표

```text
src/distributed_runtime/{core,finite,continuous,gcp,worker,control,testing}/
migrations/
deploy/{terraform,images}/
examples/{finite_example,continuous_example}/
tests/{unit,contract,integration,e2e}/
docs/runbooks/
```

## 6. 사람이 리뷰하기 좋은 커밋 계획

코드 커밋은 원칙적으로 추가와 삭제를 합쳐 300~500줄로 만든다. 커밋 하나에는 한 가지
변경 의도와 그 변경을 확인하는 테스트를 함께 넣는다.

자동 생성 파일, lock file, 문서만 바꾼 커밋은 줄 수를 맞추려고 의미 없는 내용을
추가하지 않는다. 범위를 벗어난 이유는 커밋 본문에 남긴다.

Gate 약어:

| 약어 | 뜻 |
|---|---|
| `UT` | 작은 클래스나 함수 하나를 확인하는 unit test |
| `CT` | 공개 API와 메시지 형식을 확인하는 contract test |
| `IT` | 여러 component를 함께 실행하는 integration test |
| `FT` | 장애·재시도·복구 흐름을 확인하는 기능 테스트 |
| `TF` | Terraform plan과 배포 설정 검사 |
| `E2E` | 실제 GCP에서 요청부터 결과까지 확인하는 전체 경로 테스트 |
| `RELEASE-GATE` | 배포 직전 보안·호환성·package 최종 검사 |

`build`, `Ruff`, `mypy`는 각각 package build, lint, type check를 뜻한다. 아직 만들지
않은 Gate의 정확한 명령과 테스트 파일은 해당 기능을 구현하는 커밋에서 함께 추가한다.

| # | 변경 의도 | Gate |
|---:|---|---|
| 01 | package/quality/CI scaffold | build/Ruff/mypy |
| 02 | identifiers/enums/errors | UT-CORE |
| 03 | envelopes/artifact refs | CT-ENV |
| 04 | config/capacity validation | UT-CONFIG |
| 05 | finite contracts | UT-FIN-01 |
| 06 | continuous contracts | UT-CON-01 |
| 07 | registry/facade | CT-API |
| 08 | compatibility baseline | CT-COMPAT |
| 09 | lifecycle/clocks/log context | UT-LIFE |
| 10 | finite local kit | IT-LOCAL-FIN |
| 11 | continuous local kit | IT-LOCAL-CON |
| 12 | SQL engine/PG matrix | IT-SQL-BOOT |
| 13 | migration runner/base | FT-MIGRATE-BASE |
| 14 | run/planning schema | IT-PLAN-SQL |
| 15 | 계획을 DB에 저장하는 repository | IT-PLAN-REPO |
| 16 | unit/attempt schema | IT-SQL-FIN |
| 17 | finite claim | IT-CLAIM |
| 18 | at-least-once outbox | IT-OUTBOX-01 |
| 19 | continuous schema | IT-SQL-CON |
| 20 | lease/fencing repo | IT-LEASE |
| 21 | emission/checkpoint outbox | IT-EMIT |
| 22 | capabilities/heartbeat | IT-HEART |
| 23 | Pub/Sub adapter | IT-PS-01 |
| 24 | revision-pinned planner | IT-PLANNER |
| 25 | retry/timeout/rate limit | IT-RETRY-01 |
| 26 | outbox/retry dispatcher | FT-OUTBOX |
| 27 | finite worker | IT-WORKER-FIN |
| 28 | finite crash/stale claim | FT-FIN-RETRY |
| 29 | DLQ/artifacts | FT-FIN-DLQ |
| 30 | client submit/status API | CT-HTTP-FIN |
| 31 | Terraform APIs/registry | TF-BASE |
| 32 | network/Cloud SQL | TF-STATE |
| 33 | IAM/secrets/audit | TF-IAM |
| 34 | active revision/filtered Pub/Sub | TF-PS |
| 35 | planner/finite MIG/autoscaling | TF-MIG-FIN |
| 36 | APIs/dispatchers Cloud Run | TF-CONTROL |
| 37 | PITR/migration gate | FT-MIGRATE |
| 38 | IAM negative/connection budget | IAM-CAP-GATE |
| 39 | partition discovery | IT-DISC |
| 40 | reconciler/rebalance | IT-REC |
| 41 | partition supervisor | FT-CON-OWN |
| 42 | emission failure/replay | FT-EMIT |
| 43 | revision/operator drain | FT-CON-DRAIN |
| 44 | forced deletion recovery | FT-CON-CRASH |
| 45 | reconciler Job/Scheduler | TF-REC |
| 46 | continuous MIG/CPU scale | TF-MIG-CON |
| 47 | admin/revision/sink-resume API | CT-HTTP-CON |
| 48 | finite example | CT-EX-FIN |
| 49 | continuous example | CT-EX-CON |
| 50 | GCP E2E harness/teardown | E2E-BOOT |
| 51 | finite GCP E2E | E2E-FIN |
| 52 | finite autoscaling E2E | E2E-SCALE |
| 53 | continuous failover/rebalance/scale E2E | E2E-CON |
| 54 | structured logs/Ops Agent | E2E-LOG |
| 55 | metrics | E2E-METRIC |
| 56 | dashboards/alerts | E2E-OBS |
| 57 | N-1 compatibility | CT-COMPAT |
| 58 | mixed-fleet migration | FT-MIGRATE |
| 59 | canary/rollback | FT-UPGRADE |
| 60 | SQL failover/exhaustion/storage | FT-SQL-FAIL |
| 61 | custom metric calibration | E2E-CUSTOM |
| 62 | artifact cleanup | FT-CLEAN |
| 63 | security/release audit | RELEASE-GATE |
| 64 | runbooks | docs-only exception |

### 각 단계의 완료 기준

1. Contracts: unit/contract/import matrix
2. Local kits: deterministic queue/clock/claim/lease/emission
3. PostgreSQL: migrations/state invariants
4. Finite: emulator + crash/outbox/claim
5. Infrastructure: Terraform/IAM/connection/migration preflight
6. Continuous: rebalance/emission/drain/crash
7. Actual GCP: finite/continuous/autoscaling/observability/teardown
8. Hardening: N/N-1/migration/failover/cleanup/release

각 커밋의 선행 관계는 실행 계획에 따로 기록한다. runtime, infrastructure, IAM 검사를
통과하기 전에는 비용이 발생하는 실제 GCP E2E를 시작하지 않는다.

## 7. 실제 GCP에서 확인할 완료 조건

### Finite

- unit 20개 중 19개 성공, 의도적으로 실패시킨 1개는 runtime DLQ로 이동
- 재시도, rate limit, timeout, worker crash, 중복 메시지, 영구 실패 상황 주입
- 같은 메시지를 중복 발행해도 논리 실행과 부수 효과는 하나
- MIG가 15분 안에 0개에서 필요한 수로 증가하고 30분 안에 다시 0개
- revision 변경 전에 제출한 run은 이전 revision으로 끝까지 처리

### Continuous

- partition 6개를 worker 최소 2개가 처리
- owner를 종료한 뒤 150초 안에 더 큰 token을 가진 worker가 인수
- 이전 token의 쓰기 거부
- 제한된 수만 이동해 재배치한 뒤 worker 간 무게 차이가 1 이하
- CPU 부하를 주면 15분 안에 worker가 2개에서 3개 이상으로 증가
- 부하를 제거하면 30분 안에 최소값 2개로 복귀

### Security/operations

- client caller는 admin API 호출 불가
- service account key/Editor/Owner 없음
- worst-case SQL connections <= max의 70%
- migration recovery evidence
- teardown 뒤 unexpected transient resource 없음

## 8. 기본 alert 기준

| Alert | Fire condition | Fire deadline |
|---|---|---|
| oldest unacked | >600초 5분 | 10분 |
| retry lag | >120초 5분 | 10분 |
| heartbeat age | >45초 2분 | 5분 |
| unassigned | 3 ticks | 5분 |
| reconciler missing | no success 3분 | 5분 |
| blocked sink | >2분 | 5분 |
| custom metric missing | no sample 3분 | 5분 |
| SQL connections | >70% 5분 | 10분 |
| SQL storage | >80% 15분 | 20분 |
| SQL CPU | >80% 10분 | 15분 |
| MIG unhealthy | >0 5분 | 10분 |

검증 증거에는 alert policy ID, 사용한 query, 장애가 열린 시각과 복구된 시각을
저장한다.

## 9. 주요 위험

| 위험 | 완화 |
|---|---|
| Cloud SQL 병목 또는 장애 | 짧은 transaction, 70% 연결 예산, HA 장애 테스트 |
| 중복 메시지 발행 | 고정 ID, claim generation, 중복 저장 방지 |
| Finite handler가 겹쳐 실행 | 만료되는 claim과 최신 generation만 완료 허용 |
| 권한을 잃은 partition owner | DB lease, fencing token, 고정 결과 ID |
| 여러 version으로 잘못 전달 | revision filter, 요청 revision 고정, worker 기능 확인 |
| 같은 요청의 planner 결과 변경 | 바뀌지 않는 입력과 순서·key·hash 비교 |
| sink 장애로 데이터 증가 | 처리 속도 제한, 한도, 실패 상태, 재개 기능 |
| DB migration 장애 | 단계적 변경, PITR, 이전 단계로 전진 복구 |
| 느리거나 비싼 E2E | 고유한 이름, 제한 시간 polling, 전체 리소스 정리 |

## 10. 공식 근거

- Pub/Sub lease/retry: https://cloud.google.com/pubsub/docs/lease-management
- MIG autoscaling: https://cloud.google.com/compute/docs/autoscaler/scaling-cloud-monitoring-metrics
- Cloud SQL HA: https://cloud.google.com/sql/docs/postgres/high-availability
- PostgreSQL locks: https://www.postgresql.org/docs/current/explicit-locking.html
- Scheduled Cloud Run Jobs: https://cloud.google.com/run/docs/execute/jobs-on-schedule
- Python extras: https://packaging.python.org/en/latest/guides/writing-pyproject-toml/
- Artifact Registry Python: https://cloud.google.com/artifact-registry/docs/python
- IAM: https://cloud.google.com/iam/docs/best-practices-service-accounts

## 11. 실행 경계

실제 GCP E2E에는 credential 사용, 비용이 드는 리소스 생성, IAM 변경이 포함된다.
따라서 실행할 때는 외부 환경 변경 절차를 따로 거쳐야 한다.

이 문서는 무엇을 구현할지 정한 계획이다. 이 문서 자체가 GCP 리소스를 만들 권한을
부여하지는 않는다.
