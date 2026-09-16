# 상태 스키마 설계

> PostgreSQL(Cloud SQL) 상태 계층의 테이블·불변식·트랜잭션 경계를 정한다.
> `implementation-plan.md`의 #12~22(IT-SQL-BOOT → IT-HEART)에 앞서는 설계 문서다.
> 이 문서는 DDL 초안이 아니라 **스키마가 지켜야 할 계약**을 정한다. 실제 DDL은
> migration 파일이 소유하고, 이 문서는 그 파일들이 만족해야 할 조건을 나열한다.

## 1. 목적과 범위

runtime의 모든 orchestration metadata는 PostgreSQL 한 DB의 `runtime_state`
스키마 아래에 둔다. 제품 데이터(크롤링 결과 본문, 변환 파일 등)는 이 스키마에
들어가지 않는다 — unit 결과와 emission은 envelope payload로만 저장된다
(ADR-004, ADR-007).

이 문서가 정하는 것:

- 테이블 목록과 각 행이 나타내는 것
- 테이블을 가로지르는 불변식(invariant)
- 상태 변경과 outbox 메시지를 한 transaction에 묶는 경계 규칙
- Python 계약 타입 → SQL 타입 매핑 규칙
- migration 버전 관리 방식

이 문서가 정하지 않는 것:

- 실제 DDL 문법·인덱스 명세 — migration 파일(#14, #16, #19)이 소유
- 쿼리 튜닝·파티셔닝 — 부하 테스트(#50~) 결과로 결정
- 제품 스키마 — 제품 DB/버킷이 소유

## 2. 네임스페이스와 migration

- 모든 runtime 테이블은 `runtime_state` 스키마 아래에 둔다. 제품 DB와 같은
  인스턴스를 공유해도 이름 충돌하지 않는다.
- migration은 `runtime_state.schema_migrations` 테이블이 순번(`version` int,
  단조 증가)과 적용 시각, 체크섬을 기록한다.
- migration 파일은 `000N_name.sql` 형태로 순서대로만 적용된다. 이미 적용된
  버전은 체크섬이 다르면 실패로 처리한다(덮어쓰기 금지).
- migration runner(#13)는 advisory lock으로 동시 적용을 막는다.
- N-1 호환성 규칙(#54): 새 컬럼은 nullable 또는 default를 갖는다. 기존 컬럼의
  타입 변경·삭제는 expand-migrate-contract 순서의 별도 migration으로만 한다.

## 3. Finite 테이블

### 3.1 `finite_runs` — 제출된 run 한 건

한 행 = `client.submit()` 한 번.

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `run_id` | text PK | `RunId` (`run:...`) |
| `workload_id` | text | `WorkloadId` |
| `workload_name` / `workload_version` | text | 등록 키 |
| `planner_revision` / `execution_revision` | text | `RevisionId` — 계획과 실행의 revision 고정 |
| `status` | text | `RunStatus` 값 |
| `input` | jsonb | `WorkloadRequest.payload` |
| `created_at` / `updated_at` | timestamptz | |
| `cancelled_at` | timestamptz null | |

불변식:

- `status`는 `RunStatus` 전이만 허용한다: `pending → planning → running →
  succeeded | failed | cancelled`. 뒤로 가는 전이는 없다.
- 같은 `run_id`의 재제출은 행을 새로 만들지 않고 기존 행을 돌려준다
  (제출 idempotency는 client API 계약 — #30).

### 3.2 `finite_plan_units` — 계획이 낸 unit 목록

한 행 = `PlanningRecord` 한 개. planner가 yield한 순서 그대로 저장된다.

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `run_id` | text FK→finite_runs | |
| `ordinal` | int | yield 순서, 0부터 |
| `unit_key` | text | |
| `payload_hash` | text | 계획 drift 비교용 해시 |
| `unit` | jsonb | `ExecutionUnit` 직렬화 (handler, payload, execution_class, timeout_seconds, max_attempts, idempotency_key) |
| PK | `(run_id, ordinal)` | |
| UNIQUE | `(run_id, unit_key)` | `validate_plan`과 같은 규칙을 DB에서도 강제 |

불변식:

- 저장된 계획은 불변이다. replanning은 새 `planning_generation` 컬럼 값으로
  구분한 새 행 집합을 쓰고, `expected`와 비교해 drift를 `PlanningDriftError`로
  표면화한다 — drift 비교는 repository가 `payload_hash`로 한다.

### 3.3 `finite_units` — unit 실행 상태

한 행 = 실행 대상 unit 한 개.

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `run_id` / `unit_key` | text | PK `(run_id, unit_key)` |
| `status` | text | `ExecutionStatus` 값 |
| `attempt_count` | int | 시도된 횟수 |
| `next_attempt_at` | timestamptz null | 재시도 예약 시각 |
| `claimed_by` | text null | `RuntimeInstanceId` — 현재 claim 소유자 |
| `claim_expires_at` | timestamptz null | claim TTL (`timeout_seconds + claim_grace_seconds`) |
| `claim_token` | bigint | claim 때마다 증가 — fencing과 같은 역할 |
| `result` | jsonb null | 성공 시 handler 반환값(envelope payload) |
| `last_error` | jsonb null | `ErrorKind`, 메시지 |
| `created_at` / `updated_at` | timestamptz | |

불변식:

- claim은 낙관적 갱신이다: `UPDATE ... WHERE status='ready' AND
  (claim_expires_at IS NULL OR claim_expires_at < now())`가 행을 잡았을 때만
  실행 권한이 생긴다. `claim_token`이 올라가면 이전 claim holder의 쓰기는
  거부된다.
- `status='succeeded'`인 행의 `result`는 첫 성공 값에서 바뀌지 않는다 —
  중복 전달·재실행 결과가 와도 기존 행을 유지한다(idempotent 기록).
- `result`는 runtime이 읽어서 `client.results(run_id)`로 돌려주는 값이다.
  종합하지 않는다(ADR-007).

### 3.4 `finite_attempts` — 시도 이력

한 행 = unit 실행 한 번. 감사·디버깅용 append-only 로그.

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `run_id` / `unit_key` / `attempt` | text/text/int | PK 3개 복합 |
| `execution_id` | text | `ExecutionId` — run+revision+unit+attempt로부터 결정 |
| `started_at` / `finished_at` | timestamptz | |
| `outcome` | text | succeeded / retryable / permanent / rate_limited / cancelled |
| `error` | jsonb null | |

불변식: 같은 `(run_id, unit_key, attempt)`는 한 번만 쓴다 — worker crash 후
재기록 시도는 충돌로 감지한다.

## 4. 공통: `outbox_messages`

한 행 = 발행할 메시지 한 개. finite unit dispatch와 continuous emission이 같은
테이블을 공유한다.

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `outbox_id` | bigint PK generated | 순서 부여용 |
| `kind` | text | `unit_dispatch` / `emission` / `checkpoint` |
| `dedup_key` | text | UNIQUE — 같은 논리 메시지는 한 행 |
| `destination` | text | Pub/Sub topic 또는 sink 이름 |
| `envelope` | jsonb | `VersionedEnvelope` 직렬화 |
| `status` | text | pending / published / failed |
| `published_at` | timestamptz null | |
| `attempts` | int | 발행 재시도 횟수 |

불변식:

- **outbox 규칙**: 상태 변경과 outbox 행 insert는 반드시 같은 transaction이다.
  이 규칙이 at-least-once의 근거다 — 상태는 저장됐는데 메시지가 없는 경우가
  없고, 메시지는 있는데 상태가 없는 경우도 없다.
- 발행은 최소 한 번이다. dispatcher(#26)가 `pending`을 읽어 발행하고
  `published`로 마킹한다. Pub/Sub 재전달이 일어나도 `dedup_key`로 같은 행에
  모인다.
- emission 행의 `dedup_key`는 `(deployment_id, partition_id, stable_id)`다.
  다른 owner가 같은 `stable_id`로 다시 쓰면 같은 행에 모이되, `fencing_token`
  은 발행 시점의 최대값을 기록한다(컬럼 `fencing_token` bigint).

## 5. Continuous 테이블

### 5.1 `continuous_deployments` — 배포 한 건

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `deployment_id` | text PK | `DeploymentId` |
| `workload_id` / `workload_version` | text | |
| `status` | text | `DeploymentStatus` 값 |
| `config` | jsonb | `ContinuousPolicy` + 제품 config |
| `created_at` / `updated_at` | timestamptz | |

### 5.2 `continuous_partitions` — partition 상태 + lease

lease는 별도 테이블이 아니라 partition 행의 컬럼이다 — lease 갱신과 partition
상태 변경이 한 행 갱신이어야 fence 비교가 원자적이기 때문이다.

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `deployment_id` / `partition_id` | text/text | PK 복합 |
| `status` | text | `PartitionStatus` 값 |
| `weight` | int | `Partition.weight` |
| `payload` | jsonb | `Partition.payload` |
| `owner_id` | text null | `RuntimeInstanceId` |
| `fencing_token` | bigint | 새 lease 때마다 단조 증가 |
| `lease_expires_at` | timestamptz null | |
| `assigned_at` | timestamptz null | |

불변식:

- `fencing_token`은 절대 감소하지 않는다. lease 갱신·인계는
  `UPDATE ... WHERE fencing_token = :expected` 조건부 갱신으로만 한다.
- emission 쓰기 권한 = `owner_id` 일치 + `fencing_token` 일치 +
  `lease_expires_at > now()`. 셋 중 하나라도 다르면 거부 — 이 검사는 emission
  insert와 같은 transaction에서 일어난다.
- `owner_id IS NULL`이면 unassigned다.

### 5.3 `worker_instances` — capabilities/heartbeat

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `instance_id` | text PK | `RuntimeInstanceId` |
| `execution_classes` | jsonb | 지원 execution_class 목록 |
| `revision` | text | `RevisionId` — 배포 revision |
| `last_heartbeat_at` | timestamptz | |
| `status` | text | active / draining / dead |

불변식: `last_heartbeat_at`이 `heartbeat_seconds`의 일정 배수를 넘으면
reconciler가 `dead`로 표시하고 해당 instance의 partition을 회수 대상으로 본다.

## 6. 트랜잭션 경계 규칙

이 규칙들은 repository 구현(#15~#21)이 지켜야 할 것으로, 스키마가 전제하는
계약이다:

1. **제출**: `finite_runs` insert + (필요시) planning 시작 표시 — 1 transaction.
2. **계획 저장**: `finite_plan_units` 전체 insert + `finite_units` ready insert
   + `finite_runs.status='running'` — 1 transaction. 계획의 일부만 저장되는
   일은 없다.
3. **claim**: `finite_units` 조건부 UPDATE 1건 = 1 transaction. claim한 worker만
   다음 단계로 간다.
4. **완료 기록**: `finite_attempts` insert + `finite_units` 상태/result 갱신 +
   (성공 시) 결과는 같은 행에 — 1 transaction. queue ack는 이 transaction이
   commit된 뒤에만 한다(extension-boundaries 규칙).
5. **lease 갱신**: `continuous_partitions` 조건부 UPDATE — 1 transaction.
6. **emission**: `outbox_messages` insert + `continuous_partitions`의 fencing
   검증 UPDATE — 1 transaction. fencing이 실패하면 emission도 rollback된다.
7. **partition 인계**: `fencing_token` 증가 + `owner_id` 교체 + 기존 owner
   context 취소 표시 — 1 transaction.

## 7. 타입 매핑 규칙

| Python 계약 | SQL |
|---|---|
| `RunId`, `DeploymentId` 등 식별자 | `text` (접두사 포함 문자열 그대로) |
| `payload`, `envelope`, `result` | `jsonb` |
| 시각 | `timestamptz` (UTC만) |
| 상태 enum | `text` + CHECK 제약 대신 repository가 enum으로 검증 — enum 추가 시 migration 불필요하게 하기 위해 |
| fencing/claim token | `bigint` |
| ordinal/attempt/카운터 | `int` |

## 8. 명시적으로 범위 밖

- **결과 종합 테이블** — 없다. 결과는 `finite_units.result`에 unit별로 있고
  종합은 제품이 조회해서 한다(ADR-007).
- **제품 도메인 테이블** — runtime 스키마에 두지 않는다.
- **실제 Pub/Sub·GCS 상태** — outbox는 "발행 의도"의 기록이지 Pub/Sub 자체의
  상태가 아니다.
- **샤딩·멀티 DB** — 단일 Postgres를 전제한다.
- **secret 저장** — Secret Manager가 소유, DB에는 참조만 둘 수 있다.
