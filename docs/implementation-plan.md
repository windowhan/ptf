# GCP Distributed Workload Runtime 구현 세부 계획

## 1. 목표와 범위

`docs/first.md`의 Phase 1~5를 모두 구현한다. 최초 릴리스는 한 사람이 관리하는 단일 GCP 프로젝트·단일 리전의 private internal runtime이며, domain-neutral Finite/Continuous examples를 실제 GCP에 배포해 E2E 완료를 증명한다.

비목표는 multicloud, Kubernetes abstraction, DAG engine, arbitrary remote code execution, exactly-once guarantee, product-specific workload, multi-project/region/team isolation, public PyPI다.

## 2. 핵심 결정

| 영역 | 결정 |
|---|---|
| Python | 단일 `distributed-runtime` distribution, Python >=3.12 |
| 상태 | Cloud SQL PostgreSQL 권위 원장 |
| Finite | Pub/Sub StreamingPull + revision별 MIG |
| Continuous | min 2 MIG + DB-time lease/fencing |
| Control | IAM client/admin Cloud Run services 분리 |
| Reconcile | Cloud Scheduler → single-task Cloud Run Job |
| Retry | Cloud SQL schedule/outbox; Pub/Sub retry/DLQ는 infra safety net |
| Emission | token-authorized, stable-ID emission outbox |
| Rollout | revision-filtered subscriptions + capability-aware assignment |
| Infra | Terraform, private Artifact Registry, dedicated service accounts |

```text
Private consumer → Client API → Cloud SQL
                              ├→ revision-filtered planner topic → planner MIG
                              └→ revision-filtered execution topic → finite MIG

Admin API → deployment/revision/drain/sink-resume
Scheduler → reconciler Job → Cloud SQL leases → continuous MIG
Emission outbox → dispatcher → external sink
```

## 3. Correctness invariants

### Durable planning

1. submit transaction이 immutable request와 planner/execution revision을 pin한다.
2. planner는 `(ordinal, unit_key, payload_hash)` sequence를 재현한다.
3. retry 결과가 기존 prefix/count/checksum과 다르면 `PlanningDriftError`다.
4. planning finalize 전에는 unit을 publish하지 않는다.

### Finite

1. outbox publish는 at-least-once이며 publish-side duplicate를 허용한다.
2. active unit은 owner/generation/expiry claim을 가진다.
3. current claim generation만 result/terminal state를 commit한다.
4. DB commit 뒤에만 Pub/Sub ack한다.
5. external side effect는 handler idempotency contract를 따른다.

### Continuous

1. lease는 DB clock과 monotonic fencing token을 사용한다.
2. renew/release/checkpoint/emission은 owner+token 조건이다.
3. emission dedupe key는 token-independent stable logical ID다.
4. successor replay도 같은 emission row로 수렴한다.
5. custom direct sink는 runtime fencing guarantee 밖이다.

### Revision/migration

1. submit 뒤 planner/execution revision pin은 불변이다.
2. 새 submission만 active revision 전환의 영향을 받는다.
3. finite message는 revision subscription 하나에만 일치한다.
4. continuous partition은 capability-compatible instance에만 할당된다.
5. migration은 expand → backfill → capability gate → contract 순서다.
6. contract 전 old backlog/claim/lease/incompatible heartbeat가 없어야 한다.

## 4. 운영 기본값

### Finite

- timeout 300초, 최대 3,600초
- max attempts 5
- retry 5초 base/300초 cap/full jitter
- infra DLQ 20 delivery attempts
- claim TTL = timeout + 120초

### Continuous

- heartbeat 15초, lease 60초, expiry 30초 전 renewal
- reconcile 매분, drain 120초, takeover 150초
- MIG min 2

### Rebalance

- ACTIVE/recent/compatible/non-draining instance만 후보
- weighted load 최소 instance 우선
- load 차이 <2면 sticky ownership
- tick당 `min(10, ceil(partitions × 5%))` 이동
- partition cooldown 10분
- steady-state weighted load 차이 <=1

### Sink 장애

- retry 5초 base/300초 cap
- 100 attempts 또는 24시간 뒤 `FAILED`
- 10,000 events 또는 256 MiB에서 backpressure
- 60초 초과 시 `BLOCKED_SINK`
- operator `:resume-sink` replay, stable emission ID 유지

## 5. Repository 목표

```text
src/distributed_runtime/{core,finite,continuous,gcp,worker,control,testing}/
migrations/
deploy/{terraform,images}/
examples/{finite_example,continuous_example}/
tests/{unit,contract,integration,e2e}/
docs/runbooks/
```

## 6. Review-sized commit plan

코드 commit은 원칙적으로 추가·삭제 합계 300~500줄이며 한 논리 변경과 targeted test를 함께 담는다. generated/lock/docs-only 예외는 padding하지 않고 commit body에 이유를 남긴다.

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
| 15 | durable planning repo | IT-PLAN-REPO |
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

### Milestone exit gates

1. Contracts: unit/contract/import matrix
2. Local kits: deterministic queue/clock/claim/lease/emission
3. PostgreSQL: migrations/state invariants
4. Finite: emulator + crash/outbox/claim
5. Infrastructure: Terraform/IAM/connection/migration preflight
6. Continuous: rebalance/emission/drain/crash
7. Actual GCP: finite/continuous/autoscaling/observability/teardown
8. Hardening: N/N-1/migration/failover/cleanup/release

상세 commit dependency graph은 consensus plan의 각 commit에 대해 명시하며, E2E는 runtime·infra·IAM exit gate 이전에 시작하지 않는다.

## 7. Actual GCP acceptance

### Finite

- 20 units: 19 success, 1 runtime dead-letter
- retry/rate-limit/timeout/crash/duplicate/permanent failure 주입
- publish duplicate여도 logical execution/side effect 하나
- MIG 0→N 15분, N→0 30분 이내
- revision switch 전 run은 기존 revision에서 완료

### Continuous

- 6 partitions, 최소 2 instances
- owner kill 뒤 150초 내 higher-token successor
- stale token write 거부
- bounded rebalance 후 weighted load 차이 <=1
- CPU load로 2→3 이상 15분 내 scale-out
- load 제거 뒤 30분 내 min 2 복귀

### Security/operations

- client caller는 admin API 호출 불가
- service account key/Editor/Owner 없음
- worst-case SQL connections <= max의 70%
- migration recovery evidence
- teardown 뒤 unexpected transient resource 없음

## 8. Alert baseline

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

Evidence는 policy ID, query, incident open/recovery timestamp를 저장한다.

## 9. 주요 위험

| 위험 | 완화 |
|---|---|
| SQL bottleneck/outage | short transactions, 70% budget, HA/fail-closed tests |
| publish duplicate | stable IDs, claim generation, dedupe |
| overlapping finite handler | expiring claim + fenced terminal commit |
| stale partition owner | DB lease/fencing/stable emission ID |
| mixed-version delivery | revision filters + submit pin + capabilities |
| planner drift | immutable input + ordinal/key/hash/checksum |
| sink outage growth | backpressure/watermark/failed/resume |
| migration outage | expand/contract/PITR/roll-forward |
| flaky/costly E2E | unique prefix/bounded polling/teardown inventory |

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

실제 GCP E2E는 credentials, billable resources, IAM 변경을 포함하므로 execution 단계에서 별도 external-production gate가 필요하다. 이 문서는 구현 권한이 아니라 합의된 계획이다.
