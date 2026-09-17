# Runbook: 배포와 revision

## process role

`python -m distributed_runtime <role>` 하나의 image가 role을 나눈다.

| role | 동작 | 배포 대상 |
|---|---|---|
| `migrate` | schema migration 적용 후 종료 | Cloud Run Job |
| `worker` | finite claim + continuous supervise + heartbeat | MIG (COS) |
| `control` | planner + outbox dispatcher (+ 선택적 reconciler) | Cloud Run service |
| `api` | client JSON API — run 제출/조회/결과/취소 | Cloud Run service |
| `admin` | admin JSON API — deployment deploy/drain/stop/view | Cloud Run service |
| `reconcile` | continuous reconcile 1회 실행 후 종료 | Cloud Run Job (Scheduler) |

## HTTP API

- `api`와 `admin`은 별도 Cloud Run service로 배포하고
  `api_invokers`/`admin_invokers`로 IAM을 분리한다 — 호출자가
  client 표면에 접근 가능해도 admin은 거부(403)되어야 한다.
- 두 서비스 모두 `/`와 `/healthz`에 `{"status": "ok"}`를 반환하므로
  Cloud Run startup probe가 그대로 동작한다.
- 오류 매핑: 잘못된 본문/식별자 → 400, 없는 자원 → 404,
  상태 충돌 → 409, 나머지 → 500.
- api 서비스는 `RUNTIME_PLANNER_REVISION`/`RUNTIME_EXECUTION_REVISION`을
  읽어 제출된 run에 revision을 고정한다 (기본값은 pool revision).

## reconciler: loop vs scheduler

- `RUNTIME_CONTROL_RECONCILER=loop` (기본): control 서비스 안의
  loop이 매 tick reconcile을 돌린다. control replica가 없으면
  reconcile도 멈춘다.
- `RUNTIME_CONTROL_RECONCILER=off`: in-process loop을 끄고
  Cloud Scheduler → `reconcile` Cloud Run Job이 주기적으로 실행한다.
  scheduler job 생성자는 control SA에 `iam.serviceAccounts.actAs`
  권한이 필요하다 (`control_act_as` 변수).

## 새 revision 배포

1. product wheel을 빌드하고 runtime-worker image에 주입한다
   (`examples/*/Dockerfile` 참고).
2. image를 Artifact Registry에 push한다.
3. `terraform apply`로 MIG/Cloud Run이 새 image를 가리키게 한다.
4. `extra_pool_revisions`로 이전 revision subscription을 유지한다 —
   worker는 자기 revision의 메시지만 구독하고, store poll 경로도
   run에 고정된 execution revision만 claim하므로 이전 revision에 핀된
   run이 새 pool으로 새지 않는다.

## DB migration

- migration은 advisory lock 아래 순서대로 실행되며 checksum이 검증된다.
- 새 migration은 `src/distributed_runtime/state/migrations/`에
  `NNNN_name.sql`로 추가한다.
- migration 실패 시: lock을 잡은 connection이 남아있지 않은지 확인하고,
  checksum 불일치면 파일을 수정하지 말고 새 migration으로 되돌린다.

## 배포 후 확인

- worker registry에 새 instance가 heartbeat를 시작했는지 확인한다.
- continuous deployment가 있으면 reconciler가 partition을 새 worker에
  배정했는지 확인한다.
- `api`/`admin` 서비스가 `/healthz`에 200을 반환하는지 확인한다.
