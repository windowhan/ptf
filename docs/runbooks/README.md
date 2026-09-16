# Runbooks

> 운영자가 runtime을 배포·감시·복구할 때 따라 하는 절차 모음이다.
> `implementation-plan.md` #64 (docs-only)에 해당한다.
> 모든 명령은 배포 환경에 맞게 project/region을 바꿔 실행한다.

| 상황 | runbook |
|---|---|
| 새 배포, revision 올리기 | [deploy.md](deploy.md) |
| Finite run 조회, DLQ 처리 | [finite-operations.md](finite-operations.md) |
| Continuous 배포, drain/stop, worker 장애 | [continuous-operations.md](continuous-operations.md) |

공통 전제:

- 상태는 Cloud SQL의 `runtime_state` 스키마에 있다.
- 실행 메시지는 Pub/Sub을 지나고, durable outbox가 최소 한 번 발행을
  보장한다 — consumer는 중복을 견뎌야 한다.
- Continuous emission은 fencing token으로 보호된다 — 오래된 owner의
  emit은 저장소가 거부한다.
