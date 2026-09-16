# Runbook: 배포와 revision

## 새 revision 배포

1. product wheel을 빌드하고 runtime-worker image에 주입한다
   (`examples/*/Dockerfile` 참고).
2. image를 Artifact Registry에 push한다.
3. `terraform apply`로 MIG/Cloud Run이 새 image를 가리키게 한다.
4. Pub/Sub module의 `pool_revisions`에 새 revision을 추가한다 —
   worker는 자기 revision의 topic만 구독한다(revision-pinned dispatch).

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
