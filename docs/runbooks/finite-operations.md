# Runbook: Finite 운영

## Run 상태 조회

`RuntimeClient`로 run 상태와 unit 결과를 조회한다.

```python
run = await client.get(run_id)        # 상태, 시각
partials = await client.results(run_id)  # SUCCEEDED unit 결과, 계획 순서
```

실패한 unit은 `results`에 포함되지 않는다 — 부분 결과로 종합할지는
제품이 정한다(ADR-007).

## Unit이 계속 실패할 때

1. attempt 기록에서 오류 분류를 확인한다 — `RETRYABLE`이면
   `retry_at`에 따라 dispatcher가 다시 깨운다.
2. `max_attempts`를 넘으면 dead-letter 된다. run은 부분 실패 상태로
   남는다.
3. dead-letter된 unit은 payload와 오류를 보고 제품이 수동으로
   처리한다. runtime은 dead-letter를 자동으로 재실행하지 않는다.

## Claim이 stale 상태로 남을 때

- claim expiry가 지나면 다른 worker가 같은 unit을 `FOR UPDATE
  SKIP LOCKED`로 다시 잡는다 — 수동 개입 불필요.
- 같은 unit이 중복 실행돼도 `idempotency_key`가 같으므로 결과
  저장은 한 번만 반영된다.

## Pub/Sub backlog가 쌓일 때

- planner dispatch와 retry dispatcher는 at-least-once다. worker가
  느리면 unacked가 쌓인다.
- MIG autoscaling이 custom metric으로 worker 수를 늘린다. backlog가
  계속 늘면 max_replicas와 DB connection 한도(`PoolBudget`)를 확인한다.
