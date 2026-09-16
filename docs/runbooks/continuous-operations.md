# Runbook: Continuous 운영

## 배포 생성

```python
deployment = await admin.deploy(workload="shard.pulse", version="1.0.0")
```

생성 직후 reconciler가 `discover_partitions()` 결과를 저장하고 활성
worker에게 lease를 배정한다.

## 정상 종료: drain → stop

```python
await admin.drain(deployment.deployment_id)   # DRAINING + lease 해제
await admin.stop(deployment.deployment_id)    # drain 선행 후 최종 상태
```

- drain은 활성 lease를 fenced release로 해제한다 — 오래된 owner의
  emit이 release를 가로채지 못한다.
- DRAINING/정지된 deployment에는 reconciler가 partition을 다시
  배정하지 않는다.

## Worker 장애

1. heartbeat가 끊긴 worker는 registry에서 비활성으로 간주된다.
2. reconciler가 다음을 감지해 partition을 회수한다:
   - lease가 만료된 partition
   - owner가 활성 worker 목록에 없는 partition
3. 회수된 partition은 새 fencing token과 함께 살아있는 worker에게
   재배정된다. 이전 owner는 fencing이 밀렸음을 감지해 emit을 멈춘다.

## Partition이 배정되지 않을 때

- 활성 worker가 없거나, worker의 `execution_classes`가 workload의
  execution class를 포함하지 않으면 배정되지 않는다.
- reconciler 결과의 `unassigned` 수를 확인한다.

## Sink가 오래된 emit을 거부할 때

- 정상 동작이다 — lease 상실 후 emit을 시도한 stale owner가 있다는 뜻.
- 해당 worker의 supervisor가 partition을 아직 잡고 있는지,
  fencing token이 올라갔는지 확인한다.
