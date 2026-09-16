# continuous_example — shard.pulse

고정 샤드 집합을 discovery하고, 각 파티션이 fencing이 적용된 `pulses`
sink로 유한한 pulse 이벤트를 발행하는 continuous workload 예제다.

## 구조

```text
example_stream/
├── app.py                      # RuntimeApplication 조합 루트
├── sinks.py                    # ListSink — fenced 이벤트 수집
└── workloads/shard_pulse.py    # ContinuousWorkload (discover + run)
runtime.yaml                    # 배포 매니페스트
Dockerfile                    # runtime-worker base image + product wheel
```

## 동작

- `discover_partitions()`가 `shard:0..3` 파티션 4개를 반환한다.
- reconciler가 활성 worker에게 lease를 배정하고 fencing token을 발급한다.
- 각 파티션 핸들러는 `PULSES_PER_SHARD`개 이벤트를 발행하고 종료한다.
- `stable_id`가 deterministic이라 at-least-once 재전달이 dedup된다.
- cancellation(lease 상실, drain)이 오면 즉시 발행을 멈춘다 — 오래된
  fencing token의 emit은 runtime이 거부한다.

## 로컬 검증 (GCP 없음)

```bash
PYTHONPATH=. pytest tests/
```

`ContinuousRuntimeTestKit`으로 deploy → reconcile → `run_for` → emission
기록 조회 경로를 검증한다.

## GCP 실행

1. `hatch build`로 wheel 생성.
2. `Dockerfile`로 product wheel을 runtime-worker image에 주입.
3. admin API로 배포 생성 → reconciler가 파티션 배정 → supervisor가
   파티션 핸들러 실행 → fenced sink로 발행.
