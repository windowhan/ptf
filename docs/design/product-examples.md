# 제품 예제 설계

> `examples/finite_example`(구간 합)과 `examples/continuous_example`
> (shard pulse)의 설계 계약이다. 두 예제 모두 로컬 테스트 킷으로
> 검증됐으며, 실제 GCP 배포 검증은 #50 이후 E2E 구간에서 진행한다.
>
> **문서 상태:** 구현 반영됨

## 목적

예제는 두 가지를 보여준다.

1. 새 제품이 runtime을 사용해 처음부터 끝까지 만드는 방법
2. 같은 제품 코드가 로컬 테스트 킷과 실제 GCP 배포에서 동일하게 동작하는 것

쉽게 말하면, **예제가 곧 E2E 검증 대상이자 사용 설명서**다. 예제는 특정 제품에
묶이지 않는다. 쇼핑몰, 거래소 같은 이름을 쓰지 않고 workload 형태만 보여준다.

## Finite 예제: 구간 합

`implementation-plan.md` #48, Gate `CT-EX-FIN`에 해당한다.

### 왜 수치 계산인가

- "분배 → 실행 → 종합" 흐름이 가장 단순하게 드러난다.
- 외부 서비스가 없어 로컬 킷과 GCP에서 같은 코드가 그대로 동작한다.
- 결과의 정답을 미리 알 수 있어(`sum(range(start, end))`) E2E 검증이 명확하다.

### Workload

요청 `[start, end)`을 고정 크기 구간으로 나누고, 각 unit이 구간 합을 반환한다.

| 항목 | 값 |
|---|---|
| workload 이름 | `range.sum` |
| execution class | `lightweight` |
| unit key 형태 | `range:{start}-{end}` |
| unit 결과 | `{"subtotal": <구간 합>}` — 작은 JSON 결과 |

```python
class RangeSumPlanner:
    name = "range.sum"
    version = "1.0.0"
    mode = WorkloadMode.FINITE

    async def __call__(self, request: WorkloadRequest):
        lo, hi = request.payload["start"], request.payload["end"]
        while lo < hi:
            step = min(lo + 10_000, hi)
            yield ExecutionUnit(
                unit_key=f"range:{lo}-{step}",
                handler="range_sum.partial",
                payload={"start": lo, "end": step},
                execution_class="lightweight",
            )
            lo = step


async def sum_partial(ctx, payload):
    return {"subtotal": sum(range(payload["start"], payload["end"]))}
```

`unit_key`는 배열 순번이 아니라 처리 대상(`range:0-10000`)을 나타낸다.
[Finite workload 개념 문서](../concepts/finite-workloads.md)의 key 지침을
따른다.

### 결과 종합 — 제품 소유

ADR-007에 따라 runtime은 종합 단계를 실행하지 않는다. 예제의 제출 코드가
결과를 읽어 직접 합친다.

```python
run = await client.submit(
    workload="range.sum",
    version="1.0.0",
    input={"start": 0, "end": 1_000_000},
)
await client.wait(run.id)

partials = await client.results(run.id)
total = sum(p["subtotal"] for p in partials)
```

`client.submit`, `client.wait`, `client.results`는 #30의 client API로
구현된다. 실패한 unit이 있을 때 어떻게 할지(부분 합 인정 여부)도 제품이
정한다. 예제는 "실패 unit이 있으면 종합하지 않고 보고"하는 것을 기본
동작으로 한다.

### 파일 구성

```text
examples/finite_example/
├── pyproject.toml              # distributed-runtime 의존
├── example_product/
│   └── workloads/range_sum.py  # planner + handler
├── runtime.yaml                # application 이름, workload, execution class
├── Dockerfile                  # base runtime image + product wheel
└── README.md                   # 로컬 실행과 GCP 배포 방법
```

`Dockerfile`은 first.md §21.2의 배포 모델을 따른다.

```dockerfile
FROM runtime-worker:<version>

COPY dist/example_product.whl /tmp/
RUN pip install /tmp/example_product.whl

COPY runtime.yaml /app/runtime.yaml
```

### 검증 경로

같은 workload 코드를 두 환경에서 실행한다.

1. **로컬:** `FiniteRuntimeTestKit`에 등록해 submit → run → 결과 기록 조회.
   [로컬 테스트 킷 설계](local-testing-kits.md) 참고.
2. **GCP:** wheel로 빌드해 runtime image에 넣고 배포한 뒤 client로
   submit → wait → results → 합산. #50 이후 E2E의 검증 대상.

## Continuous 예제: shard pulse

`implementation-plan.md` #49, Gate `CT-EX-CON`에 해당한다. 구현된
소재는 `shard.pulse`다 — 고정 샤드 집합을 discovery하고 각 파티션이
fenced `pulses` sink로 유한한 pulse 이벤트를 발행한다.

| 항목 | 값 |
|---|---|
| workload 이름 | `shard.pulse` |
| execution class | `stateful-stream` |
| partition | `shard:{i}` (4개) |
| emission | deterministic `stable_id` `{partition}:{tick}` — at-least-once 재전달이 dedup된다 |

선택 조건을 모두 만족한다: partition으로 나눌 수 있고, 외부 서비스
없이 로컬 킷으로 동작하며, `PartitionContext.emit()` 경로를 보여주고,
제품 특화 이름을 쓰지 않는다. cancellation(lease 상실, drain) 시
즉시 발행을 멈추는 것도 예제 테스트가 검증한다.

## 완료 기준

- 예제 package가 wheel로 빌드되고 runtime image 주입 형태를 따른다.
- 로컬 킷에서 요청 → 분산 실행 → 결과 조회 → 종합 경로가 동작한다.
- 실제 GCP 배포와 teardown은 #50 이후 E2E gate에서 확인한다.

## 관련 결정

- [ADR-007](../first.md): 결과 종합은 제품이 소유한다. runtime은 unit 결과
  저장과 run 단위 조회만 제공한다.
- [ADR-003](../first.md): 제품에 치우치지 않은 이름을 쓴다. 예제는
  `range.sum` 같은 일반 이름만 사용한다.
