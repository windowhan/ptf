# finite_example — 구간 합

`[start, end)` 범위를 청크로 나눠 분산 실행하고, 제품 코드가 결과를
종합하는 finite workload 예제다.

## 구조

```text
example_product/
├── app.py                    # RuntimeApplication 조합 루트
└── workloads/range_sum.py    # planner + handler + 제품 측 종합
runtime.yaml                  # 배포 매니페스트
Dockerfile                    # runtime-worker base image + product wheel
```

## 로컬 검증 (GCP 없음)

```bash
pip install -e ../..   # distributed-runtime
pip install -e .
pytest tests/
```

`FiniteRuntimeTestKit`으로 submit → unit 실행 → `results()` 조회 →
`aggregate_subtotals()` 종합 경로를 검증한다. 같은 workload 코드가 GCP
배포에서도 그대로 동작한다.

## GCP 실행

1. `hatch build`로 wheel 생성.
2. `Dockerfile`로 product wheel을 runtime-worker image에 주입.
3. runtime client으로 submit:

```python
run = await client.submit(
    workload="range.sum", version="1.0.0",
    input={"start": 0, "end": 1_000_000},
)
await client.wait(run.id)
total = aggregate_subtotals(await client.results(run.id))
```

실패한 unit이 있으면 `results`에 빠지므로 부분 합 인정 여부는 제품이
결정한다(기본: 실패 unit이 있으면 보고하고 종합하지 않는다).
