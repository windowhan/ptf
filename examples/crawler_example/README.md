# crawler_example — URL fetch 크롤러

URL 목록을 unit으로 나눠 분산 fetch하는 finite workload 예제다.
URL 하나 = unit 하나 — 재시도·멱등성·DLQ가 페이지 단위로 동작한다.

## 구조

```text
crawler_product/
├── app.py                  # RuntimeApplication 조합 루트
└── workloads/crawl.py      # planner + handler + 링크 추출/집계
runtime.yaml                # 배포 매니페스트
Dockerfile                  # runtime-worker base image + product wheel
```

## 설계 포인트

- **planner는 deterministic** — 링크 추적(depth>0)은 plan 중에 불가.
  결과의 `links`를 `next_frontier(results)`로 모아 **다음 run**을
  제출하는 breadth-first 방식이다.
- **unit_key는 URL의 SHA-256 앞 16자** — `?`,`&`,`=` 등이 contract
  이름 규칙(`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`)에 안 맞아 해시를 쓴다.
  같은 URL은 planner에서 dedup하므로 duplicate unit_key 오류가 없고,
  `idempotency_key` 기본값이 unit_key라 재전달도 안전하다.
- **HTTP → 오류 계약 매핑**: 네트워크 오류/5xx → `RetryableExecutionError`,
  429 → `RateLimitedExecutionError`(`retry-after` 헤더 반영),
  4xx → `PermanentExecutionError`(unit DLQ).
- **본문은 결과에 넣지 않는다** — 결과는 작은 JSON(title/links/bytes).
  원문 HTML이 필요하면 GCS에 쓰고 `ArtifactReference`를 반환한다
  (`core/artifacts.py`, sha256/generation 검증 내장).

## 로컬 검증 (GCP 없음)

```bash
pip install -e ../..   # distributed-runtime
pip install -e .
pytest tests/
```

`FiniteRuntimeTestKit`으로 submit → `run_pending` 실행 → `results()`
조회 → `next_frontier()` 집계 경로를 검증한다. HTTP는 `httpx.MockTransport`로
대체하며(`crawl._client_factory` 교체), 같은 workload 코드가 GCP 배포에서도
그대로 동작한다.

## GCP 실행

1. `hatch build`로 wheel 생성.
2. `Dockerfile`로 product wheel을 runtime-worker image에 주입.
3. runtime client으로 submit:

```python
run = await client.submit(
    workload="crawl.fetch", version="1.0.0",
    input={"urls": ["https://example.com/", ...]},
)
await client.wait(run.run_id)
results = await client.results(run.run_id)

# 다음 depth: 발견된 링크로 새 run 제출
run2 = await client.submit(
    workload="crawl.fetch", version="1.0.0",
    input={"urls": next_frontier(results)},
)
```

실패한 unit(404/403/DLQ)은 `results`에서 빠지므로 부분 결과 인정
여부는 제품이 결정한다.
