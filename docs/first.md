# GCP Distributed Workload Runtime
## 재사용 가능한 Python library 중심의 목표 아키텍처

> 이 문서는 Phase 1~5 전체 **목표 아키텍처**를 설명한다.
> 현재 Milestone01에서 실제 구현된 계약 계층과 미구현 경계는
> [`docs/README.md`](README.md)와
> [`현재 계약 계층 아키텍처`](architecture/current-contract-layer.md)를 먼저 참고한다.
> 처음 보는 용어는 [`쉬운 용어 설명`](glossary.md)에서 확인할 수 있다.

**문서 상태:** 최종 초안
**문서 버전:** 1.1
**실행 환경:** Google Cloud Platform
**배포 형태:** 재사용하는 Python library와 실제 실행 component

---

# 1. 먼저 읽는 요약

이 프로젝트는 특정 제품 하나를 위한 실행 프로그램이 아니다. 여러 제품이 공통으로
가져다 쓸 수 있는 분산 작업 runtime을 만드는 것이 목표다.

runtime은 크게 두 종류의 작업을 처리한다.

1. **Finite workload: 처리할 양이 정해진 작업**
   - 큰 요청을 작은 unit으로 나눈다.
   - 각 unit을 따로 실행하고 재시도한다.
   - 모든 unit 처리가 끝나면 run도 끝난다.
   - queue에 쌓인 양에 따라 worker 수를 바꾼다.

2. **Continuous workload: 계속 실행하는 작업**
   - 처리 대상을 partition으로 나눈다.
   - worker가 일정 시간 처리 권한인 lease를 얻는다.
   - worker 상태를 heartbeat로 확인한다.
   - worker가 죽으면 다른 worker가 partition을 이어받는다.

예를 들어 쇼핑몰 상품 1,000개를 한 번 수집하는 일은 Finite 작업이다. 실시간 시장
데이터를 계속 받는 일은 Continuous 작업이다.

상품이나 시장 데이터의 구체적인 처리 방법은 runtime에 넣지 않는다. 각 제품 package가
자신의 handler와 데이터 형식을 제공한다.

```text
Product A                    Product B
Batch Data Collection       Streaming Data Ingestion
       |                              |
       +--------------+---------------+
                      |
                      v
         +---------------------------+
         | Distributed Runtime SDK   |
         | finite / continuous       |
         +-------------+-------------+
                       |
                       v
         +---------------------------+
         | GCP Runtime Components    |
         | Pub/Sub / MIG / SQL / GCS |
         +---------------------------+
```

---

# 2. 최종 사용 모습

다른 프로젝트가 Python dependency 하나를 추가해 runtime을 사용할 수 있어야 한다.

```toml
[project]
dependencies = [
  "distributed-runtime[gcp]"
]
```

아래 코드는 **최종 목표 API의 예시**다. 현재 Milestone01 API와는 다를 수 있다.
제품 코드는 GCP 리소스 생성이나 worker 배치 방법을 직접 구현하지 않는다.

```python
from distributed_runtime import finite

@finite.handler("product.snapshot")
async def snapshot_product(ctx, payload):
    return await collect_snapshot(payload["product_id"])
```

```python
from distributed_runtime import continuous

class MarketStream(continuous.PartitionHandler):
    async def run(self, ctx, partition):
        async for event in subscribe(partition):
            await ctx.emit(event)
```

Runtime library가 맡을 일:

- workload 이름, 버전, 종류와 handler 등록
- Finite unit과 Continuous partition이 따라야 할 입력·출력 규칙 제공
- 요청과 결과를 Pub/Sub 또는 DB에 저장할 형식으로 변환
- 일시적 실패의 재시도 시각과 최대 횟수 관리
- 같은 메시지가 다시 와도 같은 작업임을 알아보는 ID 관리
- partition lease 발급, 연장, 만료, 인계
- worker가 살아 있는지 heartbeat로 확인
- Pub/Sub 메시지 보내기, 받기, ack/nack 처리
- 제품 handler 실행과 결과 저장
- 종료 신호를 받았을 때 새 작업을 멈추고 진행 중인 일 정리
- 로그, metric, 오류 정보 수집
- 실행에 사용한 image, version, pool, instance, region 기록

제품 package가 맡을 일:

- 상품 수집처럼 제품에만 있는 실제 처리 코드
- Finite 요청을 `ExecutionUnit`으로 나누는 planner
- handler가 받을 payload의 field와 타입
- 현재 처리해야 할 Continuous partition 목록
- 제품 결과와 event의 의미와 형식
- checkpoint 복구나 보상 처리처럼 제품에만 있는 복구 방법

---

# 3. 만들지 않는 것

초기 버전은 아래 기능까지 해결하려 하지 않는다.

- AWS와 GCP를 같은 API로 다루는 멀티클라우드 기능
- Kubernetes 공통 계층
- 사용자가 보낸 임의의 코드를 원격 실행하는 기능
- 작업 순서를 그래프로 만드는 범용 DAG engine
- 외부 부수 효과까지 포함한 exactly-once 보장
- 특정 제품의 crawler 또는 collector
- 제품 DB schema 강제
- Kafka Streams 같은 범용 stream-processing engine 대체

---

# 4. 핵심 개념

## 4.1 Workload

`Workload`는 runtime이 실행하고 관리할 수 있는 최상위 논리 단위다.

```python
class Workload(Protocol):
    name: str
    version: str
    mode: WorkloadMode
```

```python
class WorkloadMode(str, Enum):
    FINITE = "finite"
    CONTINUOUS = "continuous"
```

---

## 4.2 Finite Workload

Finite Workload는 유한한 실행 단위 집합으로 분해된다.

```python
class FiniteWorkload(Protocol):
    name: str
    version: str

    async def plan(
        self,
        request: WorkloadRequest,
    ) -> AsyncIterator[ExecutionUnit]:
        ...
```

예:

```text
Workload Request
    |
    v
Planner
    |
    v
Execution Unit 1
Execution Unit 2
Execution Unit 3
```

`ExecutionUnit`은 기존 문서의 product-specific `Task`를 대체하는 범용 용어다.

```python
class ExecutionUnit:
    id: str
    workload: str
    handler: str
    payload: dict
    execution_class: str
    timeout_seconds: int
    max_attempts: int
    idempotency_key: str
```

---

## 4.3 Continuous Workload

Continuous workload는 서비스가 운영되는 동안 계속 실행된다. 외부 event를 구독하거나
데이터를 반복해서 읽는 작업에 사용한다.

하나의 workload는 서로 따로 처리할 수 있는 여러 partition으로 나뉜다. 제품은
`discover_partitions()`에서 지금 처리해야 할 partition 목록을 runtime에 전달한다.

runtime은 각 partition을 실행 가능한 worker에 배정하고 `run_partition()`을 호출한다.
lease가 유효한 동안에는 worker 하나만 그 partition을 맡는다.

lease는 partition을 처리할 수 있는 **만료 시간이 있는 임시 권한**이다. worker가
명시적으로 반납하지 못해도 시간이 지나면 자동으로 끝난다. 정상 worker는 heartbeat를
보내고 lease를 주기적으로 연장한다.

worker가 죽거나 heartbeat를 보내지 못하면 lease가 끝난다. runtime은 partition을 다른
worker에 다시 배정한다. 이 과정은 제품 코드와 분리해 처리한다.

```python
class ContinuousWorkload(Protocol):
    name: str
    version: str

    async def discover_partitions(
        self,
    ) -> list[Partition]:
        ...

    async def run_partition(
        self,
        context: PartitionContext,
        partition: Partition,
    ) -> None:
        ...
```

예:

```text
Continuous Workload
    |
    v
Partition Discovery
    |
    +-- Partition 0
    +-- Partition 1
    +-- Partition 2
```

Partition은 다음과 같은 대상을 표현할 수 있다.

- market group
- WebSocket subscription group
- blockchain range
- account group
- tenant partition
- log stream
- message source
- long-running polling range

---

## 4.4 Execution Class

Execution Class는 어떤 runtime pool에서 workload가 실행될지를 나타낸다.

```yaml
execution_classes:
  lightweight:
    queue: runtime-lightweight
    runtime_pool: lightweight-pool

  browser:
    queue: runtime-browser
    runtime_pool: browser-pool

  stateful-stream:
    runtime_pool: stream-pool
```

Core library는 `crawler`, `collector`, `polymarket` 같은 domain name을 알지 못한다.

Product가 자신의 workload를 execution class에 연결한다.

```python
@finite.handler(
    name="product.snapshot",
    execution_class="browser",
)
async def handler(ctx, payload):
    ...
```

---

# 5. Python package 구성

권장 package 구조:

```text
distributed-runtime/
├── packages/
│   ├── runtime-core/
│   ├── runtime-finite/
│   ├── runtime-continuous/
│   ├── runtime-gcp/
│   ├── runtime-worker/
│   ├── runtime-control/
│   └── runtime-testing/
│
├── deploy/
│   ├── terraform/
│   └── images/
│
├── examples/
│   ├── finite-example/
│   └── continuous-example/
│
└── docs/
```

Python namespace:

```text
distributed_runtime.core
distributed_runtime.finite
distributed_runtime.continuous
distributed_runtime.gcp
distributed_runtime.worker
distributed_runtime.testing
```

---

# 6. 제품 코드와 연결하는 방법

Product repository는 runtime library를 dependency로 사용한다.

```text
product-repository/
├── pyproject.toml
├── product/
│   ├── workloads/
│   │   ├── batch_snapshot.py
│   │   └── live_stream.py
│   ├── schemas/
│   └── domain/
├── runtime.yaml
└── Dockerfile
```

예시 설정:

```yaml
application:
  name: market-data-product
  version: 1.0.0

workloads:
  product-snapshot:
    mode: finite
    implementation: product.workloads.batch_snapshot
    execution_class: browser

  market-stream:
    mode: continuous
    implementation: product.workloads.live_stream
    execution_class: stateful-stream
```

Runtime library는 product package를 import하고 workload registry를 구성한다.

---

# 7. Workload 등록소

Library는 decorator 또는 explicit registration을 지원한다.

## 7.1 Decorator로 등록

```python
from distributed_runtime import finite

@finite.handler(
    name="catalog.snapshot",
    execution_class="browser",
)
async def snapshot(ctx, payload):
    ...
```

## 7.2 함수로 직접 등록

```python
registry.register_finite(
    name="catalog.snapshot",
    handler=snapshot,
    execution_class="browser",
)
```

Continuous workload:

```python
registry.register_continuous(
    name="market.stream",
    workload=MarketStream(),
    execution_class="stateful-stream",
)
```

---

# 8. 전체 시스템 구조

```text
                         +-------------------------+
                         | Product Application     |
                         | Workload Definitions    |
                         +------------+------------+
                                      |
                                      v
                         +-------------------------+
                         | Runtime SDK             |
                         | Registry / Client       |
                         +------------+------------+
                                      |
                                      v
                         +-------------------------+
                         | Runtime Control Plane   |
                         | API / State / Reconcile |
                         +------------+------------+
                                      |
                    +-----------------+-----------------+
                    |                                   |
                    v                                   v
       +----------------------------+      +----------------------------+
       | Finite Execution Plane     |      | Continuous Execution Plane |
       | Pub/Sub + Runtime Pools    |      | Partition + Lease Runtime  |
       +-------------+--------------+      +-------------+--------------+
                     |                                   |
                     v                                   v
       +----------------------------+      +----------------------------+
       | Product Handlers           |      | Product Partition Handlers |
       +-------------+--------------+      +-------------+--------------+
                     |                                   |
                     +-----------------+-----------------+
                                       |
                                       v
                          +---------------------------+
                          | Product-Owned Data Sinks  |
                          +---------------------------+
```

---

# 9. Finite 작업 처리 흐름

## 9.1 요청 제출

```python
client.submit(
    workload="catalog.snapshot",
    request={
        "product_ids": ["A", "B", "C"],
    },
)
```

## 9.2 작은 작업으로 나누기

제품이 제공한 planner는 공통 형식인 `ExecutionUnit`을 하나씩 반환한다.

```python
async def plan(request):
    for product_id in request["product_ids"]:
        yield ExecutionUnit(
            handler="catalog.snapshot.item",
            payload={"product_id": product_id},
            execution_class="browser",
        )
```

## 9.3 Queue로 보내기

runtime은 `execution_class`에 맞는 Pub/Sub topic을 선택한다.

```text
lightweight
  -> runtime-lightweight

browser
  -> runtime-browser

compute
  -> runtime-compute
```

별도 TaskRouter service는 두지 않는다. 이 mapping은 runtime configuration으로 처리한다.

## 9.4 Worker에서 처리하기

```text
Execution Unit
   -> Pub/Sub
   -> Runtime Worker
   -> Handler Registry
   -> Product Handler
   -> Result
```

## 9.5 재시도와 최종 실패

Runtime은 다음을 표준화한다.

- **acknowledgment deadline**: worker가 Pub/Sub 메시지를 처리할 수 있는 제한 시간이다.
  완료하면 ack한다. 시간이 더 필요하면 deadline을 연장한다.
- **retry classification**: 오류를 재시도 가능, 영구 실패, rate limit 등으로 나눠
  다음 행동을 정한다.
- **exponential backoff metadata**: 시도 횟수가 늘수록 더 오래 기다리도록 현재 시도,
  대기 시간, 다음 실행 가능 시각을 기록한다.
- **max attempts**: unit 하나를 실행할 수 있는 최대 횟수다. 한도를 넘으면 최종 실패
  또는 dead-letter 상태로 바꾼다.
- **dead-letter metadata**: workload, payload 위치, 총 시도 횟수, 마지막 오류,
  실패 시각을 기록한다. 원인 분석과 수동 재처리에 사용한다.
- **execution attempt records**: 최초 실행과 재시도를 각각 기록한다. 실행한 worker,
  시작·종료 시각, 결과 상태, 오류 정보를 남긴다.

Product handler는 오류를 분류할 수 있다.

```python
raise RetryableExecutionError(...)
raise PermanentExecutionError(...)
raise RateLimitedExecutionError(retry_after=60)
```

---

# 10. Continuous 작업 처리 흐름

## 10.1 Partition 찾기

Product implementation이 partition 목록을 제공한다.

```python
async def discover_partitions():
    return [
        Partition(id="0", payload={...}),
        Partition(id="1", payload={...}),
    ]
```

Runtime은 partition의 의미를 알 필요가 없다.

## 10.2 Partition 처리 권한

```text
Runtime Instance
   -> register
   -> acquire partition lease
   -> run product handler
   -> heartbeat
   -> renew lease
```

Lease가 만료되면 다른 runtime instance가 partition을 획득할 수 있다.

## 10.3 원하는 상태와 실제 상태 맞추기

Continuous Reconciler는 다음을 비교한다.

```text
desired partitions
actual partitions
active runtime instances
current leases
expired leases
```

그리고 다음을 수행한다.

- missing partition 생성
- orphan partition 비활성화
- expired lease 회수
- unassigned partition 재할당
- terminating instance의 partition drain

## 10.4 Worker 수 조절

Continuous runtime pool은 초기에는 다음을 사용한다.

```text
minimum capacity
+ CPU/network-based GCP native autoscaling
+ partition assignment reconciler
```

향후 custom metric을 사용할 수 있다.

```text
unassigned_partition_count
partition_lag
events_per_runtime
connection_count
```

---

# 11. GCP 연결 코드

`runtime-gcp` package는 GCP integration을 제공한다.

```text
distributed_runtime.gcp.pubsub
distributed_runtime.gcp.compute
distributed_runtime.gcp.storage
distributed_runtime.gcp.secrets
distributed_runtime.gcp.monitoring
distributed_runtime.gcp.sql
```

이는 멀티클라우드 추상화 계층이 아니다. GCP SDK 호출을 runtime core에서 분리하기 위한 adapter다.

---

# 12. Runtime 기능별 GCP 서비스

| Runtime Capability | GCP Service |
|---|---|
| Finite execution queue | Pub/Sub |
| Runtime pool | Compute Engine MIG |
| Runtime image | Artifact Registry |
| Operational state | Cloud SQL PostgreSQL |
| Large artifacts | Cloud Storage |
| Secrets | Secret Manager |
| Metrics | Cloud Monitoring |
| Logs | Cloud Logging |
| API deployment | Cloud Run |
| Scheduled reconciliation | Cloud Scheduler → single-task Cloud Run Job |
| Distributed coordination | Cloud SQL PostgreSQL transaction/row state |

---

# 13. DB에 저장할 runtime 상태

## 13.1 Finite State

```text
workload_runs
execution_units
execution_attempts
dead_letter_records
runtime_pools
runtime_instances
artifacts
```

## 13.2 Continuous State

```text
continuous_deployments
partitions
partition_leases
runtime_instances
heartbeats
checkpoints
incidents
```

Core tables는 product-specific field를 포함하지 않는다.

Domain metadata는 JSON field 또는 product-owned table에 저장한다.

---

# 14. 데이터 소유권

Runtime library는 작업을 조정하는 metadata만 소유한다. 아래 목록은 runtime과 제품의
경계를 설명하는 예시이며 모든 데이터를 나열한 것은 아니다.

Runtime-owned data 예시:

- run status
- execution status
- attempts
- leases
- heartbeat
- runtime pool state
- dead-letter metadata
- artifact reference
- runtime metrics

Product-owned data 예시:

- 상품 정보
- 가격 snapshot
- 틱 데이터
- 체결 데이터
- order book
- normalized business entities
- product-specific checkpoints
- product-specific analytics

이 경계가 중요하다.

```text
Runtime DB
  != Product Data Warehouse
```

---

# 15. 제품 저장소와 연결

Library는 storage를 강제하지 않는다.

Product handler는 직접 결과를 저장하거나 runtime sink interface를 사용할 수 있다.

```python
class EventSink(Protocol):
    async def emit(self, event: EventEnvelope) -> None:
        ...
```

제공 가능한 기본 sink:

```text
PubSubSink
CloudStorageSink
BigQuerySink
CompositeSink
```

Product는 custom sink를 구현할 수 있다.

```python
registry.register_sink(
    "product-event-sink",
    ProductEventSink(),
)
```

---

# 16. Handler에 전달할 실행 정보

Finite handler context:

```python
class ExecutionContext:
    run_id: str
    execution_id: str
    attempt: int
    logger: Logger
    metrics: Metrics
    artifacts: ArtifactClient
    secrets: SecretClient
    cancellation: CancellationToken
```

Continuous handler context:

```python
class PartitionContext:
    deployment_id: str
    partition_id: str
    lease: LeaseHandle
    logger: Logger
    metrics: Metrics
    sinks: SinkRegistry
    secrets: SecretClient
    cancellation: CancellationToken
```

Product code는 GCP SDK를 직접 사용하지 않고 context 기능을 사용할 수 있다. 다만 필요하면 직접 사용도 허용한다.

---

# 17. Worker 수 자동 조절

## 17.1 Finite Runtime Pools

GCP native MIG autoscaler를 사용한다.

Scaling signal:

```text
Pub/Sub undelivered messages
```

Configuration:

```yaml
runtime_pools:
  browser:
    mode: finite
    min_replicas: 0
    max_replicas: 20
    target_messages_per_instance: 5
```

## 17.2 Continuous Runtime Pools

초기 구성:

```yaml
runtime_pools:
  stateful-stream:
    mode: continuous
    min_replicas: 2
    max_replicas: 10
    target_cpu_utilization: 0.60
```

Partition Reconciler가 workload distribution을 담당한다.

## 17.3 앞으로 추가할 custom metric

다음 지표가 필요해질 때 custom metric을 추가한다.

```text
unassigned_partition_count
partition_processing_lag
effective_backlog
active_connections_per_instance
runtime_error_rate
```

자체 VM autoscaler는 마지막 수단으로 둔다.

---

# 18. 실패를 처리하는 규칙

## 18.1 Finite

| 실패 상황 | Runtime 동작 |
|---|---|
| Runtime process 종료 | Pub/Sub이 메시지를 다시 전달 |
| VM 종료 | Pub/Sub이 메시지를 다시 전달 |
| 재시도 가능한 오류 | 정해진 시각 뒤 다시 실행 |
| 영구 오류 | 실패 또는 dead-letter 상태로 변경 |
| 같은 메시지 중복 전달 | idempotency key로 이미 처리했는지 확인 |
| 결과 DB 저장 실패 | 성공 상태로 확정하지 않음 |

## 18.2 Continuous

| 실패 상황 | Runtime 동작 |
|---|---|
| Runtime process 종료 | lease가 만료되면 다른 worker가 인수 |
| VM 종료 | partition을 다른 worker에 배정 |
| 외부 연결 끊김 | 제품 handler가 다시 연결 |
| Heartbeat 중단 | lease를 회수하고 다시 배정 |
| Sink 장애 | 횟수와 용량 한도 안에서 재시도하고 임시 저장 |
| Partition 쏠림 | reconciler가 일부 partition을 이동 |

---

# 19. 로그와 metric 규칙

Runtime은 공통 metric을 제공한다.

## 19.1 Finite Metrics

```text
runtime_execution_submitted_total
runtime_execution_running
runtime_execution_duration_seconds
runtime_execution_success_total
runtime_execution_retry_total
runtime_execution_dead_letter_total
runtime_queue_backlog
```

## 19.2 Continuous Metrics

```text
runtime_partition_total
runtime_partition_assigned
runtime_partition_unassigned
runtime_partition_lease_expired_total
runtime_instance_heartbeat_age
runtime_partition_lag
runtime_events_emitted_total
```

Product는 domain metric을 별도로 추가한다.

```text
product_price_snapshots_total
product_stream_events_total
product_sequence_gap_total
```

---

# 20. 사용자용 테스트 도구

`runtime-testing`은 product 개발자가 local test를 작성할 수 있도록 한다.

```python
from distributed_runtime.testing import FiniteRuntimeTestKit

runtime = FiniteRuntimeTestKit()
runtime.register(snapshot_handler)

result = await runtime.execute(
    handler="catalog.snapshot",
    payload={"product_id": "A"},
)
```

Continuous test:

```python
runtime = ContinuousRuntimeTestKit()
runtime.assign_partition("partition-0")

await runtime.run_for(seconds=5)
```

제공 기능:

- in-memory queue
- fake lease manager
- fake secret provider
- execution recording
- retry simulation
- cancellation simulation
- heartbeat timeout simulation

---

# 21. 배포 방법

Runtime library는 두 방식으로 제공한다.

## 21.1 제품에 설치하는 SDK

Product process가 SDK를 직접 import한다.

```text
Product API
  + runtime client
```

## 21.2 별도로 배포하는 runtime component

공통 runtime image를 배포하고 product package를 plugin처럼 포함한다.

```text
Base Runtime Image
    +
Product Wheel
    +
Product Configuration
```

예시 Dockerfile:

```dockerfile
FROM runtime-worker:1.1

COPY dist/product_package.whl /tmp/
RUN pip install /tmp/product_package.whl

COPY runtime.yaml /app/runtime.yaml
```

이 모델이 product 간 재사용에 가장 적합하다.

---

# 22. 권장 저장소 분리

## Runtime Repository

```text
distributed-runtime/
├── packages/
├── deploy/
├── terraform/
├── examples/
└── docs/
```

## Product Repository

```text
resale-data-product/
├── product/
│   └── workloads/
├── runtime.yaml
└── Dockerfile
```

```text
market-data-product/
├── product/
│   └── workloads/
├── runtime.yaml
└── Dockerfile
```

Runtime repository에는 특정 쇼핑몰, 리셀 플랫폼 또는 시장 이름을 포함하지 않는다.

---

# 23. 목표 공개 API 예시

```python
from distributed_runtime import RuntimeApplication
from distributed_runtime.finite import finite_handler
from distributed_runtime.continuous import PartitionHandler

app = RuntimeApplication("example-product")
```

Finite:

```python
@finite_handler(
    app=app,
    name="snapshot.item",
    execution_class="browser",
)
async def snapshot_item(ctx, payload):
    ...
```

Continuous:

```python
@app.continuous(
    name="event.stream",
    execution_class="stateful-stream",
)
class EventStream(PartitionHandler):
    async def discover_partitions(self):
        ...

    async def run(self, ctx, partition):
        ...
```

Submission:

```python
run = await app.client.submit(
    workload="snapshot.item",
    input={"ids": ["A", "B"]},
)
```

---

# 24. 단계별 구현 범위

## Phase 1: Runtime Core

- registry
- workload model
- finite execution contract
- continuous partition contract
- configuration
- runtime context
- local testing package

## Phase 2: GCP Finite Runtime

- Pub/Sub integration
- Worker runtime
- Cloud SQL execution state
- MIG deployment
- native backlog autoscaling
- retry and dead-letter

## Phase 3: GCP Continuous Runtime

- partition registry
- lease manager
- heartbeat
- reconciler
- graceful drain
- MIG deployment
- CPU-based autoscaling

## Phase 4: Product Integration

- one finite workload product
- one continuous workload product
- shared deployment pattern
- product wheel injection
- end-to-end observability

## Phase 5: Hardening

- version compatibility
- schema migration
- workload upgrade policy
- lease fencing token
- custom metrics
- artifact cleanup
- rolling deployment safety

---

# 25. 중요한 설계 결정

## ADR-001: Runtime은 제품이 아니라 library다

**Decision:** 특정 수집 대상과 domain logic을 runtime repository에 포함하지 않는다.

**Reason:** 여러 제품이 동일한 distributed execution capability를 dependency로 재사용해야 한다.

## ADR-002: 공통 workload 종류는 두 개다

**Decision:** `FiniteWorkload`와 `ContinuousWorkload`를 core abstraction으로 제공한다.

**Reason:** batch-style execution과 stateful long-running execution은 서로 다른 lifecycle을 가진다.

## ADR-003: 제품에 치우치지 않은 이름을 쓴다

**Decision:** `crawler`, `collector`, `market`, `product`처럼 특정 제품을 떠올리게 하는
이름을 core API와 저장소 구조에서 사용하지 않는다.

**Reason:** library abstraction이 특정 use case에 종속되어 보이는 것을 방지한다.

## ADR-004: 제품 데이터는 제품이 소유한다

**Decision:** Runtime은 orchestration metadata만 소유한다.

**Reason:** product-specific schema와 data lifecycle이 runtime에 결합되는 것을 방지한다.

## ADR-005: 먼저 GCP 기본 autoscaling을 사용한다

**Decision:** VM lifecycle은 GCP MIG native autoscaler에 맡긴다.

**Reason:** Runtime은 workload semantics와 partition coordination에 집중해야 한다.

## ADR-006: 제품 코드는 package로 주입한다

**Decision:** 공통 runtime image에 product wheel과 configuration을 추가하는 배포 모델을 지원한다.

**Reason:** runtime 구현을 복제하지 않고 서로 다른 제품에서 재사용할 수 있다.

## ADR-007: 결과 종합은 제품이 소유한다

**Decision:** Runtime은 unit별 결과를 저장하고 run 단위로 조회하는 기능만 제공한다.
결과를 모아 의미 있는 값으로 만드는 종합 로직은 제품 코드가 수행하며, runtime이
실행하는 별도의 종합 단계는 두지 않는다.

**Reason:** 종합 방식은 제품 데이터의 의미에 의존한다. Runtime이 종합 단계를
실행하려면 결과 전달 방식과 종합 실패 정책을 계약으로 새로 정해야 하고, "모든
unit이 끝나면 run도 끝난다"는 완료 의미가 복잡해진다.

---

# 26. 완료 기준

## Library

> 새 제품은 runtime package를 설치하고 제품 handler와 설정만 작성해 GCP 분산 실행
> 환경을 만들 수 있어야 한다.

## Finite Workload

> 제품이 planner와 handler를 등록하면 runtime이 unit 생성, Pub/Sub 전송, 재시도,
> 최종 실패, worker 수 조절, 상태 기록을 맡아야 한다.

## Continuous Workload

> 제품이 partition 발견 코드와 handler를 등록하면 runtime이 lease, heartbeat,
> 장애 복구, 재배치, 안전한 종료를 맡아야 한다.

## Isolation

> Runtime core 저장소에는 특정 쇼핑몰, 거래소, 리셀 플랫폼, 시장 데이터 수집기
> 전용 구현이 없어야 한다.
