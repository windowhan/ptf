# GCP Distributed Workload Runtime
## Library-Oriented Architecture Document

**Status:** Final Draft  
**Version:** 1.1  
**Target platform:** Google Cloud Platform  
**Distribution model:** Reusable Python library and deployable runtime components

---

# 1. Overview

본 프로젝트는 특정 크롤러, 거래소, 데이터 수집기 또는 제품에 종속된 애플리케이션이 아니다.

목표는 다른 제품이 Python dependency로 가져와 다음 두 종류의 workload를 GCP에서 분산 실행할 수 있게 하는 **재사용 가능한 distributed workload runtime**을 제공하는 것이다.

1. **Finite Workload**
   - 입력이 유한함
   - 여러 독립 실행 단위로 분할 가능
   - 실행 후 완료됨
   - queue backlog 기반 autoscaling에 적합

2. **Continuous Workload**
   - 장시간 또는 무기한 실행
   - 외부 연결이나 로컬 상태를 유지
   - partition 또는 shard ownership 필요
   - heartbeat, lease, rebalance가 필요

쇼핑몰 크롤링은 Finite Workload의 한 사용 사례이며, 실시간 시장 데이터 수집은 Continuous Workload의 한 사용 사례다. 이 use case들은 core library에 포함되지 않고 별도 product package에서 runtime을 사용한다.

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

# 2. Primary Design Goal

다른 프로젝트가 다음과 같이 dependency를 추가해 사용할 수 있어야 한다.

```toml
[project]
dependencies = [
  "distributed-runtime[gcp]"
]
```

Product code는 GCP resource lifecycle이나 worker orchestration을 직접 구현하지 않는다.

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

Library는 다음을 담당한다.

- **workload registration**: workload의 이름, 버전, 실행 모드와 handler를 등록하고, 실행 요청이 올바른 product 코드로 연결되도록 관리한다.
- **execution contract**: Finite Workload의 실행 단위와 Continuous Workload의 partition이 어떤 입력, context, 결과 및 생명주기를 따라야 하는지 공통 인터페이스로 정의한다.
- **serialization**: 요청, payload, 결과, event를 Pub/Sub 메시지나 저장소에 기록할 수 있는 형식으로 변환하고 다시 복원한다.
- **retries**: 일시적 오류가 발생한 실행을 backoff와 최대 시도 횟수 정책에 따라 재시도하고, 한도를 넘으면 실패 또는 dead-letter 상태로 전환한다.
- **idempotency**: 메시지 중복 전달이나 재시도가 발생해도 동일한 실행 단위의 부수 효과가 중복으로 적용되지 않도록 식별하고 제어한다.
- **leases**: 특정 runtime instance에 작업 또는 partition의 소유권을 제한된 시간 동안 부여하고, 만료 시 다른 instance가 인계할 수 있게 한다.
- **heartbeat**: runtime instance가 정상 동작 중임을 주기적으로 기록하여 장애를 감지하고, 필요한 경우 보유한 lease를 갱신한다.
- **partition ownership**: 하나의 partition을 동시에 하나의 유효한 runtime instance만 처리하도록 할당, 재할당 및 drain 과정을 조정한다.
- **Pub/Sub integration**: 실행 메시지의 publish, consume, ack/nack와 재전달 처리를 GCP Pub/Sub에 연결한다.
- **Worker runtime**: product handler를 로드해 실행하고, 실행 context 주입부터 상태 기록과 결과 처리까지 worker의 전체 생명주기를 관리한다.
- **graceful shutdown**: 종료 신호를 받으면 신규 작업 수신을 중단하고, 진행 중인 작업을 안전하게 마치거나 다른 instance가 인계할 수 있는 상태로 정리한다.
- **observability**: 실행 상태, 구조화 로그, metric 및 오류 정보를 수집하여 처리량, 지연, 실패와 runtime 상태를 추적할 수 있게 한다.
- **GCP deployment metadata**: 배포된 runtime의 image/version, runtime pool, instance 및 region 같은 정보를 실행 기록과 연결하여 운영과 장애 분석에 사용한다.

Product는 다음만 제공한다.

- **workload handler**: 개별 실행 단위 또는 partition에서 실제로 수행할 상품 수집, 시장 데이터 구독 등의 domain 로직을 구현한다.
- **input planner**: 하나의 Finite Workload 요청을 병렬 처리 가능한 여러 `ExecutionUnit`으로 분해하는 기준과 방법을 제공한다.
- **payload schema**: handler가 받을 domain 입력 데이터의 필드, 타입, 필수 조건 및 호환 가능한 버전을 정의한다.
- **partition discovery**: Continuous Workload가 현재 처리해야 할 partition 목록을 domain 정보에 근거해 찾아 runtime에 전달한다.
- **domain-specific result/event schema**: handler가 생성하는 결과나 event의 business 의미, 필드 구조 및 검증 규칙을 정의한다.
- **domain-specific recovery logic**: 일반적인 retry만으로 해결할 수 없는 checkpoint 복원, 외부 시스템 상태 확인, 보상 처리 등 domain 고유의 복구 절차를 구현한다.

---

# 3. Non-Goals

초기 버전은 다음을 목표로 하지 않는다.

- AWS 또는 멀티클라우드 지원
- Kubernetes abstraction
- arbitrary remote code execution
- 범용 DAG engine
- exactly-once execution 보장
- product-specific crawler 또는 collector 구현
- domain-specific storage schema 강제
- 범용 stream-processing engine 대체

---

# 4. Core Abstractions

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

Continuous Workload는 정해진 실행 단위를 모두 처리하면 종료되는 Finite Workload와 달리, 서비스가 운영되는 동안 외부 event를 구독하거나 데이터를 반복 수집하는 장기 실행 workload다. 하나의 workload는 서로 독립적으로 처리할 수 있는 여러 partition으로 나뉘며, Product는 `discover_partitions()`를 통해 현재 처리해야 할 partition 목록을 Runtime에 제공한다.

Runtime은 발견된 각 partition을 가용한 runtime instance에 할당하고 `run_partition()`을 실행한다. 이때 하나의 partition은 lease가 유효한 동안 하나의 runtime instance만 소유한다.

Lease는 특정 partition을 처리할 권한을 한 runtime instance에 일정 시간 동안만 부여하는 **만료 기한이 있는 임시 소유권**이다. 영구적인 lock과 달리 소유자가 명시적으로 해제하지 못하더라도 기한이 지나면 자동으로 무효화되며, 정상 동작 중인 instance는 heartbeat를 보내 lease를 주기적으로 갱신한다.

instance가 비정상 종료되거나 heartbeat를 보내지 못하면 lease가 만료되고, Runtime은 해당 partition을 다른 instance에 재할당한다. 이를 통해 중복 처리를 최소화하면서 장애 복구, 수평 확장 및 안전한 배포 종료를 Product 코드와 분리해 처리한다.

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

# 5. Library Packages

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

# 6. Product Integration Model

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

# 7. Runtime Registry

Library는 decorator 또는 explicit registration을 지원한다.

## 7.1 Decorator Registration

```python
from distributed_runtime import finite

@finite.handler(
    name="catalog.snapshot",
    execution_class="browser",
)
async def snapshot(ctx, payload):
    ...
```

## 7.2 Explicit Registration

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

# 8. System Architecture

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

# 9. Finite Execution Plane

## 9.1 Submission

```python
client.submit(
    workload="catalog.snapshot",
    request={
        "product_ids": ["A", "B", "C"],
    },
)
```

## 9.2 Planning

Product-provided planner returns generic `ExecutionUnit` values.

```python
async def plan(request):
    for product_id in request["product_ids"]:
        yield ExecutionUnit(
            handler="catalog.snapshot.item",
            payload={"product_id": product_id},
            execution_class="browser",
        )
```

## 9.3 Dispatch

Runtime maps `execution_class` to a Pub/Sub topic.

```text
lightweight
  -> runtime-lightweight

browser
  -> runtime-browser

compute
  -> runtime-compute
```

별도 TaskRouter service는 두지 않는다. 이 mapping은 runtime configuration으로 처리한다.

## 9.4 Processing

```text
Execution Unit
   -> Pub/Sub
   -> Runtime Worker
   -> Handler Registry
   -> Product Handler
   -> Result
```

## 9.5 Retry and Dead Letter

Runtime은 다음을 표준화한다.

- **acknowledgment deadline**: Worker가 Pub/Sub 메시지를 수신한 뒤 처리 권한을 유지할 수 있는 제한 시간이다. 처리가 끝나면 ack하고, 시간 내 완료하지 못하면 deadline을 연장하거나 메시지가 다시 전달되도록 한다.
- **retry classification**: 발생한 오류를 재시도 가능한 오류, 영구 오류, rate limit 오류 등으로 분류하여 다음 처리 방식을 결정한다.
- **exponential backoff metadata**: 재시도 횟수가 늘어날수록 대기 시간을 지수적으로 증가시키기 위해 현재 시도 횟수, 대기 시간 및 다음 실행 가능 시각을 기록한다.
- **max attempts**: 하나의 `ExecutionUnit`에 허용되는 최대 실행 횟수다. 이 횟수를 초과하면 더 이상 자동 재시도하지 않고 최종 실패 또는 dead-letter로 전환한다.
- **dead-letter metadata**: 최종 처리에 실패한 실행의 workload, payload 참조, 총 시도 횟수, 마지막 오류와 실패 시각 등을 기록하여 원인 분석과 수동 재처리에 사용한다.
- **execution attempt records**: 최초 실행과 모든 재시도를 각각 독립된 attempt로 기록한다. 각 기록에는 실행 instance, 시작·종료 시각, 결과 상태 및 오류 정보가 포함된다.

Product handler는 오류를 분류할 수 있다.

```python
raise RetryableExecutionError(...)
raise PermanentExecutionError(...)
raise RateLimitedExecutionError(retry_after=60)
```

---

# 10. Continuous Execution Plane

## 10.1 Partition Discovery

Product implementation이 partition 목록을 제공한다.

```python
async def discover_partitions():
    return [
        Partition(id="0", payload={...}),
        Partition(id="1", payload={...}),
    ]
```

Runtime은 partition의 의미를 알 필요가 없다.

## 10.2 Lease Ownership

```text
Runtime Instance
   -> register
   -> acquire partition lease
   -> run product handler
   -> heartbeat
   -> renew lease
```

Lease가 만료되면 다른 runtime instance가 partition을 획득할 수 있다.

## 10.3 Reconciliation

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

## 10.4 Scaling

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

# 11. GCP Runtime Adapter

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

# 12. GCP Resource Mapping

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
| Scheduled reconciliation | Cloud Scheduler / Compute Engine |
| Distributed coordination | PostgreSQL or Memorystore Redis |

---

# 13. Runtime State Model

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

# 14. Data Ownership

Runtime library는 orchestration metadata만 소유한다. 아래 항목은 데이터 소유권의 경계를 설명하기 위한 대표적인 예시이며, 전체 목록을 의미하지 않는다.

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

# 15. Storage Integration

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

# 16. Runtime Context

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

# 17. Autoscaling

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

## 17.3 Future Custom Metrics

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

# 18. Failure Semantics

## 18.1 Finite

| Failure | Runtime Behavior |
|---|---|
| Runtime process crash | Pub/Sub redelivery |
| VM termination | Pub/Sub redelivery |
| Retryable error | retry |
| Permanent error | failed or dead-letter |
| Duplicate delivery | idempotency check |
| Result persistence failure | success 확정 금지 |

## 18.2 Continuous

| Failure | Runtime Behavior |
|---|---|
| Runtime process crash | lease expiry |
| VM termination | partition reassignment |
| External connection loss | product handler reconnect |
| Heartbeat loss | lease recovery |
| Sink failure | bounded retry/buffering |
| Partition imbalance | reconciler rebalance |

---

# 19. Observability Contract

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

# 20. Testing Package

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

# 21. Deployment Model

Runtime library는 두 방식으로 제공한다.

## 21.1 Embedded SDK

Product process가 SDK를 직접 import한다.

```text
Product API
  + runtime client
```

## 21.2 Managed Runtime Components

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

# 22. Recommended Repository Split

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

# 23. Public API Sketch

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

# 24. MVP Scope

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

# 25. Key Architectural Decisions

## ADR-001: Runtime Is a Library, Not a Product

**Decision:** 특정 수집 대상과 domain logic을 runtime repository에 포함하지 않는다.

**Reason:** 여러 제품이 동일한 distributed execution capability를 dependency로 재사용해야 한다.

## ADR-002: Two Generic Workload Modes

**Decision:** `FiniteWorkload`와 `ContinuousWorkload`를 core abstraction으로 제공한다.

**Reason:** batch-style execution과 stateful long-running execution은 서로 다른 lifecycle을 가진다.

## ADR-003: Domain-Neutral Naming

**Decision:** `crawler`, `collector`, `market`, `product` 같은 이름을 core API와 repository structure에서 사용하지 않는다.

**Reason:** library abstraction이 특정 use case에 종속되어 보이는 것을 방지한다.

## ADR-004: Product Owns Domain Data

**Decision:** Runtime은 orchestration metadata만 소유한다.

**Reason:** product-specific schema와 data lifecycle이 runtime에 결합되는 것을 방지한다.

## ADR-005: GCP Native Autoscaling First

**Decision:** VM lifecycle은 GCP MIG native autoscaler에 맡긴다.

**Reason:** Runtime은 workload semantics와 partition coordination에 집중해야 한다.

## ADR-006: Product Package Injection

**Decision:** 공통 runtime image에 product wheel과 configuration을 추가하는 배포 모델을 지원한다.

**Reason:** runtime 구현을 복제하지 않고 서로 다른 제품에서 재사용할 수 있다.

---

# 26. Success Criteria

## Library

> 새로운 제품이 runtime package를 dependency로 추가하고, domain handler와 configuration만 작성하여 GCP 분산 실행 환경을 구성할 수 있어야 한다.

## Finite Workload

> Product가 planner와 handler를 등록하면 Runtime이 execution unit 생성, Pub/Sub dispatch, retry, dead-letter, autoscaling, 상태 기록을 처리해야 한다.

## Continuous Workload

> Product가 partition discovery와 partition handler를 등록하면 Runtime이 lease, heartbeat, failure recovery, reassignment, graceful shutdown을 처리해야 한다.

## Isolation

> Runtime core repository를 열어보았을 때 특정 쇼핑몰, 거래소, 리셀 플랫폼 또는 시장 데이터 수집기의 구현이 존재하지 않아야 한다.
