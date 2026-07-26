# 빠른 시작

이 문서는 현재 구현된 계약 계층을 가장 짧게 사용하는 방법을 설명한다.
아직 worker나 GCP adapter는 없으므로, 예제의 목표는 workload를 **등록하고 계약을
검증하는 것**이다.

## 요구 사항

- Python 3.12 이상
- `uv` 권장

개발 환경 설치:

```bash
uv sync --all-groups
```

패키지 import 확인:

```bash
uv run python -c \
  "from distributed_runtime import RuntimeApplication; print(RuntimeApplication('sample'))"
```

## Application 생성

제품 통합의 최상위 객체는 `RuntimeApplication`이다.

```python
from distributed_runtime import RuntimeApplication

app = RuntimeApplication("sample-service")
```

application 이름은 workload, artifact, registry identity의 namespace로 사용된다.
생성된 `app.registry`에 Finite/Continuous workload와 sink를 등록한다.

## 최소 Finite workload

Finite planner는 immutable request를 받아 async iterator로 `ExecutionUnit`을 생성한다.

```python
from collections.abc import AsyncIterator, Mapping

from distributed_runtime import RuntimeApplication
from distributed_runtime.core import JsonValue, WorkloadMode
from distributed_runtime.finite import (
    ExecutionContext,
    ExecutionUnit,
    WorkloadRequest,
)


class GreetingPlanner:
    name = "greeting"
    version = "1.0.0"
    mode = WorkloadMode.FINITE

    async def __call__(
        self,
        request: WorkloadRequest,
    ) -> AsyncIterator[ExecutionUnit]:
        yield ExecutionUnit(
            unit_key="hello",
            handler="print-greeting",
            payload={"name": request.payload["name"]},
            execution_class="default",
        )


async def greeting_handler(
    context: ExecutionContext,
    payload: Mapping[str, JsonValue],
) -> JsonValue:
    context.cancellation.raise_if_cancelled()
    return {"message": f"hello {payload['name']}"}


app = RuntimeApplication("sample-service")
app.registry.register_finite(
    name="greeting",
    semantic_version="1.0.0",
    execution_class="default",
    planner=GreetingPlanner(),
    handler=greeting_handler,
)
```

등록 시 다음 조건이 검증된다.

- 등록 name과 planner의 `name`이 동일하다.
- 등록 version과 planner의 `version`이 동일하다.
- planner mode가 `WorkloadMode.FINITE`다.
- semantic version이 ASCII SemVer 2.0 형식이다.
- 동일한 application/name/version/mode가 중복되지 않는다.

이 코드는 unit을 실제 queue에 발행하거나 handler를 실행하지 않는다. 계획을
검증하려면 `validate_plan()`을 명시적으로 호출한다.

## 계획 검증

```python
from distributed_runtime.core import RevisionId, RunId, WorkloadId
from distributed_runtime.finite import WorkloadRequest, validate_plan

request = WorkloadRequest(
    run_id=RunId("run:demo"),
    workload_id=WorkloadId("greeting"),
    planner_revision=RevisionId("planner:v1"),
    execution_revision=RevisionId("handler:v1"),
    payload={"name": "Codex"},
)

units = await validate_plan(request, GreetingPlanner())
```

`validate_plan()`은 다음을 확인한다.

- `unit_key` 중복 여부
- payload의 canonical SHA-256
- planner 출력 순서
- 이전 durable planning record와의 drift
- cooperative cancellation

## 최소 Continuous workload

Continuous workload는 원하는 partition 집합을 발견하고 각 partition을 lease 하에서
처리하는 계약이다.

```python
from collections.abc import Sequence

from distributed_runtime import RuntimeApplication
from distributed_runtime.continuous import Partition, PartitionContext
from distributed_runtime.core import PartitionId, WorkloadMode


class EventStream:
    name = "event-stream"
    version = "1.0.0"
    mode = WorkloadMode.CONTINUOUS

    async def discover_partitions(self) -> Sequence[Partition]:
        return [
            Partition(
                partition_id=PartitionId("partition:0"),
                payload={"topic": "events-0"},
            ),
            Partition(
                partition_id=PartitionId("partition:1"),
                payload={"topic": "events-1"},
            ),
        ]

    async def run_partition(
        self,
        context: PartitionContext,
        partition: Partition,
    ) -> None:
        context.cancellation.raise_if_cancelled()
        # 실제 polling과 event emission은 제품/GCP adapter가 구현한다.


app = RuntimeApplication("sample-service")
app.registry.register_continuous(
    name="event-stream",
    semantic_version="1.0.0",
    execution_class="stream-default",
    workload=EventStream(),
)
```

실제 partition 정규화에는 `discover_partitions()` helper를 사용한다. helper는 결과를
`PartitionId` 순으로 정렬하고 중복 ID를 거부한다.

## 다음 문서

- 실행 identity와 drift가 중요하면 [Finite workload](concepts/finite-workloads.md)
- lease/fencing이 중요하면 [Continuous workload](concepts/continuous-workloads.md)
- 전송 payload를 만들려면 [Envelope와 artifact](reference/envelopes-and-artifacts.md)
- pool 설정을 만들려면 [Configuration](reference/configuration.md)
