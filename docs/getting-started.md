# 빠른 시작

이 문서는 현재 코드를 직접 사용해 보는 가장 짧은 예제다.

아직 실제 worker나 GCP 연결 코드는 없다. 여기서는 다음 두 가지만 해본다.

1. workload를 registry에 등록한다.
2. 입력과 planner 결과가 정해진 규칙에 맞는지 확인한다.

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

제품 코드에서는 `RuntimeApplication`으로 시작한다.

```python
from distributed_runtime import RuntimeApplication

app = RuntimeApplication("sample-service")
```

application 이름은 서로 다른 제품의 workload가 섞이지 않게 구분하는 값이다.
artifact를 만들 때도 같은 application 이름을 넣는 것을 권장한다.

생성된 `app.registry`에는 Finite workload, Continuous workload, sink를 등록할 수
있다.

## 최소 Finite workload

Finite planner는 하나의 요청을 받아 작은 작업인 `ExecutionUnit`을 하나씩 만든다.
요청 값은 planner 실행 중 바뀌지 않는다.

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

등록하면 다음 내용을 바로 확인한다.

- 등록할 때 적은 이름과 planner의 `name`이 같다.
- 등록할 때 적은 버전과 planner의 `version`이 같다.
- planner가 자신을 Finite 작업으로 선언했다.
- 버전이 `1.0.0` 같은 ASCII SemVer 형식이다.
- 같은 application, 이름, 버전, 실행 종류가 이미 등록되지 않았다.

여기까지 실행해도 작업이 queue로 보내지거나 handler가 실행되지는 않는다.
planner가 만든 결과를 확인하려면 `validate_plan()`을 따로 호출한다.

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

`validate_plan()`은 항상 다음 동작을 수행한다.

- planner의 결과를 끝까지 읽어 tuple로 반환한다.
- 각 payload를 정해진 JSON 형식으로 바꾼 뒤 SHA-256을 계산한다.
- 같은 `unit_key`가 두 번 나오면 오류를 발생시킨다.

이 함수가 DB를 직접 읽지는 않는다. 호출자가 DB에서 읽은 이전 `PlanningRecord`를
`expected=`로 전달한 경우에만 unit 개수, 순서, key, SHA-256이 이전 계획과 같은지
비교한다. `cancellation=` token을 전달한 경우에는 계획을 읽는 중간에도 중단 요청을
확인한다.

## 최소 Continuous workload

Continuous workload는 계속 처리해야 할 대상을 여러 partition으로 나눈다. 실제
worker는 각 partition의 일정 시간 처리 권한인 lease를 얻은 뒤 작업한다.

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

partition 목록을 확인하려면 `discover_partitions()` helper를 사용한다. 이 함수는
목록을 `PartitionId` 순으로 정렬한다. 같은 ID가 두 번 나오면 오류를 발생시킨다.

## 다음 문서

- 처음 보는 용어가 있으면 [쉬운 용어 설명](glossary.md)
- 실행 ID와 계획 변경 검사가 궁금하면 [Finite workload](concepts/finite-workloads.md)
- lease와 오래된 worker 차단이 궁금하면
  [Continuous workload](concepts/continuous-workloads.md)
- 전송 메시지를 만들려면 [Envelope와 artifact](reference/envelopes-and-artifacts.md)
- worker 묶음과 DB 연결 수를 설정하려면 [Configuration](reference/configuration.md)
