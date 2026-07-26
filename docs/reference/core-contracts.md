# Core contracts 레퍼런스

`distributed_runtime.core`는 Finite, Continuous, adapter가 공유하는 가장 낮은 계층이다.
이 모듈은 GCP SDK나 데이터베이스 client에 의존하지 않는다.

## Identifier

모든 identifier는 frozen/slots/order dataclass다.

허용 형식:

```text
^[A-Za-z0-9][A-Za-z0-9._:/-]*$
```

공통 규칙:

- 빈 문자열 금지
- 선행·후행 공백 금지
- 최대 255자
- 첫 문자는 ASCII 영숫자
- 생성 뒤 값 변경 불가
- 문자열 값 기준 정렬 가능
- `str(identifier)`는 원래 value 반환
- `ConcreteIdentifier.parse(value)` 지원

| 타입 | 의미 |
|---|---|
| `WorkloadId` | application 또는 workload의 안정적인 ID |
| `RevisionId` | planner/handler/runtime revision |
| `RunId` | Finite submission |
| `ExecutionId` | Finite execution unit |
| `DeploymentId` | Continuous deployment |
| `PartitionId` | Continuous partition |
| `RuntimeInstanceId` | worker process/VM |
| `ArtifactId` | immutable artifact |

잘못된 값은 `InvalidIdentifierError`이며 details에 `identifier_kind`와 입력값을 담는다.

## 상태 enum

모든 enum은 문자열 직렬화가 가능한 `StrEnum`이다.

### `WorkloadMode`

- `finite`
- `continuous`

### `RunStatus`

```text
pending → planning → running → succeeded | failed | cancelled
```

상태 전이 엔진은 아직 구현되지 않았다. enum은 저장소와 API가 공유할 vocabulary다.

### `ExecutionStatus`

- `pending`
- `ready`
- `running`
- `retry_scheduled`
- `succeeded`
- `failed`
- `dead_lettered`
- `cancelled`

### `DeploymentStatus`

- `pending`
- `active`
- `draining`
- `stopped`
- `failed`

### `PartitionStatus`

- `unassigned`
- `assigned`
- `running`
- `blocked_sink`
- `failed`
- `inactive`

## 구조화 오류

`distributed_runtime.core.errors`가 정의하는 구조화 오류는
`RuntimeContractError`를 상속한다.

모든 public validation 실패가 이 계층을 사용하는 것은 아니다. registry 등록 실패는
`RegistrationError`, 일반 value-object와 Finite/Continuous 계약의 잘못된 입력은
`ValueError` 또는 `TypeError`가 될 수 있다. 따라서 외부 boundary에서 오류를
정규화하려면 구조화 오류뿐 아니라 해당 API가 명시한 표준 예외도 처리해야 한다.

공통 속성:

- `code`: 안정적인 machine-readable string
- `kind`: `ErrorKind`
- `message`: 비어 있지 않은 진단
- `retryable`: runtime retry 가능 여부
- `details`: 재귀적으로 immutable한 JSON-compatible mapping

```python
try:
    ...
except RuntimeContractError as error:
    diagnostic = error.as_dict()
```

`as_dict()` 결과:

```json
{
  "code": "retryable_execution",
  "kind": "retryable_execution",
  "message": "temporary upstream failure",
  "retryable": true,
  "details": {}
}
```

| 오류 | 기본 retry | 의미 |
|---|---:|---|
| `InvalidIdentifierError` | 아니오 | identifier lexical contract 위반 |
| `InvariantViolationError` | 아니오 | runtime 내부 불변식 위반 |
| `PlanningDriftError` | 아니오 | durable plan과 retry plan 불일치 |
| `CancelledExecutionError` | 아니오 | 실행 취소 |
| `RetryableExecutionError` | 예 | 자동 재시도 가능한 제품 오류 |
| `PermanentExecutionError` | 아니오 | 자동 재시도하면 안 되는 제품 오류 |
| `RateLimitedExecutionError` | 예 | 최소 retry delay가 있는 오류 |
| `UnsupportedVersionError` | 아니오 | 해석할 수 없는 contract version |

`RateLimitedExecutionError`는 positive `timedelta`를 요구하고
`retry_after_seconds`를 details에 추가한다.

### Error details 규칙

허용:

- string, exact integer, boolean, finite float, `None`
- list/tuple
- 문자열 key를 가진 mapping

금지:

- 비문자 mapping key
- NaN/Infinity
- set, arbitrary object

입력 mapping이 비어 있지 않지만 falsey하게 동작하는 custom mapping이어도 버리지 않고
명시적으로 보존한다.

## Clock와 Deadline

`Clock` Protocol:

```python
class Clock(Protocol):
    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...
```

- `SystemClock`: UTC wall clock와 process monotonic clock 사용
- `FakeClock`: 테스트가 명시적으로 시간을 전진
- `Deadline`: monotonic expiry만 사용

```python
clock = FakeClock()
deadline = Deadline.after(clock, 30)
clock.advance(10)
assert deadline.remaining_seconds() == 20
```

parent deadline을 전달하면 더 이른 expiry가 선택된다. parent와 child가 다른 clock
객체를 사용하면 실패한다. timeout, expiry, advance에 boolean/NaN/Infinity/음수를
허용하지 않는다.

## Cancellation

`CancellationSource`는 쓰기 권한, `CancellationToken`은 읽기 권한이다.

```python
source = CancellationSource()
token = source.token

source.cancel("deployment draining")
token.raise_if_cancelled()
```

특성:

- `cancel()`은 최초 호출만 `True`
- 취소 이유는 최초 값을 유지
- parent 취소는 child에 전파
- child 취소는 parent/sibling에 역전파되지 않음
- deadline 만료는 `"deadline exceeded"`
- source/deadline은 같은 clock domain 사용

## GracefulShutdown

상태:

```text
running → draining → stopped
```

`begin(grace_seconds)`는 최초 호출에만 deadline을 만든다. draining 중 다시 호출해도
grace window가 늘어나지 않는다.

`poll()`은 deadline이 만료되면 cancellation reason을 설정하고 stopped로 전환한다.
`stop()`은 즉시 cancellation과 stopped 상태를 설정한다.

후속 worker는 SIGTERM을 `begin()`에 연결하고, poll 또는 event loop를 통해 grace
deadline을 관찰해야 한다.

## LogContext

`LogContext`는 immutable structured field 집합이다.

```python
base = LogContext({"service": "finite-worker"})
execution = base.bind(run_id="run:1", attempt=2)

with execution.activate():
    record = execution.as_dict("handler.started", handler="resize")
```

field 규칙:

- key는 non-empty string
- `event`는 예약 key
- value는 exact JSON scalar
- float는 finite

병합 우선순위:

```text
현재 활성 context < instance fields < as_dict 호출 fields < event
```

`ContextVar`를 사용하므로 async task별 활성 context가 분리되고 context manager 종료
후 이전 값으로 복원된다.
