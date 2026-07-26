# Core contracts: 공통 규칙

`distributed_runtime.core`에는 Finite와 Continuous 작업이 함께 사용하는 기본 타입이
있다. ID, 상태, 오류, 시간, 중단, 로그처럼 어떤 작업에도 필요한 기능을 모아 둔다.

이 package는 GCP SDK나 데이터베이스 client를 사용하지 않는다.

## 형식을 검사하는 ID

`RunId`, `ExecutionId` 같은 identifier는 일반 문자열처럼 보이지만 만들 때 형식을
확인한다. 한 번 만든 ID의 값은 바꿀 수 없고 문자열 값으로 정렬할 수 있다.

허용하는 문자:

```text
^[A-Za-z0-9][A-Za-z0-9._:/-]*$
```

모든 ID가 따르는 규칙:

- 빈 문자열 금지
- 앞뒤 공백 금지
- 최대 255자
- 첫 문자는 ASCII 영숫자
- 생성 뒤 값 변경 불가
- 문자열 값 기준 정렬 가능
- `str(identifier)`로 원래 문자열 확인
- 각 ID 클래스의 `parse(value)`로 문자열 변환

| 타입 | 무엇을 구분하는가 |
|---|---|
| `WorkloadId` | application 또는 workload |
| `RevisionId` | planner, handler, runtime의 배포 버전 |
| `RunId` | 한 번 제출한 Finite 요청 |
| `ExecutionId` | Finite 실행 단위 하나 |
| `DeploymentId` | Continuous 배포 하나 |
| `PartitionId` | Continuous 처리 구역 하나 |
| `RuntimeInstanceId` | worker process 또는 VM 하나 |
| `ArtifactId` | 내용이 고정된 외부 파일 하나 |

형식이 잘못되면 `InvalidIdentifierError`가 발생한다. 오류의 `details`에는 어떤 종류의
ID였는지와 실제 입력값이 들어간다.

## 저장소와 API가 함께 쓰는 상태 이름

상태 enum은 문자열로 저장하고 전송할 수 있는 `StrEnum`이다.

### `WorkloadMode`

- `finite`
- `continuous`

### `RunStatus`

```text
pending → planning → running → succeeded | failed | cancelled
```

현재는 허용할 상태 이름만 정의한다. 예를 들어 `running`에서 `succeeded`로 바꾸는
상태 전이 코드는 아직 없다. 후속 DB와 API가 같은 문자열을 사용하도록 미리 정한 것이다.

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

## 코드가 판단할 수 있는 오류

`distributed_runtime.core.errors`의 오류는 모두 `RuntimeContractError`를 상속한다.
사람이 읽을 메시지뿐 아니라 프로그램이 다음 행동을 정할 정보도 함께 담는다.

하지만 모든 잘못된 입력이 `RuntimeContractError`인 것은 아니다.

- registry 등록 실패: `RegistrationError`
- 일반 값 검증 실패: `ValueError` 또는 `TypeError`

API나 worker에서 오류 형식을 하나로 맞출 때는 이 표준 예외도 함께 처리해야 한다.

구조화 오류에 공통으로 들어가는 값:

- `code`: 프로그램이 비교할 수 있는 고정된 오류 코드
- `kind`: 오류 종류를 나타내는 `ErrorKind`
- `message`: 사람이 읽을 설명
- `retryable`: 자동으로 다시 시도해도 되는지
- `details`: 만든 뒤 바뀌지 않는 추가 정보

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

| 오류 | 자동 재시도 | 쉬운 뜻 |
|---|---:|---|
| `InvalidIdentifierError` | 아니오 | ID 형식이 잘못됨 |
| `InvariantViolationError` | 아니오 | 반드시 지켜야 할 내부 규칙이 깨짐 |
| `PlanningDriftError` | 아니오 | 이전 계획과 다시 만든 계획이 다름 |
| `CancelledExecutionError` | 아니오 | 실행이 취소됨 |
| `RetryableExecutionError` | 예 | 잠시 뒤 다시 시도할 수 있는 제품 오류 |
| `PermanentExecutionError` | 아니오 | 다시 시도해도 해결되지 않는 제품 오류 |
| `RateLimitedExecutionError` | 예 | 지정한 시간만큼 기다린 뒤 재시도할 오류 |
| `UnsupportedVersionError` | 아니오 | 현재 코드가 읽을 수 없는 형식 버전 |

`RateLimitedExecutionError`에는 0초보다 큰 대기 시간을 넣어야 한다. 초 단위 값은
`details["retry_after_seconds"]`에서 확인할 수 있다.

### 오류의 추가 정보에 넣을 수 있는 값

허용:

- 문자열, 정확한 정수, boolean, 유한한 float, `None`
- list/tuple
- 문자열 key를 가진 key-value 데이터

금지:

- 문자열이 아닌 key
- NaN/Infinity
- set 또는 임의의 Python 객체

특별하게 만든 mapping이 `False`처럼 평가되더라도 실제 항목이 있으면 버리지 않는다.

## 테스트할 수 있는 시각과 마감 시간

코드에서 `datetime.now()`를 직접 부르면 시간 관련 테스트가 어려워진다. `Clock`
규칙을 사용하면 운영에서는 실제 시각을, 테스트에서는 원하는 시각을 넣을 수 있다.

```python
class Clock(Protocol):
    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...
```

- `SystemClock`: 실제 UTC 시각과 process의 단조 증가 시각 사용
- `FakeClock`: 테스트 코드가 원하는 만큼 시간을 이동
- `Deadline`: 시스템 시각 변경의 영향을 받지 않는 단조 증가 시각으로 만료 계산

```python
clock = FakeClock()
deadline = Deadline.after(clock, 30)
clock.advance(10)
assert deadline.remaining_seconds() == 20
```

상위 작업에 deadline이 있으면 상위와 하위 deadline 중 더 빠른 시각을 사용한다.
서로 다른 clock으로 만든 deadline을 섞으면 오류다.

시간 값에는 boolean, `NaN`, `Infinity`, 음수를 넣을 수 없다.

## 실행 코드에 중단을 알리기

`CancellationSource`는 중단 상태를 바꾸는 쪽에서 사용한다. handler처럼 중단 여부만
확인하는 코드는 `CancellationToken`을 받는다.

```python
source = CancellationSource()
token = source.token

source.cancel("deployment draining")
token.raise_if_cancelled()
```

동작 규칙:

- `cancel()`은 처음 상태를 바꾼 호출만 `True`를 반환한다.
- 취소 이유는 처음 전달한 값을 유지한다.
- 상위 token이 취소되면 하위 token도 취소된다.
- 하위 token 취소는 상위 또는 다른 하위 token에 영향을 주지 않는다.
- deadline이 지나면 이유는 `"deadline exceeded"`다.
- source와 deadline은 같은 clock 객체를 사용해야 한다.

## 진행 중인 일을 정리하고 종료하기

상태:

```text
running → draining → stopped
```

`begin(grace_seconds)`을 호출하면 새 작업을 받지 않는 `draining` 상태로 바뀐다.
처음 호출할 때만 종료 deadline을 만든다. 반복 호출해도 기다리는 시간이 늘어나지
않는다.

`poll()`은 기다릴 수 있는 시간이 끝났는지 확인한다. 시간이 끝나면 실행 취소 이유를
설정하고 `stopped`로 바꾼다. `stop()`은 기다리지 않고 즉시 중단한다.

후속 worker는 운영체제의 SIGTERM을 받으면 `begin()`을 호출해야 한다. event loop에서
`poll()` 또는 같은 역할의 검사를 반복해 종료 deadline을 지켜야 한다.

## 검색하기 쉬운 공통 로그 정보

`LogContext`는 모든 로그에 함께 넣을 key-value 정보를 담는다. 새 값을 묶으면 기존
객체를 바꾸지 않고 새로운 context를 만든다.

```python
base = LogContext({"service": "finite-worker"})
execution = base.bind(run_id="run:1", attempt=2)

with execution.activate():
    record = execution.as_dict("handler.started", handler="resize")
```

field가 지켜야 할 규칙:

- key는 비어 있지 않은 문자열
- `event`는 예약 key
- value는 JSON의 단일 값
- float는 `NaN`이나 `Infinity`가 아닌 유한한 값

병합 우선순위:

```text
현재 활성 context < instance fields < as_dict 호출 fields < event
```

내부에서 `ContextVar`를 사용한다. 여러 async task가 동시에 실행되어도 각 task의
로그 정보가 섞이지 않는다. `with` 블록이 끝나면 이전 context로 돌아간다.
