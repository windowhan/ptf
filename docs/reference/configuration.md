# Configuration: 안전한 실행 설정

`distributed_runtime.core.config`는 worker 수, 재시도, lease 시간, DB 연결 수를
설정하는 타입을 제공한다. 설정 객체를 만든 뒤에는 값이 바뀌지 않는다.

파일, 환경 변수, Secret Manager에서 설정을 읽는 코드는 아직 없다. 앞으로 adapter가
외부 값을 읽은 다음 이 문서의 타입으로 바꿔야 한다. 모든 검사를 통과한 뒤에만
runtime을 시작한다.

## 설정이 막으려는 문제

- 너무 짧은 lease 때문에 같은 partition이 여러 worker에 배정되는 문제
- 재시도 간격이나 timeout에 잘못된 값이 들어가는 문제
- Finite pool과 Continuous pool의 확장 방식을 섞는 문제
- worker가 늘어났을 때 Cloud SQL 연결 한도를 넘는 문제
- 배포가 `latest` secret을 읽어 나중에 다른 비밀번호를 사용하는 문제

## 사용할 secret version 고정하기

```python
from distributed_runtime.core import SecretReference

database_password = SecretReference(
    project_id="runtime-prod",
    secret_id="database-password",
    version="7",
)

assert database_password.resource_name == (
    "projects/runtime-prod/secrets/database-password/versions/7"
)
```

입력 규칙:

- project ID와 secret ID는 앞뒤 공백이 없는 문자열이다.
- version은 `"7"`처럼 1 이상의 10진수 문자열이다.
- 어떤 값으로 바뀔지 모르는 `"latest"`는 허용하지 않는다.
- `resource_name`에는 version까지 들어간다.

배포가 정확히 어떤 secret을 읽었는지 나중에 확인할 수 있다. 문제가 생기면 이전
배포와 같은 version으로 되돌릴 수도 있다.

## 끝나는 작업의 timeout과 재시도 설정

기본값:

| 필드 | 기본값 | 쉬운 뜻 |
|---|---:|---|
| `timeout_seconds` | 300 | 실행 시도 하나를 기다리는 시간 |
| `max_timeout_seconds` | 3600 | workload가 요청할 수 있는 최대 시간 |
| `max_attempts` | 5 | 처음 실행을 포함한 최대 시도 횟수 |
| `retry_base_seconds` | 5 | 첫 재시도 간격을 계산할 기준 |
| `retry_cap_seconds` | 300 | 재시도 간격이 넘지 못하는 최대값 |
| `claim_grace_seconds` | 120 | timeout 뒤 기존 worker를 기다리는 여유 |

worker가 실행 권한을 유지하는 전체 시간은 다음과 같다.

```text
timeout_seconds + claim_grace_seconds
```

시간과 횟수에는 1 이상의 정수만 넣을 수 있다. `True`, `1.5`, `0`은 허용하지 않는다.
기본 timeout은 최대 timeout보다 클 수 없고, 재시도 기준 시간은 재시도 최대 간격보다
클 수 없다.

## 계속되는 작업의 heartbeat와 lease 설정

기본값:

| 필드 | 기본값 | 쉬운 뜻 |
|---|---:|---|
| `heartbeat_seconds` | 15 | worker가 살아 있다고 알리는 주기 |
| `lease_seconds` | 60 | partition 처리 권한이 유지되는 시간 |
| `renew_before_expiry_seconds` | 30 | 만료 몇 초 전부터 lease를 연장할지 |
| `reconcile_seconds` | 60 | 원하는 상태와 실제 상태를 비교하는 주기 |
| `drain_seconds` | 120 | 종료 전에 진행 중인 일을 정리할 시간 |
| `takeover_seconds` | 150 | 오래 응답 없는 worker의 작업을 넘길 기준 |

필수 관계:

```text
heartbeat <= renew_before_expiry < lease <= takeover
```

이 관계가 깨지면 lease가 끝난 뒤에 연장을 시도하거나, 너무 일찍 다른 worker가 작업을
가져갈 수 있다. 모든 값은 1 이상의 정확한 정수여야 한다.

## 같은 방식으로 실행되는 worker 묶음

`RuntimePoolConfig`는 같은 설정으로 실행되는 worker 묶음 하나를 나타낸다. worker가
최대로 늘어났을 때 필요한 DB 연결 수도 함께 계산한다.

공통으로 설정하는 값:

- `mode`: Finite 또는 Continuous
- `min_replicas`: 최소 worker 수
- `max_replicas`: 최대 worker 수
- `connections_per_replica`: worker 하나가 사용할 최대 DB 연결 수

### Finite worker 묶음

```python
from distributed_runtime.core import RuntimePoolConfig, WorkloadMode

finite_pool = RuntimePoolConfig(
    mode=WorkloadMode.FINITE,
    min_replicas=0,
    max_replicas=10,
    connections_per_replica=4,
    target_messages_per_instance=20,
)
```

Finite pool 규칙:

- worker 하나가 맡을 목표 메시지 수를 반드시 설정한다.
- CPU 사용률 목표는 설정하지 않는다.
- 할 일이 없을 때 worker를 0개까지 줄일 수 있다.
- 처리되지 않은 메시지 수를 기준으로 worker 수를 바꾼다.

### Continuous worker 묶음

```python
continuous_pool = RuntimePoolConfig(
    mode=WorkloadMode.CONTINUOUS,
    min_replicas=2,
    max_replicas=5,
    connections_per_replica=3,
    target_cpu_utilization=0.65,
)
```

Continuous pool 규칙:

- worker는 최소 2개다.
- 목표 CPU 사용률은 0보다 크고 1보다 작다.
- worker당 목표 메시지 수는 설정하지 않는다.
- queue에 쌓인 메시지 수로 확장하지 않는다.

`mode="finite"`처럼 문자열을 직접 넣으면 자동으로 바꾸지 않고 실패한다.
`WorkloadMode.FINITE` 또는 `WorkloadMode.CONTINUOUS`를 사용한다.

worker가 최대로 늘어났을 때 DB 연결 수:

```text
max_replicas × connections_per_replica
```

## Workload를 worker 묶음에 연결하기

workload를 등록할 때 적은 `execution_class`가 어떤 runtime pool을 사용할지 연결한다.

```python
from distributed_runtime.core import ExecutionClassConfig

batch = ExecutionClassConfig(
    runtime_pool="finite-default",
    queue="finite-default-subscription",
)
```

- Finite pool에 연결하려면 Pub/Sub queue 또는 subscription 이름이 필요하다.
- Continuous pool에 연결할 때는 queue를 설정하면 안 된다.
- 설정에 없는 runtime pool 이름을 사용할 수 없다.

## Cloud SQL 연결 한도 계산하기

```python
from distributed_runtime.core import DatabaseCapacity

database = DatabaseCapacity(
    max_connections=200,
    reserved_connections=20,
    utilization_limit=0.70,
    fixed_service_connections=10,
)
```

전체 DB 연결 중 runtime이 사용해도 되는 수:

```text
floor((max_connections - reserved_connections) × utilization_limit)
```

> **현재 계산의 주의점**
>
> 구현은 `math.floor()`와 Python `float`를 사용한다. `0.70`이 컴퓨터 안에서는 정확한
> 10진수 0.70보다 조금 작게 저장될 수 있다. 따라서 위 예의 실제 결과는 126이 아니라
> 125다.

```text
floor((200 - 20) × 0.70) = 125
```

계산 방식을 `Decimal`이나 정수 비율로 바꾸면 결과가 달라질 수 있다. 이 변경은 단순한
내부 정리가 아니라 공개 동작 변경으로 검토하고 테스트 기준도 함께 바꿔야 한다.

`utilization_limit`은 0보다 크고 0.70 이하여야 한다. 갑자기 연결 수가 늘거나 운영
도구가 DB에 접속할 수 있도록 최소 30%를 runtime 밖에 남긴다.

## RuntimeConfig

```python
from distributed_runtime.core import (
    DatabaseCapacity,
    ExecutionClassConfig,
    RuntimeConfig,
)

config = RuntimeConfig(
    runtime_pools={
        "finite-default": finite_pool,
        "continuous-default": continuous_pool,
    },
    execution_classes={
        "batch": ExecutionClassConfig(
            runtime_pool="finite-default",
            queue="batch-subscription",
        ),
        "stream": ExecutionClassConfig(
            runtime_pool="continuous-default",
        ),
    },
    database=DatabaseCapacity(
        max_connections=200,
        reserved_connections=20,
        fixed_service_connections=10,
    ),
)
```

runtime pool과 execution class dictionary는 생성할 때 복사한다. 호출자가 원래
dictionary를 나중에 수정해도 config 값은 바뀌지 않는다.

## 최악의 경우에도 연결 수가 안전한지 확인하기

`RuntimeConfig.connection_capacity()`는 다음 계산 결과를 보여 준다.

- `budget`: runtime이 사용할 수 있는 연결 수
- `fixed_demand`: API나 reconciler처럼 고정 서비스가 사용할 연결 수
- `pool_demand`: 각 worker pool이 최대로 늘어났을 때 사용할 연결 수
- `total_demand`: 고정 서비스와 모든 pool의 합계
- `remaining`: 모두 사용한 뒤 남는 연결 수
- `is_safe`: 수요가 budget 안에 들어오는지

위 예:

```text
finite pool:     10 × 4 = 40
continuous pool: 5 × 3 = 15
fixed services:          10
total demand:            65
budget:                 125
remaining:               60
```

`RuntimeConfig`를 만드는 순간 전체 수요를 계산한다. budget을 넘으면 runtime을
시작하지 않고 `ConfigurationError`를 발생시킨다. 오류에는 budget, 전체 수요,
초과한 수가 들어간다.

## GCP나 설정 loader를 만들 때 확인할 것

- 외부 문자열 mode를 명시적으로 `WorkloadMode`로 바꾼다.
- boolean이나 소수를 정수로 억지 변환하지 않는다.
- 배포 revision에 실제 사용한 secret version을 함께 기록한다.
- Terraform의 MIG 최대 VM 수와 config의 `max_replicas`를 같게 유지한다.
- Cloud SQL 설정을 바꾸면 모든 pool의 최대 연결 수를 다시 계산한다.
- 설정 검사가 실패하면 warning만 남기고 실행을 계속하지 않는다.
