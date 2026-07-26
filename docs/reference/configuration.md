# Configuration 레퍼런스

`distributed_runtime.core.config`는 runtime adapter가 공유하는 immutable 설정과
capacity 검증을 제공한다.

설정을 읽는 파일/env/Secret Manager loader는 아직 구현되지 않았다. adapter는 외부
설정 값을 이 타입으로 변환하고 생성 성공 이후에만 runtime을 시작해야 한다.

## SecretReference

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

규칙:

- project와 secret ID는 non-empty trimmed string
- version은 positive decimal string
- `"latest"` 금지
- resource name은 version까지 포함

moving alias를 허용하지 않기 때문에 배포 revision이 참조하는 credential version을
감사하고 rollback할 수 있다.

## FinitePolicy

기본값:

| 필드 | 기본값 | 의미 |
|---|---:|---|
| `timeout_seconds` | 300 | 한 attempt timeout |
| `max_timeout_seconds` | 3600 | workload가 요청할 수 있는 상한 |
| `max_attempts` | 5 | runtime 최대 attempt |
| `retry_base_seconds` | 5 | retry exponential base |
| `retry_cap_seconds` | 300 | retry delay 상한 |
| `claim_grace_seconds` | 120 | timeout 뒤 claim 보호 여유 |

`claim_ttl_seconds`는 다음과 같다.

```text
timeout_seconds + claim_grace_seconds
```

모든 시간과 횟수는 exact positive integer다. timeout은 max timeout 이하,
retry base는 cap 이하여야 한다.

## ContinuousPolicy

기본값:

| 필드 | 기본값 | 의미 |
|---|---:|---|
| `heartbeat_seconds` | 15 | instance heartbeat 주기 |
| `lease_seconds` | 60 | partition lease TTL |
| `renew_before_expiry_seconds` | 30 | expiry 전 renew 기준 |
| `reconcile_seconds` | 60 | desired/current 상태 비교 주기 |
| `drain_seconds` | 120 | shutdown drain window |
| `takeover_seconds` | 150 | stale owner takeover 기준 |

필수 관계:

```text
heartbeat <= renew_before_expiry < lease <= takeover
```

모든 값은 exact positive integer다.

## RuntimePoolConfig

한 autoscaling worker pool의 mode와 최악의 DB connection 수요를 표현한다.

공통 필드:

- `mode`
- `min_replicas`
- `max_replicas`
- `connections_per_replica`

### Finite pool

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

규칙:

- `target_messages_per_instance` 필수
- `target_cpu_utilization` 금지
- `min_replicas=0` 허용
- backlog scaling을 전제로 함

### Continuous pool

```python
continuous_pool = RuntimePoolConfig(
    mode=WorkloadMode.CONTINUOUS,
    min_replicas=2,
    max_replicas=5,
    connections_per_replica=3,
    target_cpu_utilization=0.65,
)
```

규칙:

- 최소 replica 2
- CPU utilization은 0과 1 사이
- `target_messages_per_instance` 금지
- queue backlog scaling 대상으로 취급하지 않음

mode에 일반 문자열을 넘기면 자동 변환하지 않고 실패한다.

`worst_case_connections`:

```text
max_replicas × connections_per_replica
```

## ExecutionClassConfig

workload registration의 `execution_class`를 runtime pool에 연결한다.

```python
from distributed_runtime.core import ExecutionClassConfig

batch = ExecutionClassConfig(
    runtime_pool="finite-default",
    queue="finite-default-subscription",
)
```

- finite pool에 연결되는 class는 queue/subscription이 필요하다.
- continuous pool에 연결되는 class는 queue를 설정하면 안 된다.
- 존재하지 않는 runtime pool은 참조할 수 없다.

## DatabaseCapacity

```python
from distributed_runtime.core import DatabaseCapacity

database = DatabaseCapacity(
    max_connections=200,
    reserved_connections=20,
    utilization_limit=0.70,
    fixed_service_connections=10,
)
```

`runtime_budget`:

```text
floor((max_connections - reserved_connections) × utilization_limit)
```

현재 구현은 `math.floor()`와 Python binary `float`를 사용한다. 따라서 아래
`0.70`은 내부적으로 정확한 10진수 0.70보다 조금 작은 값이 될 수 있다. 위 예의 실제
반환값은 다음과 같다.

```text
floor((200 - 20) × 0.70) = 125
```

순수한 10진 산술이라면 결과는 126이지만, 현재 public behavior는 125다. 향후
`Decimal` 또는 정수 비율로 바꾸는 경우 capacity 결과가 달라질 수 있으므로 호환성
변경으로 리뷰하고 관련 capacity test와 snapshot을 함께 갱신해야 한다.

`utilization_limit`은 0보다 크고 0.70 이하여야 한다. 운영자/관리 작업과 connection
burst를 위해 30% 이상의 여유를 강제한다.

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

mapping은 생성 시 복사 후 immutable하게 보관된다.

## ConnectionCapacity

`RuntimeConfig.connection_capacity()`는 다음 값을 반환한다.

- `budget`
- `fixed_demand`
- `pool_demand`
- 계산된 `total_demand`
- 계산된 `remaining`
- 계산된 `is_safe`

위 예:

```text
finite pool:     10 × 4 = 40
continuous pool: 5 × 3 = 15
fixed services:          10
total demand:            65
budget:                 125
remaining:               60
```

`RuntimeConfig` 생성 시 `is_safe`가 false면 `ConfigurationError`가 발생한다. details는
budget, total demand, 초과량을 포함한다.

## Adapter 구현 체크리스트

- 외부 문자열 mode를 명시적으로 `WorkloadMode`로 parse한다.
- bool/fraction을 integer field로 coercion하지 않는다.
- secret version을 deployment revision과 함께 기록한다.
- Terraform/MIG max replica와 config의 값을 동일하게 유지한다.
- Cloud SQL 설정 변경 시 전체 worst-case capacity를 다시 검증한다.
- validation 실패를 warning으로 낮추거나 기본값으로 우회하지 않는다.
