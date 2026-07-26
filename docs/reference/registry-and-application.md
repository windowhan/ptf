# Registry와 application 레퍼런스

## RuntimeApplication

제품 통합의 최소 root facade다.

```python
from distributed_runtime import RuntimeApplication

app = RuntimeApplication("sample-service")
registry = app.registry
```

application 이름은 `WorkloadId` 규칙으로 검증되고 registry namespace가 된다.

최상위 `distributed_runtime` package가 공개하는 값은 다음 두 개뿐이다.

- `RuntimeApplication`
- `__version__`

세부 계약은 `distributed_runtime.core`, `.finite`, `.continuous`에서 명시적으로
import한다.

## RuntimeRegistry

registry는 startup composition root다. workload와 sink를 등록하고 조회한다.

내부 상태는 mutable하며 동시 등록을 위한 thread-safe container가 아니다. process가
요청을 처리하기 전에 한 번 구성하고 이후 read-only로 사용하는 방식을 권장한다.

## Workload identity

workload의 전체 identity:

```text
application + workload_name + semantic_version + mode
```

이를 `WorkloadIdentity`가 표현한다.

같은 application/name이라도 다음은 서로 다른 등록이다.

- `1.0.0`과 `2.0.0`
- Finite `1.0.0`과 Continuous `1.0.0`

전체 identity가 같은 중복만 거부한다.

## Finite 등록

```python
registration = app.registry.register_finite(
    name="daily-report",
    semantic_version="1.2.0",
    execution_class="batch",
    planner=planner,
    handler=handler,
)
```

생성되는 `FiniteRegistration`:

- application
- name
- semantic version
- execution class
- planner
- handler
- 계산된 mode `finite`

planner는 다음 declaration을 가져야 한다.

```python
name = "daily-report"
version = "1.2.0"
mode = WorkloadMode.FINITE
```

등록 인자와 declaration이 정확히 일치하지 않으면 실패한다.

## Continuous 등록

```python
registration = app.registry.register_continuous(
    name="event-stream",
    semantic_version="1.0.0",
    execution_class="stream",
    workload=workload,
)
```

생성되는 `ContinuousRegistration`:

- application
- name
- semantic version
- execution class
- workload
- 계산된 mode `continuous`

workload declaration도 name/version/mode가 등록 인자와 일치해야 한다.

## SemVer 규칙

registry version은 완전한 ASCII SemVer 2.0 형식을 사용한다.

유효:

```text
0.0.0
1.2.3
1.2.3-alpha
1.2.3-alpha.1
1.2.3-1a
1.2.3-0alpha
1.2.3-01a
1.2.3+build.7
1.2.3-rc.1+build.7
```

무효:

```text
01.2.3
1.02.3
1.2.03
1.2.3-01
1.2.3-alpha_1
1.2
v1.2.3
```

major/minor/patch와 numeric prerelease의 숫자는 `[0-9]`만 허용한다. Arabic-Indic 같은
Unicode decimal digit은 Python의 숫자 문자로 인식되더라도 거부한다.

digit-leading alphanumeric prerelease는 허용하지만 순수 numeric identifier의 leading
zero는 금지한다. 따라서 `01a`는 유효하고 `01`은 무효다.

## 조회

```python
registration = app.registry.workload(
    "daily-report",
    "1.2.0",
    WorkloadMode.FINITE,
)
```

name/version/mode 전체를 제공해야 한다. 없으면 `RegistrationError`다.

전체 workload는 identity 정렬 순서의 tuple로 조회한다.

```python
all_workloads = app.registry.workloads()
```

## Sink 등록

```python
app.registry.register_sink("events", sink)
registered_sink = app.registry.sink("events")
```

sink name은 non-empty trimmed string이며 같은 이름을 중복 등록할 수 없다.

현재 registry는 sink의 guarantee에 따라 adapter를 자동 선택하거나 설정과 연결하지
않는다. 후속 bootstrap 계층이 `RuntimeConfig`와 registry를 조합해야 한다.

## RegistrationError가 발생하는 경우

- 빈 name/execution class/sink name
- 잘못된 SemVer
- 동일 workload identity 중복
- planner/workload declaration 불일치
- sink 이름 중복
- 존재하지 않는 workload/sink 조회

## Public API 안정성

public surface는 다음 세 층으로 관리한다.

1. package별 `__all__`
2. `tests/contract/snapshots/api.json`
3. `tests/contract/test_public_api.py`

공개 symbol을 추가·삭제·이동할 때는 compatibility snapshot을 의도적으로 갱신하고
버전 정책을 검토해야 한다.

## 후속 bootstrap 권장 순서

1. 외부 설정을 `RuntimeConfig`로 검증한다.
2. `RuntimeApplication`을 생성한다.
3. product package에서 workload/sink factory를 import한다.
4. 명시적으로 registry에 등록한다.
5. registration의 execution class가 config에 존재하는지 검증한다.
6. registry를 worker/control component에 read-only로 전달한다.

decorator 기반 global side effect보다 위 순서의 explicit registration을 기본으로 한다.
