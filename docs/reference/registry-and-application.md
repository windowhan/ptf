# Registry와 application: 작업 등록하기

## RuntimeApplication

제품 코드가 runtime을 사용할 때 만드는 첫 번째 객체다.

```python
from distributed_runtime import RuntimeApplication

app = RuntimeApplication("sample-service")
registry = app.registry
```

application 이름은 서로 다른 제품의 등록 내용이 섞이지 않게 구분한다. 이름은
`WorkloadId`와 같은 규칙으로 검사한다.

최상위 `distributed_runtime` package에서는 자주 쓰는 다음 두 값만 바로 제공한다.

- `RuntimeApplication`
- `__version__`

나머지 타입은 역할을 분명히 알 수 있도록 `distributed_runtime.core`, `.finite`,
`.continuous`에서 직접 불러온다.

## RuntimeRegistry

registry는 프로그램 시작 시 workload와 sink를 모아 두는 곳이다. 이름과 버전으로
등록한 항목을 나중에 다시 찾을 수 있다.

등록할 때는 내부 값이 바뀐다. 여러 thread가 동시에 등록하도록 만든 객체는 아니다.
요청을 받기 전에 한 thread에서 등록을 끝낸 뒤, 실행 중에는 조회만 하는 방식을
권장한다.

## 어떤 등록이 같은 workload인가

workload 하나를 구분하려면 다음 네 값이 모두 필요하다.

```text
application + workload_name + semantic_version + mode
```

이 네 값을 `WorkloadIdentity`가 한 객체로 묶는다.

같은 application/name이라도 다음은 서로 다른 등록이다.

- `1.0.0`과 `2.0.0`
- Finite `1.0.0`과 Continuous `1.0.0`

네 값이 모두 같은 경우만 중복으로 거부한다.

## 끝나는 작업 등록하기

```python
registration = app.registry.register_finite(
    name="daily-report",
    semantic_version="1.2.0",
    execution_class="batch",
    planner=planner,
    handler=handler,
)
```

등록 결과에는 다음 값이 들어간다.

- application
- name
- semantic version
- 사용할 worker 종류인 execution class
- planner
- handler
- 자동으로 정해진 mode `finite`

planner 클래스도 자신이 누구인지 다음처럼 선언해야 한다.

```python
name = "daily-report"
version = "1.2.0"
mode = WorkloadMode.FINITE
```

함수에 전달한 이름·버전·mode와 planner의 선언이 하나라도 다르면 등록에 실패한다.
오타가 있는 workload를 잘못된 이름으로 실행하는 일을 막기 위한 검사다.

## 계속되는 작업 등록하기

```python
registration = app.registry.register_continuous(
    name="event-stream",
    semantic_version="1.0.0",
    execution_class="stream",
    workload=workload,
)
```

등록 결과에는 다음 값이 들어간다.

- application
- name
- semantic version
- 사용할 worker 종류인 execution class
- workload
- 자동으로 정해진 mode `continuous`

workload 클래스의 이름·버전·mode도 함수에 전달한 값과 같아야 한다.

## 버전 문자열 규칙

workload 버전은 SemVer 2.0 형식을 사용한다. 기본 모양은 `주버전.부버전.수버전`이다.
예를 들어 `1.2.3`은 주버전 1, 부버전 2, 수버전 3을 뜻한다.

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

숫자에는 ASCII `0`부터 `9`까지만 사용할 수 있다. 다른 언어권의 숫자 문자가 Python에서
숫자로 인식되더라도 버전에서는 거부한다.

시험 배포 이름처럼 숫자와 문자가 섞인 `01a`는 허용한다. 숫자만 있는 항목은 앞에
불필요한 0을 붙일 수 없으므로 `01`은 허용하지 않는다.

## 등록한 workload 찾기

```python
registration = app.registry.workload(
    "daily-report",
    "1.2.0",
    WorkloadMode.FINITE,
)
```

이름, 버전, mode를 모두 전달해야 한다. 일치하는 등록이 없으면
`RegistrationError`가 발생한다.

등록된 전체 workload는 identity 순으로 정렬된 tuple로 받을 수 있다.

```python
all_workloads = app.registry.workloads()
```

## 결과를 보낼 sink 등록하기

```python
app.registry.register_sink("events", sink)
registered_sink = app.registry.sink("events")
```

sink 이름은 비어 있거나 앞뒤에 공백이 있으면 안 된다. 같은 이름은 한 번만 등록할 수
있다.

현재 registry는 sink의 보호 수준을 보고 GCP adapter를 자동으로 선택하지 않는다.
sink 설정과 `RuntimeConfig`를 서로 연결하는 시작 코드도 앞으로 구현해야 한다.

## 등록이나 조회가 실패하는 경우

- 이름, execution class, sink 이름이 비어 있거나 앞뒤에 공백이 있음
- 잘못된 SemVer
- 같은 workload identity를 두 번 등록
- 함수에 전달한 값과 planner/workload 선언이 다름
- sink 이름 중복
- 등록되지 않은 workload 또는 sink 조회

## 공개 API가 실수로 바뀌지 않게 확인하기

사용자 코드가 의존할 수 있는 공개 이름은 다음 세 곳에서 확인한다.

1. package별 `__all__`
2. `tests/contract/snapshots/api.json`
3. `tests/contract/test_public_api.py`

공개 클래스나 함수를 추가, 삭제, 이동할 때는 기준 파일을 직접 갱신한다. 단순한
snapshot 업데이트로 넘기지 말고 기존 사용자 코드가 깨지는지와 package 버전을 함께
검토한다.

## 프로그램을 시작할 때 권장하는 순서

1. 외부 설정을 읽고 `RuntimeConfig`로 검사한다.
2. `RuntimeApplication`을 생성한다.
3. 제품 package에서 workload와 sink를 만드는 함수를 불러온다.
4. workload와 sink를 registry에 직접 등록한다.
5. 등록된 execution class가 config에 실제로 있는지 확인한다.
6. 등록이 끝난 registry를 worker와 control component에 전달한다.

module을 import하는 순간 몰래 등록되는 decorator 방식보다 위처럼 등록 위치가 보이는
방식을 기본으로 한다. 그래야 테스트와 시작 과정에서 어떤 workload가 들어갔는지 쉽게
확인할 수 있다.
