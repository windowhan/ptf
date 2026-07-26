# Envelope와 artifact: 메시지와 큰 데이터

## 언제 사용하는가

`VersionedEnvelope`는 서로 다른 코드가 같은 뜻으로 메시지를 읽도록 만든 공통 전송
형식이다. 현재 package는 Python 객체를 JSON bytes로 바꾸고 다시 읽을 수 있다.
Pub/Sub으로 보내고 받는 기능은 아직 없다.

- 작은 데이터: 메시지 안의 `payload`에 직접 넣는다.
- 큰 데이터: 외부 저장소에 두고 `payload_ref`로 위치와 해시를 전달한다.

두 방법을 동시에 쓰거나 둘 다 비워 둘 수는 없다.

## 메시지에 들어가는 필드

| 필드 | 타입 | 쉬운 설명과 규칙 |
|---|---|---|
| `schema_version` | `int` | 현재는 정수 `1`만 허용 |
| `message_kind` | `str` | `finite.execution.requested` 같은 메시지 종류 |
| `run_id` | `str` | Finite 요청 ID |
| `execution_id` | `str` | 실행 단위 ID |
| `idempotency_key` | `str` | 중복 처리를 막는 데 사용할 ID |
| `application` | `str` | 이 메시지를 소유한 제품 이름 |
| `workload` | `str` | workload 이름 |
| `workload_version` | `str` | workload 버전 |
| `handler` | `str` | 실행할 handler 이름 |
| `attempt_generation` | `int` | 1부터 시작하는 메시지 발행 세대 |
| `execution_class` | `str` | 사용할 worker 종류 |
| `runtime_pool_revision` | `str` | 사용할 worker 설정 버전 |
| `published_at` | `str` | UTC offset이 들어간 RFC 3339 시각 |
| `payload` | mapping 또는 `None` | 메시지 안에 직접 넣는 작은 데이터 |
| `payload_ref` | artifact 또는 `None` | 외부 저장소에 둔 큰 데이터 참조 |
| `trace_context` | mapping | 분산 추적에 전달할 문자열 key-value |

문자열 필드는 비어 있거나 앞뒤에 공백이 있으면 안 된다. `payload`와 `payload_ref`는
둘 중 정확히 하나만 사용한다.

## 생성 예제

```python
from distributed_runtime.core import VersionedEnvelope

envelope = VersionedEnvelope(
    schema_version=1,
    message_kind="finite.execution.requested",
    run_id="run:2026-07-26",
    execution_id="execution:abc",
    idempotency_key="tenant:acme",
    application="sample-service",
    workload="daily-report",
    workload_version="1.0.0",
    handler="render-report",
    attempt_generation=1,
    execution_class="cpu-small",
    runtime_pool_revision="pool:v1",
    published_at="2026-07-26T09:00:00+09:00",
    payload={"tenant": "acme", "date": "2026-07-26"},
    trace_context={"traceparent": "00-example"},
)
```

## 같은 메시지를 항상 같은 bytes로 만들기

`to_json()`은 dictionary key 순서가 달라도 결과가 같도록 다음 규칙을 사용한다.

- key를 이름순으로 정렬
- 불필요한 공백 제거
- Unicode 문자를 그대로 UTF-8로 표현
- NaN/Infinity 금지

내용이 같은 envelope는 항상 같은 bytes가 된다. 따라서 SHA-256 계산, 기준 파일 비교,
전송 테스트에 사용할 수 있다.

```python
encoded = envelope.to_json()
decoded = VersionedEnvelope.from_json(encoded)
assert decoded == envelope
```

envelope 안의 payload는 바꿀 수 없다. 반면 `to_dict()`가 반환한 dictionary는 별도
복사본이므로 호출자가 수정해도 원래 envelope에는 영향을 주지 않는다.

## 작은 데이터를 메시지에 직접 넣기

`payload`에는 JSON으로 표현할 수 있는 key-value 데이터를 넣는다.

허용 값:

- string
- 정확한 정수
- `NaN`이나 `Infinity`가 아닌 float
- boolean
- `None`
- list
- 문자열 key를 가진 dictionary

dictionary와 list도 내부에서는 바뀌지 않는 형태로 복사된다.

정리된 JSON 결과가 256 KiB를 넘으면 envelope 생성이 실패한다. 이때는 데이터를 외부
저장소에 두고 `payload_ref`를 사용한다. 256 KiB는 Python 객체의 메모리 크기가 아니라
실제로 전송할 JSON bytes 크기다.

## 읽을 수 없는 메시지 버전 거부하기

`from_dict()`와 `from_json()`은 `supported_versions`를 받을 수 있다.

```python
VersionedEnvelope.from_json(encoded, supported_versions={1})
```

메시지 version이 지원 목록에 없으면 `UnsupportedVersionError`가 발생한다. 오류에는
받은 version과 현재 읽을 수 있는 version 목록이 들어간다.

미래 version이 선택 field를 추가할 수 있도록 모르는 최상위 field는 무시한다.
하지만 필수 field가 없거나 타입이 잘못되면 바로 실패한다.

## 큰 데이터의 위치와 내용을 고정하기

일반 GCS 경로만 저장하면 같은 경로의 파일이 나중에 바뀔 수 있다.
`ArtifactReference`는 GCS generation과 SHA-256까지 함께 저장해 정확히 어느 내용을
읽어야 하는지 고정한다.

| 필드 | 쉬운 설명과 규칙 |
|---|---|
| `application` | 이 파일을 소유한 제품 이름 |
| `bucket` | GCS bucket 이름 |
| `object_name` | `/`로 시작하거나 끝나지 않는 파일 경로 |
| `generation` | 1 이상의 정확한 정수 |
| `uri` | generation query가 포함된 전체 URI |
| `sha256` | 내용의 64자리 소문자 SHA-256 |
| `size_bytes` | 0 이상의 정확한 byte 크기 |

URI 예:

```text
gs://runtime-artifacts/requests/run-1.json?generation=42
```

`object_name` 안의 `/`는 경로 구분자로 유지한다. URI에서 특별한 의미가 있는 다른
문자는 percent encoding한다.

## 이미 읽은 bytes로 참조 만들기

```python
from distributed_runtime.core import ArtifactReference

content = b'{"tenant":"acme"}'
reference = ArtifactReference.from_bytes(
    application="sample-service",
    bucket="runtime-artifacts",
    object_name="requests/run-1.json",
    generation=42,
    content=content,
)
```

`from_bytes()`는 내용의 SHA-256과 byte 크기를 계산한다. 파일을 GCS에 올리지는 않는다.
`generation`은 storage adapter가 실제 GCS 응답에서 확인한 값을 넣어야 한다.

## 다운로드한 내용이 맞는지 확인하기

```python
reference.verify(downloaded_content)
```

크기나 SHA-256이 하나라도 다르면 `InvariantViolationError`가 발생한다. 오류에는
예상값과 실제값, application, URI가 들어간다.

## Envelope에서 큰 데이터 참조하기

```python
envelope = VersionedEnvelope(
    schema_version=1,
    message_kind="finite.execution.requested",
    run_id="run:1",
    execution_id="execution:abc",
    idempotency_key="unit:1",
    application="sample-service",
    workload="daily-report",
    workload_version="1.0.0",
    handler="render-report",
    attempt_generation=1,
    execution_class="cpu-small",
    runtime_pool_revision="pool:v1",
    published_at="2026-07-26T00:00:00Z",
    payload_ref=reference,
)
```

envelope와 artifact의 application이 다르면 실패한다. 다른 제품의 파일을 실수로
사용하지 못하게 하는 검사다.

## 참조를 dictionary로 바꾸고 다시 읽기

- `to_payload()`: envelope에 넣을 수 있는 dictionary 반환
- `from_payload()`: dictionary의 field와 타입을 확인한 뒤 참조 복원

`from_payload()`는 필수 field를 찾기 전에 모든 key가 문자열인지 먼저 확인한다.
문자열이 아닌 추가 key를 조용히 무시하지 않는다. envelope 안의 `payload_ref`도
같은 방식으로 읽는다.

## 외부에서 받은 메시지를 안전하게 읽는 순서

Pub/Sub이나 GCS 연결 코드는 다음 순서를 지킨다.

1. JSON bytes를 Python 객체로 바꾼다.
2. `VersionedEnvelope.from_dict()`로 필수 field와 타입을 확인한다.
3. artifact가 있으면 envelope와 같은 application인지 확인한다.
4. generation이 포함된 URI로 정확한 파일 version을 읽는다.
5. `ArtifactReference.verify()`로 크기와 SHA-256을 확인한다.
6. 모든 검사를 통과한 데이터만 handler에 전달한다.

다음 방식은 사용하지 않는다.

- 항상 최신 파일을 뜻하는 `latest` generation 사용
- SHA-256 검사 생략
- 다른 application의 artifact 재사용
- generation, size, attempt 같은 정수 자리에 boolean 허용
- 잘못된 메시지 값을 기본값으로 바꾸고 계속 처리
