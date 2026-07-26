# Envelope와 artifact 레퍼런스

## 목적

`VersionedEnvelope`는 transport adapter가 공유하는 canonical message 형식이다.
현재 package는 메시지를 encode/decode하지만 Pub/Sub에 전송하지 않는다.

작은 payload는 inline JSON으로, 큰 payload는 immutable `ArtifactReference`로 표현한다.

## VersionedEnvelope 필드

| 필드 | 타입 | 규칙 |
|---|---|---|
| `schema_version` | `int` | exact integer `1` |
| `message_kind` | `str` | lowercase dotted/stable identifier |
| `run_id` | `str` | non-empty trimmed |
| `execution_id` | `str` | non-empty trimmed |
| `idempotency_key` | `str` | non-empty trimmed |
| `application` | `str` | non-empty trimmed |
| `workload` | `str` | non-empty trimmed |
| `workload_version` | `str` | non-empty trimmed |
| `handler` | `str` | non-empty trimmed |
| `attempt_generation` | `int` | exact positive integer |
| `execution_class` | `str` | non-empty trimmed |
| `runtime_pool_revision` | `str` | non-empty trimmed |
| `published_at` | `str` | offset 포함 RFC 3339 |
| `payload` | mapping 또는 `None` | inline JSON |
| `payload_ref` | artifact 또는 `None` | external immutable payload |
| `trace_context` | mapping | non-empty string key와 string value |

`payload`와 `payload_ref`는 정확히 하나만 존재해야 한다.

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

## Canonical JSON

`to_json()`은 다음 규칙으로 UTF-8 bytes를 만든다.

- key 정렬
- whitespace 없는 separator
- Unicode를 ASCII escape로 강제하지 않음
- NaN/Infinity 금지
- detached mutable JSON 반환

동일 envelope는 동일 bytes로 직렬화되므로 hashing, snapshot, transport assertion에
사용할 수 있다.

```python
encoded = envelope.to_json()
decoded = VersionedEnvelope.from_json(encoded)
assert decoded == envelope
```

내부 payload는 immutable하지만 `to_dict()` 결과는 호출자가 수정할 수 있는 detached
object다.

## Inline payload

inline payload는 JSON-compatible mapping이어야 한다.

허용 값:

- string
- exact integer
- finite float
- boolean
- `None`
- list
- 문자열 key mapping

mapping과 list는 내부적으로 mapping proxy와 tuple로 freeze된다.

canonical encoding이 256 KiB를 초과하면 실패하고 `payload_ref`를 사용해야 한다.
이 제한은 Python object의 메모리 크기가 아니라 실제 canonical JSON byte 길이다.

## Version decoding

`from_dict()`와 `from_json()`은 `supported_versions`를 받을 수 있다.

```python
VersionedEnvelope.from_json(encoded, supported_versions={1})
```

version이 지원 목록에 없으면 `UnsupportedVersionError`를 발생시키며 details에 입력
version과 지원 version 목록을 담는다.

알 수 없는 optional top-level field는 forward compatibility를 위해 무시한다. 반면
필수 field 누락이나 잘못된 타입은 실패한다.

## ArtifactReference

`ArtifactReference`는 내용이 바뀔 수 있는 일반 GCS 경로가 아니라 특정 generation과
digest에 고정된 reference다.

| 필드 | 규칙 |
|---|---|
| `application` | 안정적인 ASCII 이름 |
| `bucket` | 안정적인 ASCII 이름 |
| `object_name` | non-empty relative path |
| `generation` | exact positive integer |
| `uri` | generation query가 포함된 정확한 URI |
| `sha256` | 64자리 lowercase hexadecimal |
| `size_bytes` | exact non-negative integer |

URI 예:

```text
gs://runtime-artifacts/requests/run-1.json?generation=42
```

`object_name`의 path separator는 유지하고 다른 URI 문자는 percent encoding한다.

## Bytes에서 reference 생성

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

`from_bytes()`는 SHA-256과 byte size를 계산하지만 GCS에 업로드하지 않는다.
generation은 storage adapter가 실제 object generation을 확인한 뒤 제공해야 한다.

## 내용 검증

```python
reference.verify(downloaded_content)
```

size 또는 digest가 다르면 `InvariantViolationError`이며 expected/actual size와 SHA-256,
application, URI를 details에 담는다.

## Envelope에 artifact 사용

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

envelope application과 artifact application이 다르면 실패한다.

## Payload representation

- `to_payload()`: envelope에 넣을 canonical scalar mapping
- `from_payload()`: mapping을 검증하여 reference 복원

`from_payload()`는 필수 field를 조회하기 전에 모든 key가 string인지 확인한다. 따라서
비문자 extra key를 조용히 무시하지 않는다. nested `VersionedEnvelope.payload_ref`도
동일 decoder를 사용한다.

## Trust boundary 체크리스트

transport/storage adapter는 다음 순서를 지켜야 한다.

1. JSON bytes를 object로 decode한다.
2. `VersionedEnvelope.from_dict()`로 schema와 타입을 검증한다.
3. artifact가 있으면 application scope를 확인한다.
4. generation-qualified URI로 정확한 object version을 읽는다.
5. `ArtifactReference.verify()`로 size와 digest를 확인한다.
6. 검증된 payload만 handler에 전달한다.

다음 동작은 금지한다.

- `latest` object generation 사용
- digest 검증 생략
- application이 다른 artifact 재사용
- boolean을 generation/size/attempt integer로 허용
- 잘못된 envelope를 기본값으로 보정
