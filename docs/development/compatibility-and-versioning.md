# 호환성과 버전 정책

이 프로젝트에는 서로 목적이 다른 네 가지 version이 있다. 이름이 모두 version이지만
바꾸는 이유와 영향 범위가 다르다.

| Version | 무엇이 바뀌었을 때 올리는가 |
|---|---|
| Package version | Python package의 공개 API나 동작이 바뀜 |
| Workload version | 제품 workload의 입력·출력 규칙이 바뀜 |
| Envelope schema version | 전송 메시지 구조가 바뀜 |
| Runtime revision | 실제 배포한 planner, handler, worker 설정이 바뀜 |

한 version을 바꿨다고 나머지를 자동으로 올리지는 않는다.

## Python package version

`distributed-runtime`을 build하고 배포할 때 붙는 version이다.

현재:

```text
0.1.0
```

공개 import 이름, 함수 동작, dependency, package 구성 변경을 표현한다.

## 제품 workload version

제품 workload의 입력과 결과 규칙 version이다. registry에서 workload를 구분하는 값의
일부로 사용한다.

```text
application + name + semantic_version + mode
```

ASCII SemVer 2.0을 사용한다. Python package version과는 별개다. 같은 process에
`daily-report`의 `1.0.0`과 `2.0.0`을 함께 등록할 수도 있다.

## 전송 메시지 구조 version

Pub/Sub 등을 통해 보내는 `VersionedEnvelope`의 field 구조를 나타낸다.

현재:

```text
schema_version = 1
```

decoder는 읽을 수 없는 version을 추측해서 처리하지 않고 거부한다. 읽을 수 있는
version에 모르는 선택 field가 추가된 경우에는 그 field만 무시한다.

새 schema version을 만들 때:

1. 기존 V1 메시지를 계속 읽을 수 있게 유지한다.
2. 읽을 수 있는 version 목록에 새 값을 직접 추가한다.
3. 새 메시지의 canonical JSON 기준 파일을 추가한다.
4. 이전 발행자와 새 수신자, 새 발행자와 이전 수신자 조합을 테스트한다.

## 실제 배포 revision

실제로 배포한 planner 코드, handler 코드, worker pool 설정을 구분하는 ID다.

- planner revision
- execution revision
- runtime pool revision

사용자에게 공개하는 SemVer일 필요는 없다. 내부 배포 ID나 artifact digest를 사용할 수
있다. 하지만 같은 Finite run을 재시도하거나 메시지를 다시 처리하는 중에는 바꾸면
안 된다.

## 실수 변경을 잡는 기준 파일

현재 snapshot:

- public API
- canonical envelope
- default/representative config

snapshot은 단순히 현재 결과를 복사해 둔 파일이 아니다. 사용자 코드와 저장된
메시지가 의존하는 형식을 의도적으로 고정한 기준이다.

## 어떤 변경이 기존 사용자를 깨뜨리는가

| 변경 | 보통 어떻게 판단하는가 |
|---|---|
| 비공개 helper 내부 정리 | 보통 호환됨 |
| 공개 클래스나 함수 추가 | 기존 사용자는 유지되지만 snapshot 검토 필요 |
| 공개 클래스나 함수 삭제·이동 | 기존 import가 깨짐 |
| envelope 선택 field 추가 | decoder 동작에 따라 호환 가능 |
| envelope 필수 field 또는 의미 변경 | 기존 메시지가 깨지므로 새 schema 필요 |
| 기본 timeout, 재시도 설정 변경 | API는 같아도 운영 동작이 달라짐 |
| workload 선언 변경 | 새 workload version 권장 |
| execution ID 계산식 변경 | 저장된 ID가 달라지므로 migration 필요 |

## 현재는 아직 검사하지 못하는 것

Milestone01은 기준 파일과 메시지 version 검사까지만 제공한다.

- 이전 worker와 새 worker를 함께 비교하는 자동 테스트
- 여러 runtime revision을 동시에 배포한 환경 테스트
- DB에 저장된 schema를 실제로 바꾸는 migration
- artifact와 package를 이전 version으로 자동 복구
- 일부 트래픽만 새 version으로 보내는 실제 GCP canary

Phase 5에서 이 검사를 추가할 때도 현재 snapshot을 없애지 않고 더 넓은 검증의
기준으로 사용한다.

## version을 바꾸기 전에 묻는 질문

- package, workload, envelope, runtime revision 중 무엇이 바뀌는가?
- 이전 발행자가 만든 메시지를 새 수신자가 읽을 수 있는가?
- 새 발행자가 만든 메시지를 이전 수신자가 읽을 수 있는가?
- 실행 ID, idempotency key, SHA-256 계산 결과가 달라지는가?
- 이미 DB에 저장한 행이나 queue 메시지를 바꿔야 하는가?
- snapshot 차이가 의도한 변경인가?
- 이전 version으로 되돌린 뒤에도 저장된 데이터를 읽을 수 있는가?
