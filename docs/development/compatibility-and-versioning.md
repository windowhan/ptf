# 호환성과 버전 정책

이 프로젝트에는 서로 다른 네 종류의 version이 존재한다. 하나를 변경했다고 다른
version을 자동으로 올리지 않는다.

## Package version

`distributed-runtime` 배포물의 version이다.

현재:

```text
0.1.0
```

Python import surface, 동작, dependency, packaging 변경을 표현한다.

## Workload semantic version

registry identity의 일부이며 제품 workload contract version이다.

```text
application + name + semantic_version + mode
```

ASCII SemVer 2.0을 사용한다. package version과 독립적이며 한 process에 여러 workload
version을 동시에 등록할 수 있다.

## Envelope schema version

transport payload 구조의 version이다.

현재:

```text
schema_version = 1
```

decoder는 지원하지 않는 version을 fail-closed로 거부하고, known version의 unknown
optional field는 무시한다.

새 version을 도입할 때는:

1. 기존 V1 decoder 동작을 유지한다.
2. supported version 집합을 명시적으로 확장한다.
3. canonical encoding snapshot을 추가한다.
4. mixed producer/consumer compatibility를 테스트한다.

## Runtime revision

실제 배포된 planner, execution code, runtime pool config의 immutable revision이다.

- planner revision
- execution revision
- runtime pool revision

SemVer와 달리 내부 deployment identity일 수 있다. Finite retry와 message replay 중
바뀌면 안 된다.

## Snapshot

현재 snapshot:

- public API
- canonical envelope
- default/representative config

snapshot은 “현재 출력이 이렇다”는 기록이 아니라 의도된 compatibility boundary다.

## 변경 분류

| 변경 | 기본 판단 |
|---|---|
| private helper refactor | compatible |
| public symbol 추가 | additive, snapshot 검토 |
| public symbol 제거/이동 | breaking |
| envelope optional field 추가 | 조건부 compatible |
| envelope 필수 field/의미 변경 | breaking 또는 새 schema |
| default policy 변경 | 운영 영향 검토 필요 |
| workload declaration 변경 | 새 workload version 권장 |
| execution identity 공식 변경 | breaking, migration 필요 |

## 현재 한계

Milestone01은 snapshot과 version gate만 제공한다. 다음은 아직 없다.

- N-1 worker compatibility runner
- mixed-fleet deployment test
- persisted schema migration
- artifact/package rollback automation
- 실제 GCP canary

Phase 5에서 이 기능을 추가하더라도 현재 snapshot을 삭제하지 않고 확장해야 한다.

## 변경 체크리스트

- 어떤 version domain이 바뀌는가
- old producer/new consumer 조합이 가능한가
- new producer/old consumer 조합이 가능한가
- identity/idempotency/digest가 달라지는가
- durable row 또는 message migration이 필요한가
- snapshot 변경이 의도적인가
- rollback 뒤 old runtime이 데이터를 읽을 수 있는가
