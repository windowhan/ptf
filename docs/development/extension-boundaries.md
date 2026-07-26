# 런타임 확장 경계

이 문서는 Milestone01 계약 위에 storage, worker, GCP adapter를 구현할 때 지켜야 할
경계를 설명한다.

## 의존성 규칙

허용 방향:

```text
gcp/worker/control → application/registry → finite/continuous → core
제품 integration → public contracts
```

금지 방향:

```text
core → gcp/worker/control
finite/continuous → Google client
runtime package → 제품 도메인 package
```

GCP dependency는 `distributed_runtime.gcp`와 실제 runtime component에만 둔다.
기본 `distributed_runtime` import는 credential, network, Google module import를
발생시키면 안 된다.

## Storage adapter

후속 repository는 단순 CRUD wrapper가 아니라 계약의 durable 권위 구현이다.

Finite:

- run과 pinned revision 저장
- planning record 순서 보존
- execution/idempotency unique constraint
- attempt claim/renew/complete 원자성
- 상태 변경과 outbox insert의 동일 transaction

Continuous:

- deployment/partition desired state 저장
- DB server time 기반 lease expiry
- acquire 시 fencing token 단조 증가
- renew/release/checkpoint에 owner와 token 조건
- stale owner update가 0 rows가 되도록 조건부 write

## Pub/Sub adapter

`VersionedEnvelope`만 transport boundary로 사용한다.

- publish 전에 `to_json()`을 사용한다.
- receive 후 `from_json()`을 통과시킨다.
- 완료 상태 commit 뒤 ack한다.
- 업무 retry는 DB scheduler/outbox가 권위다.
- Pub/Sub delivery attempt와 runtime attempt를 혼합하지 않는다.
- redelivery된 완료 execution은 handler를 다시 실행하지 않는다.

일반 subscription은 at-least-once로 가정한다. exactly-once 옵션이 생겨도 외부 side
effect의 exactly-once를 의미하지 않는다.

## Artifact adapter

권장 순서:

1. content를 object generation 고정 방식으로 업로드한다.
2. 실제 generation을 읽는다.
3. `ArtifactReference.from_bytes()`로 digest/size를 만든다.
4. envelope application과 동일한 namespace로 저장한다.
5. download 후 handler에 전달하기 전에 `verify()`한다.

mutable URI, digest 생략, cross-application reference를 허용하지 않는다.

## Finite worker

worker는 최소 다음 상태 기계를 구현해야 한다.

```text
message 수신
→ execution row 확인
→ attempt claim
→ context/deadline 생성
→ handler 실행
→ 결과 또는 artifact 검증
→ 완료/재시도/실패 commit
→ outbox 및 ack
```

timeout, SIGTERM, 사용자 취소를 하나의 `CancellationSource`에 연결한다.
`RateLimitedExecutionError.retry_after`는 정확한 not-before schedule에 반영한다.

## Continuous worker

```text
instance heartbeat
→ partition lease acquire
→ LeaseHandle/PartitionContext 생성
→ handler loop
→ 주기적 renew
→ lease loss 또는 drain 시 cancellation
→ checkpoint/release
```

lease loss 뒤 handler가 멈추는 것만으로는 충분하지 않다. DB와 runtime-fenced sink가
현재 fencing token을 조건으로 stale write를 거부해야 한다.

## Reconciler

reconcile tick은 중복 실행 가능하고 idempotent해야 한다.

- discovery snapshot 수집
- desired/current partition 비교
- expired lease 회수
- missing partition 생성
- 사라진 partition 상태 전이
- unassigned partition 관측

하나의 process session lock에 의존하지 않는다. 후속 Cloud SQL 구현에서는 짧은
transaction과 transaction-scoped coordination을 사용한다.

## Configuration bootstrap

권장 순서:

1. 파일/env/Terraform output을 raw config로 읽는다.
2. secret은 value가 아니라 versioned `SecretReference`로 연결한다.
3. 모든 pool/class/database 설정을 `RuntimeConfig`로 생성한다.
4. capacity 검증 통과 후 client pool을 만든다.
5. registry workload와 execution class의 교차 참조를 검증한다.
6. immutable config revision을 로그와 envelope에 기록한다.

validation 실패를 warning으로 바꾸고 실행을 계속하지 않는다.

## Observability 연결

`LogContext`의 bounded field:

- service
- runtime version
- application/workload
- run/deployment
- execution/partition
- attempt
- instance
- fencing token

execution ID와 partition ID처럼 cardinality가 큰 값은 structured log field로 사용하고
custom metric label로 직접 사용하지 않는 것을 권장한다.

모든 구조화 오류는 `RuntimeContractError.as_dict()` 형태를 유지한다. adapter-specific
exception을 catch-all로 삼지 말고 retryable/permanent/cancelled 분류로 변환한다.

## 구현 금지 패턴

- decoder에서 잘못된 값을 기본값으로 교체
- JSON key를 `str(key)`로 coercion
- `value or default`로 falsey object 손실
- boolean을 integer로 허용
- moving secret/artifact alias
- global decorator import side effect
- session lifetime advisory lock을 lease로 사용
- handler 완료 commit 전 transport ack
- fencing token 없는 continuous sink write
- core package에서 GCP client import

## 새 adapter 완료 체크리스트

- public contract를 재정의하지 않고 기존 타입을 사용했는가
- 모든 external input이 contract constructor/decoder를 통과하는가
- 재시작과 redelivery 이후 identity가 동일한가
- DB transaction과 transport ack 순서가 명시적인가
- cancellation과 graceful shutdown 경로가 있는가
- stale owner/duplicate message hostile test가 있는가
- 기본 package import가 여전히 GCP-free인가
- 전체 contract/snapshot/wheel gate가 통과하는가
