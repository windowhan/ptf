# 런타임 확장 경계

이 문서는 앞으로 DB 저장 코드, worker, GCP 연결 코드를 만들 때 지켜야 할 규칙을
설명한다. Milestone01의 공개 타입을 다시 만들지 않고 실제 서비스에 연결하는 방법이다.

## 코드가 서로를 사용해도 되는 방향

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

GCP package는 `distributed_runtime.gcp`와 실제 실행 component에서만 사용한다.
`import distributed_runtime`만 했는데 credential을 읽거나 network에 접속하거나
Google module을 불러오면 안 된다.

## DB 저장 코드

repository는 단순히 표를 읽고 쓰는 얇은 코드가 아니다. 재시작 뒤에도 실행 상태와
소유권을 지키는 최종 기준이다.

Finite:

- run과 고정한 planner/handler revision 저장
- planner가 만든 순서대로 planning record 저장
- execution ID와 idempotency key 중복 저장 방지
- 실행 권한 얻기, 연장, 완료 처리를 각각 원자적으로 수행
- 상태 변경과 outbox 메시지 추가를 같은 transaction에서 수행

Continuous:

- deployment와 원하는 partition 상태 저장
- worker 시각이 아닌 DB server 시각으로 lease 만료 계산
- 새 lease를 줄 때마다 fencing token 증가
- lease 연장·반납·진행 위치 저장 시 owner와 token 확인
- 오래된 worker의 update가 0개 행을 바꾸도록 조건부 쓰기

## Pub/Sub 연결 코드

Pub/Sub으로 보내는 runtime 메시지는 `VersionedEnvelope` 형식만 사용한다.

- 보내기 전에 `to_json()`으로 정해진 bytes를 만든다.
- 받은 뒤 `from_json()`으로 모든 field를 확인한다.
- 완료 상태를 DB에 저장한 다음 ack한다.
- 업무 재시도 시각은 DB scheduler와 outbox가 관리한다.
- Pub/Sub 전달 횟수와 runtime 실행 시도 횟수를 따로 기록한다.
- 이미 완료한 execution 메시지가 다시 오면 handler를 다시 호출하지 않는다.

기본 subscription에서는 같은 메시지가 다시 올 수 있다고 가정한다. Pub/Sub의
exactly-once 옵션을 사용하더라도 외부 API 호출이나 DB 쓰기까지 자동으로 한 번만
실행되는 것은 아니다.

## Cloud Storage 연결 코드

권장 순서:

1. 내용을 GCS에 업로드한다.
2. GCS 응답에서 실제 object generation을 읽는다.
3. `ArtifactReference.from_bytes()`로 SHA-256과 크기를 만든다.
4. envelope와 같은 application 이름을 사용한다.
5. 다운로드한 뒤 handler에 넘기기 전에 `verify()`한다.

generation이 없는 URI, SHA-256이 없는 참조, 다른 application의 참조는 허용하지
않는다.

## 끝나는 작업을 실행하는 worker

worker는 최소한 다음 순서를 지켜야 한다.

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

시간 초과, 운영체제 종료 신호, 사용자 취소는 하나의 `CancellationSource`로
handler에 전달한다. `RateLimitedExecutionError.retry_after`보다 일찍 재시도하지
않도록 DB의 다음 실행 시각에 반영한다.

## 계속되는 작업을 실행하는 worker

```text
instance heartbeat
→ partition lease acquire
→ LeaseHandle/PartitionContext 생성
→ handler loop
→ 주기적 renew
→ lease loss 또는 drain 시 cancellation
→ checkpoint/release
```

lease를 잃은 뒤 handler가 멈추기만 해서는 충분하지 않다. 중단 신호가 늦게 전달될 수
있기 때문이다. DB와 보호되는 sink도 최신 fencing token이 아니면 쓰기를 거부해야 한다.

## 원하는 상태와 실제 상태를 맞추는 reconciler

같은 reconcile 작업이 겹쳐 실행될 수 있다고 가정한다. 두 번 실행해도 최종 상태가
달라지지 않아야 한다.

- workload에서 현재 partition 목록 수집
- 원하는 partition과 DB의 현재 partition 비교
- 만료된 lease 회수
- 새로 발견된 partition 생성
- 사라진 partition을 안전한 종료 상태로 변경
- owner가 없는 partition 수 기록

하나의 process가 살아 있는 동안만 유지되는 lock에 의존하지 않는다. Cloud SQL의
짧은 transaction과 transaction이 끝나면 자동으로 풀리는 조정 방법을 사용한다.

## 프로그램 시작 시 설정 읽기

권장 순서:

1. 파일, 환경 변수, Terraform 결과를 원본 설정으로 읽는다.
2. secret 값 자체가 아니라 version이 고정된 `SecretReference`를 만든다.
3. pool, execution class, DB 설정으로 `RuntimeConfig`를 만든다.
4. DB 연결 수 검사를 통과한 뒤에만 client pool을 만든다.
5. 등록한 workload의 execution class가 config에 있는지 확인한다.
6. 사용한 config revision을 로그와 envelope에 기록한다.

설정 검사가 실패하면 warning만 남기고 계속 실행하지 않는다.

## 로그와 metric 연결

로그에서 먼저 사용할 것을 권장하는 field:

- service
- runtime version
- application/workload
- run/deployment
- execution/partition
- attempt
- instance
- fencing token

execution ID와 partition ID는 값의 종류가 계속 늘어난다. 이런 값은 로그에는 넣어도
되지만 custom metric label로 사용하면 비용과 조회 부담이 크게 늘 수 있다.

현재 `LogContext`는 key가 비어 있는지, 예약어인지, JSON 단일 값인지만 확인한다.
위 field 이름이나 값의 종류 수는 강제하지 않는다. 이 운영 규칙은 adapter와 metric
exporter에서 지켜야 한다.

core의 구조화 오류는 `RuntimeContractError.as_dict()` 형태로 로그에 남길 수 있다.
GCP client 오류를 모두 같은 오류로 처리하지 말고 재시도 가능, 영구 실패, 취소로
나눠야 한다.

registry와 일반 값 검사에서 발생하는 `RegistrationError`, `ValueError`, `TypeError`는
자동으로 구조화되지 않는다. API 경계에서 필요에 맞는 오류 응답으로 바꾼다.

## 사용하지 말아야 할 구현 방식

- decoder가 잘못된 값을 기본값으로 바꾸고 계속 처리
- JSON key를 무조건 `str(key)`로 변환
- `value or default`로 유효하지만 `False`처럼 보이는 값을 버림
- boolean을 정수로 허용
- version이 바뀔 수 있는 secret 또는 artifact 별칭 사용
- module import만으로 전역 registry를 변경
- DB session이 유지되는 동안의 advisory lock을 lease로 사용
- handler 완료를 DB에 저장하기 전에 queue 메시지 ack
- fencing token 확인 없이 Continuous 결과 저장
- core package에서 GCP client import

## 새 adapter를 완료했다고 판단하는 기준

- 기존 공개 타입을 다시 만들지 않고 그대로 사용했는가
- 외부 입력이 모두 constructor 또는 decoder 검사를 거치는가
- 재시작하거나 메시지를 다시 받아도 같은 ID가 나오는가
- DB transaction과 queue ack 순서가 코드에서 분명한가
- 작업 중단과 안전한 종료 경로가 있는가
- 오래된 owner와 중복 메시지를 재현하는 테스트가 있는가
- 기본 package import에 여전히 GCP package가 필요 없는가
- contract, snapshot, wheel 검사를 모두 통과했는가
