# 쉬운 용어 설명

이 프로젝트는 분산 실행과 GCP 용어를 함께 사용한다. 처음 읽을 때 낯설 수 있는 말을
이 문서에서 짧게 설명한다.

코드에 쓰이는 클래스명과 필드명은 영어 그대로 둔다. 설명에서는 가능한 한 쉬운
한국어를 함께 쓴다.

## 작업을 설명하는 말

| 용어 | 쉬운 뜻 |
|---|---|
| runtime | 작업을 등록하고 실행하도록 돕는 공통 프로그램 |
| workload | runtime에 맡길 작업 한 종류 |
| Finite workload | 처리할 양이 정해져 있고 언젠가 끝나는 작업 |
| Continuous workload | 계속 실행하면서 새 데이터를 처리하는 작업 |
| application | runtime을 사용하는 제품 또는 서비스의 이름 |
| contract | 서로 다른 코드가 지켜야 하는 입력·출력 규칙 |
| planner | 큰 요청을 작은 실행 단위로 나누는 코드 |
| execution unit | 따로 실행하고 따로 재시도할 수 있는 작은 작업 |
| handler | 실행 단위 하나를 실제로 처리하는 제품 코드 |
| partition | Continuous 작업을 나누는 독립 처리 구역 |
| sink | 처리 결과나 이벤트를 내보내는 목적지 |

## 안전하게 실행하기 위한 말

| 용어 | 쉬운 뜻 |
|---|---|
| identity | 실행이나 작업을 다시 찾아낼 수 있게 하는 고유한 값 |
| revision | 배포된 코드나 설정의 고정된 버전 |
| idempotency | 같은 요청을 여러 번 받아도 결과가 한 번 처리한 것과 같게 만드는 성질 |
| deterministic | 같은 입력이면 항상 같은 결과가 나오는 성질 |
| drift | 같은 요청을 다시 계산했는데 이전 결과와 달라진 상태 |
| immutable | 만든 뒤에는 값이 바뀌지 않는 성질 |
| fail-closed | 잘못된 입력을 임의로 고치지 않고 즉시 거부하는 방식 |
| canonical | 같은 의미의 값을 언제나 한 가지 형태로 표현하는 방식 |
| snapshot | 공개 형식이 실수로 바뀌지 않았는지 비교하는 기준 파일 |

## Continuous 작업 소유권

| 용어 | 쉬운 뜻 |
|---|---|
| lease | 일정 시간 동안 partition을 처리할 권리 |
| owner | 현재 lease를 가진 runtime instance |
| heartbeat | instance가 아직 살아 있다고 주기적으로 알리는 신호 |
| fencing token | 오래된 owner의 쓰기를 막기 위해 lease마다 증가시키는 번호 |
| stale owner | lease를 잃었지만 아직 자신이 owner라고 생각하는 오래된 실행 |
| reconciler | 실제 상태를 확인하고 원하는 상태와 맞추는 조정 작업 |
| rebalance | partition을 여러 instance에 다시 나누는 작업 |

`LeaseHandle.authorizes()`가 `True`를 반환한다고 해서 DB 쓰기가 자동으로 안전해지는
것은 아니다. 실제 저장 쿼리도 owner와 fencing token이 현재 값인지 확인해야 한다.

## 메시지와 저장

| 용어 | 쉬운 뜻 |
|---|---|
| envelope | 실제 데이터와 식별자·버전 정보를 함께 담는 전송 상자 |
| payload | envelope 또는 작업 안에 들어가는 실제 데이터 |
| artifact | 메시지에 직접 넣기 큰 데이터를 별도 저장소에 둔 것 |
| artifact reference | artifact 위치, 세대 번호, 크기, 해시를 담은 참조 |
| repository | DB에서 상태를 읽고 쓰는 코드 |
| transaction | 여러 DB 변경을 하나의 작업처럼 성공하거나 실패하게 하는 단위 |
| outbox | DB 변경과 메시지 발행 요청을 함께 저장하는 표 |

## 재시도와 메시지 처리

| 용어 | 쉬운 뜻 |
|---|---|
| retry | 실패한 작업을 다시 시도하는 것 |
| attempt | 한 번의 실행 시도 |
| ack | 메시지를 처리했으니 queue에서 지워도 된다는 응답 |
| nack | 지금 처리하지 못했으니 다시 보내 달라는 응답 |
| DLQ | 계속 실패한 메시지를 따로 모으는 Dead Letter Queue |
| at-least-once | 같은 메시지가 한 번 이상 전달될 수 있는 방식 |
| exactly-once | 지원 조건 안에서 메시지 전달 중복을 막는 방식 |

이 runtime의 기본 설계는 같은 메시지가 다시 올 수 있다고 가정한다. 따라서 handler와
sink도 중복 호출을 안전하게 처리해야 한다.

## 구성과 배포

| 용어 | 쉬운 뜻 |
|---|---|
| configuration | runtime이 어떤 방식으로 동작할지 정한 설정 |
| execution class | 비슷한 자원과 실행 방법을 쓰는 작업 묶음 |
| runtime pool | 같은 설정으로 실행되는 worker instance 묶음 |
| adapter | 공통 계약을 GCP나 DB 같은 실제 서비스에 연결하는 코드 |
| MIG | Compute Engine VM 수를 관리하는 Managed Instance Group |
| autoscaling | 작업량에 따라 instance 수를 자동으로 늘리거나 줄이는 기능 |
| scale-to-zero | 할 일이 없을 때 instance 수를 0으로 줄이는 동작 |
| E2E | 요청부터 실제 처리 결과까지 전체 경로를 확인하는 End-to-End 검증 |
| PITR | 특정 과거 시각의 DB 상태로 복구하는 Point-in-Time Recovery |

## Python과 API

| 용어 | 쉬운 뜻 |
|---|---|
| public API | 이 패키지를 사용하는 코드가 의존해도 되는 공개 기능 |
| facade | 자주 쓰는 기능을 한 곳에서 시작할 수 있게 만든 얇은 진입점 |
| registry | workload와 sink를 이름으로 등록하고 찾는 저장소 |
| protocol | 구현 클래스가 제공해야 하는 메서드와 속성의 모양 |
| dataclass | 데이터를 담는 Python 클래스를 간단히 만드는 기능 |
| mapping | key와 value로 데이터를 찾는 dictionary 형태 |
| async | 기다리는 동안 다른 일을 처리할 수 있는 비동기 실행 방식 |
| iterator | 값을 한꺼번에 만들지 않고 하나씩 꺼내는 방식 |
| SemVer | `1.2.3`처럼 주·부·수 버전으로 호환성을 표현하는 규칙 |

## 종료와 관측

| 용어 | 쉬운 뜻 |
|---|---|
| cancellation | 실행 중인 작업에 중단을 요청하는 것 |
| deadline | 작업을 끝내야 하는 마지막 시각 |
| graceful shutdown | 새 작업을 받지 않고 진행 중인 일을 정리한 뒤 종료하는 방식 |
| structured logging | 검색하기 쉬운 key-value 형태로 로그를 남기는 방식 |
| metric | 처리량, 실패 수, 지연 시간처럼 숫자로 관찰하는 값 |
| branch coverage | 조건문의 각 갈래까지 테스트했는지 나타내는 비율 |

## 문서에서 자주 쓰는 표현

- **현재 구현한다:** 지금 저장소의 코드와 테스트로 확인할 수 있다.
- **후속 구현이 맡는다:** 계약만 있고 실제 GCP/DB 동작은 아직 없다.
- **검증한다:** 잘못된 입력이면 현재 코드가 오류를 발생시킨다.
- **권장한다:** 운영상 좋은 방향이지만 현재 코드가 강제하지는 않는다.
- **보장하지 않는다:** 호출하는 제품 코드나 다음 구현 단계가 책임져야 한다.
