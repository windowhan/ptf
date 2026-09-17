# GCP E2E 검증 경계

> 이 문서는 로컬에서 검증된 것과 실제 GCP credential이 필요한 것의 경계를
> 정리한다. `implementation-plan.md`의 #50~64에 해당한다.

## 로컬에서 검증 완료된 것

| 검증 | 방법 |
|---|---|
| 계약, serialization, registry, lifecycle | unit/contract tests, `api.json` snapshot |
| Finite 계획→실행→결과 조회 | `FiniteRuntimeTestKit` + Postgres + Pub/Sub emulator |
| Continuous 배정→lease→fencing→emission | `ContinuousRuntimeTestKit` + Postgres integration tests |
| Dead worker partition 회수 | reconciler integration test |
| drain/stop lifecycle | admin + supervisor integration tests |
| outbox at-least-once 발행 | dispatcher integration test |
| retryable→재스케줄→성공, permanent→dead-letter, crash→claim 회수 | Postgres integration tests |
| rate-limited `retry_after` 플로어, per-attempt timeout→retry | Postgres integration tests |
| revision 고정 run은 자기 revision pool만 claim | `claim_units` revision 필터 테스트 |
| 중복 dispatch 재전달 → 재실행 없음 | Postgres integration test |
| poison 메시지 nack (stream 생존) | Pub/Sub emulator test |
| client/admin JSON API 경로 | stdlib HTTP 단위 테스트 |
| GCS artifact adapter (generation 고정, sha256 검증) | fake transport 단위 테스트 |
| Terraform 모듈 정합성 | `terraform fmt`, `terraform validate` (docker image) |
| 예제 product 패키지 | 로컬 킷 테스트 + wheel 빌드 |

## credential이 필요한 것

| Gate | 내용 | 상태 |
|---|---|---|
| E2E-BOOT | terraform apply + teardown | 검증됨 — 전체 apply/destroy 통과 |
| E2E-FIN | finite 예제 실GCP 실행 | 검증됨 — unit claim·실행·결과 완료 |
| E2E-CON | continuous failover 실측 | 검증됨 — heartbeat 중단→인수→fencing 거부 |
| E2E-FAIL | 의도 실패→unit DLQ, retry/rate-limit/timeout, 중복 주입 | 드라이버 구현됨 — 실행 대기 |
| E2E-DLQ | poison 메시지 → Pub/Sub dead-letter 토픽 | 드라이버+subscription 구현됨 — 실행 대기 |
| E2E-REV | missing revision 핀 고정 run 미claim | 드라이버 구현됨 — 실행 대기 |
| E2E-SCALE | MIG 2→3→2 리사이즈 + worker 등록 수렴 | 드라이버 구현됨 — 실행 대기 (CPU autoscaler 자체는 config 검증만) |
| E2E-IAM | client API 200 / admin API 403 분리 | 드라이버 구현됨 — 실행 대기 |
| E2E-SPREAD | 6 partition × 2+ worker 가중치 차이 ≤1 | 드라이버 구현됨 — 실행 대기 |
| E2E-LOG/METRIC/OBS | alert 정책 존재 + dead-letter metric 기록 | 드라이버 구현됨 — 실행 대기 (fire/resolve 인시던트는 확인 안 함) |
| E2E-API | api/admin Cloud Run 서비스 배포 | Terraform 구현됨 — 실배포 대기 |
| E2E-SCHED | Cloud Scheduler → reconcile Job | Terraform 구현됨 — 실배포 대기 |
| FT-MIGRATE/UPGRADE/SQL-FAIL | 혼합 fleet, canary, SQL failover, migration recovery | 미검증 |
| E2E-CUSTOM/CLEAN | custom metric, artifact cleanup | 미검증 |

## 실행 방법

실제 검증은 VPC 안에서 실행되는 Cloud Run driver job으로 진행한다
(`runtime-control-e2e-driver`, 전용 `runtime-e2e-driver` SA).
DB가 private-only라 로컬에서 직접 붙을 수 없기 때문이다.

`scripts/e2e/run_gcp_e2e.sh`가 전체 절차를 감싼다:

```bash
scripts/e2e/run_gcp_e2e.sh <project-id> [region] [scenarios]
```

내부 절차:

1. runtime wheel과 예제 wheel을 빌드한다.
2. `Dockerfile`로 runtime 이미지를, `Dockerfile.driver`로 driver
   이미지를 만들어 Artifact Registry에 push한다.
3. terraform apply에 `worker_image`/`control_image`/`driver_image`
   변수로 실제 이미지를 전달한다. `DEPLOYER`(전체 IAM member 문자열,
   예 `user:you@example.com`)를 넘기면 Scheduler가 control SA로
   oauth_token을 발급할 수 있게 `actAs`를 부여한다.
4. migration job을 실행해 schema를 적용한다.
5. driver job을 실행한다. `RUNTIME_E2E_SCENARIOS`로 시나리오를
   필터할 수 있다 (기본 전부).
6. EXIT trap이 terraform destroy를 실행한다 — 실패해도 정리된다.

## 비용 추정 (us-central1, on-demand)

| 자원 | 최소 구성 | 월 비용 |
|---|---|---:|
| Compute Engine | `e2-micro` × 2 (MIG min) | ~$12 |
| Cloud SQL | `db-f1-micro`, HA 없음 | ~$10 |
| Pub/Sub / Cloud Run / GCS | toy 수준 | ~$1 |

**합계 ~$25/월 상시 운영 시.** E2E는 돌릴 때만 켜고 destroy하므로
실행당 과금이다. `e2-micro`는 일부 리전에서 월 1대 프리티어다.

## teardown 계약

- harness는 정상 종료 시 `terraform destroy`를 실행한다.
- 중간 실패 시에는 수동으로 같은 명령을 실행해 남은 자원을 지운다.
- `terraform state`는 로컬 파일이므로 destroy에 같은 디렉터리가 필요하다.
