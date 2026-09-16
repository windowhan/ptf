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
| Terraform 모듈 정합성 | `terraform fmt`, `terraform validate` (docker image) |
| 예제 product 패키지 | 로컬 킷 테스트 + wheel 빌드 |

## credential이 필요한 것

| Gate | 내용 | 상태 |
|---|---|---|
| E2E-BOOT | terraform apply + harness/teardown | 스크립트 준비됨 (`scripts/e2e/run_gcp_e2e.sh`) |
| E2E-FIN | finite 예제 실GCP 실행 | harness 단계 예약됨 |
| E2E-SCALE | MIG autoscaling 동작 | 미검증 |
| E2E-CON | continuous failover/rebalance 실측 | 로컬 검증됨, GCP 미검증 |
| E2E-LOG/METRIC/OBS | Ops Agent, metrics, dashboards | Terraform 모듈만 존재 |
| FT-MIGRATE/UPGRADE/SQL-FAIL | 혼합 fleet, canary, SQL failover | 미검증 |
| E2E-CUSTOM/CLEAN | custom metric, artifact cleanup | 미검증 |

## 실행 방법

```bash
gcloud auth login
scripts/e2e/run_gcp_e2e.sh <project-id> [region]
```

harness는 terraform apply → 예제 실행 → destroy 순서로 진행하며,
각 단계가 implementation-plan gate 이름을 출력한다. 어느 단계에서
실패했는지 바로 알 수 있다.

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
