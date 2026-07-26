# Distributed Runtime

Finite/Continuous workload를 제품 도메인과 분리해 실행하기 위한 Python runtime
프로젝트다. 현재 `0.1.0`은 **Milestone01 계약 계층**까지 구현되어 있으며, 실제 GCP
worker와 infrastructure는 아직 포함하지 않는다.

## 문서 시작점

상세 문서는 [`docs/README.md`](docs/README.md)에서 시작한다.

- 처음 사용한다면 [빠른 시작](docs/getting-started.md)
- 현재 구현 범위를 확인하려면
  [계약 계층 아키텍처](docs/architecture/current-contract-layer.md)
- 전체 Phase 1~5 목표는 [목표 아키텍처](docs/first.md)
- 구현 순서와 GCP E2E 완료 조건은 [구현 세부 계획](docs/implementation-plan.md)

## 현재 구현

- 강타입 identifier, 상태 enum, 구조화 오류
- canonical envelope와 immutable artifact reference
- Finite planner/handler/context 계약
- Continuous partition/lease/sink/context 계약
- workload/sink registry와 application facade
- typed configuration과 DB connection capacity 검증
- lifecycle, cancellation, deadline, structured logging 계약

아직 Cloud SQL repository, Pub/Sub adapter, worker loop, reconciler, Terraform, 실제 GCP
E2E는 구현되지 않았다. 현재와 목표 상태를 혼동하지 않도록 상세 문서에서는 두 범위를
명시적으로 구분한다.

## 개발 검증

```bash
uv sync --all-groups
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run mypy --strict src tests
uv run coverage run --branch -m pytest
uv run coverage report --fail-under=90
uv build
```

테스트 분류와 wheel 검증 범위는
[테스트와 품질 게이트](docs/development/testing-and-quality.md)를 참고한다.
