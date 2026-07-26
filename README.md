# Distributed Runtime

이 프로젝트는 여러 종류의 작업을 공통된 방법으로 실행하기 위한 Python 도구다.

- **Finite 작업:** 처리할 양이 정해져 있고 끝나는 작업
- **Continuous 작업:** 계속 실행하면서 새 데이터를 처리하는 작업

현재 버전 `0.1.0`에는 작업을 정의하고 검사하는 **Milestone01 계약 계층**만 있다.
실제 GCP worker와 infrastructure는 아직 없다.

## 문서 시작점

상세 문서는 [`docs/README.md`](docs/README.md)에서 시작한다.

- 처음 사용한다면 [빠른 시작](docs/getting-started.md)
- 현재 구현 범위를 확인하려면
  [계약 계층 아키텍처](docs/architecture/current-contract-layer.md)
- 전체 Phase 1~5 목표는 [목표 아키텍처](docs/first.md)
- 구현 순서와 GCP E2E 완료 조건은 [구현 세부 계획](docs/implementation-plan.md)
- 낯선 용어는 [쉬운 용어 설명](docs/glossary.md)

## 현재 구현

- 잘못된 값을 막는 ID와 상태 값
- 같은 데이터를 항상 같은 형태로 만드는 메시지 형식
- 큰 파일의 위치와 내용이 맞는지 확인하는 참조
- Finite 작업을 나누고 처리하기 위한 규칙
- Continuous 작업의 구역과 소유권을 관리하기 위한 규칙
- workload와 결과 목적지를 등록하고 찾는 기능
- worker 수와 DB 연결 수가 안전한지 확인하는 설정
- 작업 중단, 마감 시각, 안전한 종료, 구조화 로그 규칙

Cloud SQL 저장 코드, Pub/Sub 연결, 실제 worker, 상태 조정 작업, Terraform, GCP
전체 경로 검증은 아직 없다. 상세 문서는 **지금 되는 것**과 **앞으로 만들 것**을
구분해서 설명한다.

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
