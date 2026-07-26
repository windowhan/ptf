# 테스트와 품질 게이트

## 기본 원칙

계약 계층은 외부 GCP 리소스 없이 완전히 검증 가능해야 한다.

- unit test는 개별 value object와 helper의 의미를 검증한다.
- contract test는 직렬화, 공개 API, boundary 호환성을 검증한다.
- package smoke는 source tree가 아닌 설치된 wheel을 검증한다.
- 잘못된 입력을 보정하지 않고 fail-closed 동작을 테스트한다.

## 전체 로컬 게이트

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

각 명령이 성공한 뒤 `git status --short`가 깨끗해야 한다.

## 현재 테스트 구성

| 파일 | 현재 cases | 범위 |
|---|---:|---|
| `tests/contract/test_envelope.py` | 37 | CT-ENV, artifact, canonical JSON |
| `tests/contract/test_public_api.py` | 27 | exports, registry, SemVer |
| `tests/unit/test_core.py` | 24 | identifiers, errors, logging |
| `tests/unit/test_config.py` | 23 | UT-CONFIG, pool/capacity |
| `tests/unit/test_finite_contracts.py` | 21 | plan identity/drift/cancellation |
| `tests/unit/test_lifecycle.py` | 15 | clock/deadline/shutdown |
| `tests/unit/test_continuous_contracts.py` | 8 | partition/lease/sink |
| `tests/test_package.py` | 6 | package metadata/import |
| `tests/contract/test_compatibility.py` | 3 | API/envelope/config snapshot |

총 164 cases이며 Milestone01 최종 branch coverage는 95%다.

숫자는 문서의 보장 자체가 아니다. 테스트가 추가되면 표도 함께 갱신한다.

## Named gate

### CT-ENV

`tests/contract/test_envelope.py`

검증 범위:

- V1 round trip
- canonical bytes
- version gate
- inline/artifact exactly-one
- 256 KiB limit
- immutable nested JSON
- non-string key rejection
- exact integer validation
- application-scoped artifact

### UT-CONFIG

`tests/unit/test_config.py`

검증 범위:

- Finite/Continuous policy defaults
- timing relationship
- pool mode별 scaling 규칙
- execution class routing
- explicit secret version
- immutable mapping
- worst-case connection capacity

## Hostile boundary test

Python의 암묵적 동등성이나 관대한 decoder 때문에 생길 수 있는 입력을 의도적으로
테스트한다.

예:

- `True`를 integer field로 전달
- `1.0`을 fencing token으로 전달
- Unicode decimal digit가 포함된 SemVer
- mapping의 non-string extra key
- NaN/Infinity
- falsey custom mapping
- 동일 partition/unit key 중복
- 서로 다른 clock domain

boundary bug를 수정할 때는 정상 경로 test만 추가하지 말고 재현 가능한 hostile case를
먼저 추가한다.

## Compatibility snapshot

snapshot 파일:

- `tests/contract/snapshots/api.json`
- `tests/contract/snapshots/envelope.json`
- `tests/contract/snapshots/config.json`

snapshot 변경은 자동 포맷 결과로 취급하지 않는다. 어떤 public contract가 바뀌었고
호환성 영향이 무엇인지 리뷰해야 한다.

## Wheel 검증

```bash
uv build
uv venv --python 3.12 .venv-wheel
uv pip install --python .venv-wheel/bin/python dist/*.whl
.venv-wheel/bin/python -c \
  "import distributed_runtime; import distributed_runtime.gcp"
```

검사 항목:

- package name/version
- Python requirement
- `gcp`, `testing` extras
- zero runtime `Requires-Dist`
- `py.typed`
- 최소 root facade
- source checkout가 아닌 site-packages import
- `distributed_runtime.gcp` import 시 Google module 미로딩

## CI

`.github/workflows/ci.yml`은 Python 3.12, 3.13, 3.14 matrix에서 다음을 실행한다.

1. `uv sync --all-groups`
2. Ruff lint/format
3. strict mypy
4. branch coverage 90% gate
5. sdist/wheel build
6. wheel metadata 검사
7. isolated installed-wheel smoke

로컬에서 Python 3.12만 검증했다면 나머지 버전의 최종 증거는 CI 결과다.

## 변경 유형별 최소 검증

| 변경 | 최소 게이트 |
|---|---|
| identifier/error/logging | `test_core.py`, Ruff, mypy |
| envelope/artifact | CT-ENV, Ruff, mypy |
| config | UT-CONFIG, Ruff, mypy |
| Finite contract | finite unit tests, CT-ENV 영향 확인 |
| Continuous contract | continuous unit tests, fencing hostile cases |
| registry/public API | public API + compatibility snapshot |
| package metadata | full build + isolated wheel smoke |
| lifecycle | lifecycle tests + finite/continuous cancellation 영향 |

최종 병합 전에는 변경 범위와 무관하게 전체 게이트를 실행한다.
