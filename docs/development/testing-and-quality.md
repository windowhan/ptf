# 테스트와 품질 게이트

이 문서는 변경을 끝냈다고 판단하기 전에 무엇을 확인해야 하는지 설명한다.

## 기본 생각

현재 계약 계층은 GCP에 접속하지 않고도 모두 검사할 수 있어야 한다.

- unit test: 클래스와 작은 함수 하나의 동작을 확인한다.
- contract test: 메시지 형식과 공개 API가 실수로 바뀌지 않았는지 확인한다.
- package test: 개발 환경에서 package 정보와 공개 이름을 확인한다.
- isolated wheel smoke: 새 환경에 wheel을 설치해 실제 import가 되는지 확인한다.
- hostile test: Python이 비슷하게 취급하는 잘못된 값을 의도적으로 넣어 본다.

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

모든 명령이 성공해야 한다. 마지막에는 `git status --short`로 테스트가 추적 파일을
바꾸지 않았는지도 확인한다.

## 현재 테스트 구성

| 파일 | 테스트 수 | 쉽게 말하면 |
|---|---:|---|
| `tests/contract/test_envelope.py` | 37 | 메시지와 artifact가 항상 같은 형식을 지키는지 |
| `tests/contract/test_public_api.py` | 27 | 공개 이름, registry, 버전 형식이 맞는지 |
| `tests/unit/test_core.py` | 24 | ID, 오류, 로그가 올바르게 동작하는지 |
| `tests/unit/test_config.py` | 23 | worker와 DB 연결 설정이 안전한지 |
| `tests/unit/test_finite_contracts.py` | 21 | 계획 ID, 변경 감지, 중단이 동작하는지 |
| `tests/unit/test_lifecycle.py` | 15 | 시각, deadline, 안전한 종료가 동작하는지 |
| `tests/unit/test_continuous_contracts.py` | 8 | partition, lease, sink 규칙을 지키는지 |
| `tests/test_package.py` | 6 | package 정보와 import가 올바른지 |
| `tests/contract/test_compatibility.py` | 3 | 공개 API, 메시지, 설정 기준 파일과 같은지 |

현재 총 164개 테스트가 있고 branch coverage는 95%다.

이 숫자는 계속 바뀔 수 있다. 테스트를 추가하거나 삭제하면 이 표도 함께 고친다.

## 중요한 두 가지 테스트 묶음

### CT-ENV: 외부 메시지를 믿기 전에 확인하는 것

`tests/contract/test_envelope.py`

확인하는 내용:

- V1 메시지를 썼다가 다시 읽어도 같은 값인지
- 같은 메시지가 항상 같은 bytes가 되는지
- 지원하지 않는 version을 거부하는지
- inline payload와 artifact 중 정확히 하나만 사용하는지
- 메시지 크기가 256 KiB를 넘지 않는지
- 중첩된 JSON 값이 나중에 바뀌지 않는지
- 문자열이 아닌 key를 거부하는지
- 정수 자리에 boolean이나 float를 받지 않는지
- 다른 application의 artifact를 사용하지 못하는지

### UT-CONFIG: 잘못된 설정으로 실행하지 않는지

`tests/unit/test_config.py`

확인하는 내용:

- Finite/Continuous 기본값
- heartbeat, lease, takeover 시간 관계
- pool 종류별 자동 확장 규칙
- execution class와 pool 연결
- secret version 고정
- 생성 뒤 설정 dictionary가 바뀌지 않는지
- worker가 최대로 늘어났을 때 DB 연결 한도를 넘는지

## 정상처럼 보이는 잘못된 값도 테스트하기

Python은 `True == 1`처럼 서로 다른 값을 같게 취급할 때가 있다. decoder가 잘못된
입력을 조용히 받아들일 수도 있다. 이런 경계값을 일부러 테스트한다.

예:

- 정수 field에 `True` 전달
- fencing token에 `1.0` 전달
- 다른 문자권 숫자가 포함된 SemVer
- dictionary에 문자열이 아닌 추가 key 전달
- NaN/Infinity
- 값은 있지만 `False`로 평가되는 custom mapping
- 같은 partition ID 또는 unit key를 두 번 전달
- 서로 다른 clock으로 parent/child deadline 생성

이런 버그를 고칠 때는 정상 입력 테스트만 추가하지 않는다. 버그를 다시 만들 수 있는
잘못된 입력 테스트를 먼저 추가한다.

## 공개 형식 기준 파일

snapshot 파일:

- `tests/contract/snapshots/api.json`
- `tests/contract/snapshots/envelope.json`
- `tests/contract/snapshots/config.json`

snapshot이 달라졌다면 공개 형식이 달라졌다는 뜻이다. 새 결과로 파일을 덮어쓰고
끝내지 않는다. 무엇이 바뀌었고 기존 사용자에게 어떤 영향이 있는지 리뷰한다.

## Package와 wheel 검증

package 검사는 두 단계로 나뉜다.

1. `tests/test_package.py`는 현재 개발 환경에서 version, extras, 공개 진입점,
   subpackage, `py.typed`, Google client 미로딩을 확인한다.
2. CI는 빈 virtual environment에 wheel을 설치하고 root와 `gcp` package를 실제로
   불러온다.

로컬에서 두 번째 층까지 재현하는 명령:

```bash
uv build
uv venv --python 3.12 .venv-wheel
uv pip install --python .venv-wheel/bin/python dist/*.whl
.venv-wheel/bin/python -c \
  "import distributed_runtime; import distributed_runtime.gcp"
```

각 단계가 지금 직접 확인하는 범위:

| 검사 | `test_package.py` | CI metadata | isolated wheel |
|---|---:|---:|---:|
| runtime version과 distribution version 일치 | O | - | - |
| `gcp`, `testing` extras | O | O | - |
| 최소 root 진입점과 예상 subpackage | O | - | import만 |
| `py.typed` | O | - | - |
| Google client 미로딩 | O | - | O |
| wheel의 distribution name | - | O | 설치 성공 |

지원 Python version과 runtime dependency는 현재 `pyproject.toml` 선언과 build 결과에
의존한다. 전용 테스트는 없다. release에서 반드시 막아야 하는 조건으로 올릴 때는
wheel의 `METADATA`를 직접 검사하는 CI 단계가 필요하다.

## CI

`.github/workflows/ci.yml`은 Python 3.12, 3.13, 3.14 matrix에서 다음을 실행한다.

1. `uv sync --all-groups`
2. Ruff lint/format
3. strict mypy
4. branch coverage 90% gate
5. sdist/wheel build
6. wheel name/extras metadata 검사
7. isolated installed-wheel smoke

로컬에서 Python 3.12만 실행했다면 3.13과 3.14 지원 여부는 CI 결과로 확인한다.

## 변경 유형별 최소 검증

| 바꾼 영역 | 최소한 실행할 검사 |
|---|---|
| identifier/error/logging | `test_core.py`, Ruff, mypy |
| envelope/artifact | CT-ENV, Ruff, mypy |
| config | UT-CONFIG, Ruff, mypy |
| Finite contract | finite unit tests, CT-ENV 영향 확인 |
| Continuous contract | continuous unit tests, fencing hostile cases |
| registry/public API | public API + compatibility snapshot |
| package metadata | full build + isolated wheel smoke |
| lifecycle | lifecycle tests + finite/continuous cancellation 영향 |

작업 중에는 관련 테스트만 빠르게 실행할 수 있다. 하지만 병합하기 전에는 반드시
문서 앞부분의 전체 로컬 게이트를 모두 실행한다.
