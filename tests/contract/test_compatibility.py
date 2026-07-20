"""CT-COMPAT: committed snapshots for compatibility-sensitive contracts."""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import distributed_runtime
import distributed_runtime.continuous as continuous
import distributed_runtime.core as core
import distributed_runtime.finite as finite
from distributed_runtime.registry import RuntimeRegistry

_SNAPSHOT_DIRECTORY = Path(__file__).with_name("snapshots")


def _snapshot(name: str) -> dict[str, Any]:
    value = json.loads((_SNAPSHOT_DIRECTORY / f"{name}.json").read_text())
    assert isinstance(value, dict)
    return value


def _parameters(callable_object: Callable[..., object]) -> list[dict[str, object]]:
    return [
        {
            "name": parameter.name,
            "kind": parameter.kind.name,
            "required": parameter.default is inspect.Parameter.empty,
        }
        for parameter in inspect.signature(callable_object).parameters.values()
    ]


def test_public_api_snapshot() -> None:
    methods = (
        "register_continuous",
        "register_finite",
        "register_sink",
        "sink",
        "workload",
        "workloads",
    )
    actual = {
        "root_exports": distributed_runtime.__all__,
        "core_exports": core.__all__,
        "finite_exports": finite.__all__,
        "continuous_exports": continuous.__all__,
        "registry_methods": {name: _parameters(getattr(RuntimeRegistry, name)) for name in methods},
    }

    assert actual == _snapshot("api")
