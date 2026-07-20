"""Package scaffold gates that run before feature modules exist."""

from __future__ import annotations

import importlib
import importlib.metadata
import pkgutil
from pathlib import Path

import distributed_runtime

EXPECTED_SUBPACKAGES = {"control", "core", "finite", "gcp", "testing", "worker"}


def test_distribution_metadata_matches_runtime_version() -> None:
    assert importlib.metadata.version("distributed-runtime") == distributed_runtime.__version__


def test_scaffold_exports_version_and_namespace_packages() -> None:
    assert distributed_runtime.__version__ == "0.1.0"
    discovered = {
        module.name for module in pkgutil.iter_modules(distributed_runtime.__path__) if module.ispkg
    }
    assert discovered == EXPECTED_SUBPACKAGES
    for package_name in sorted(EXPECTED_SUBPACKAGES):
        assert importlib.import_module(f"distributed_runtime.{package_name}").__package__


def test_distribution_exposes_planned_optional_features() -> None:
    metadata = importlib.metadata.metadata("distributed-runtime")
    assert set(metadata.get_all("Provides-Extra") or ()) == {"gcp", "testing"}


def test_typed_marker_is_packaged() -> None:
    assert (Path(distributed_runtime.__file__).parent / "py.typed").is_file()
