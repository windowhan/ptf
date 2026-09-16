"""Contract tests for the installable package scaffold."""

from __future__ import annotations

import importlib
import importlib.metadata
import pkgutil
import sys
from pathlib import Path

import distributed_runtime

EXPECTED_SUBPACKAGES = {
    "continuous",
    "control",
    "core",
    "finite",
    "gcp",
    "state",
    "testing",
    "worker",
}
EXPECTED_EXTRAS = {"gcp", "postgres", "testing"}


def test_distribution_metadata_matches_runtime_version() -> None:
    """The distribution and import package expose one version."""
    assert importlib.metadata.version("distributed-runtime") == distributed_runtime.__version__


def test_distribution_exposes_planned_optional_features() -> None:
    """Consumers can request stable GCP and testing extras."""
    metadata = importlib.metadata.metadata("distributed-runtime")

    assert set(metadata.get_all("Provides-Extra") or ()) == EXPECTED_EXTRAS


def test_root_facade_is_intentionally_minimal() -> None:
    """The root publishes only the application entry point and version."""
    assert distributed_runtime.__all__ == ["RuntimeApplication", "__version__"]
    assert distributed_runtime.__version__ == "0.1.0"


def test_expected_subpackages_are_installable() -> None:
    """Every repository target namespace is included in the wheel package."""
    discovered = {
        module.name for module in pkgutil.iter_modules(distributed_runtime.__path__) if module.ispkg
    }

    assert discovered == EXPECTED_SUBPACKAGES
    for package_name in sorted(EXPECTED_SUBPACKAGES):
        module = importlib.import_module(f"distributed_runtime.{package_name}")
        assert module.__package__ == f"distributed_runtime.{package_name}"


def test_base_import_does_not_load_google_clients() -> None:
    """Importing the public root must stay free of optional cloud clients."""
    loaded_google_modules = {
        module_name
        for module_name in sys.modules
        if module_name == "google" or module_name.startswith("google.")
    }

    assert loaded_google_modules == set()


def test_typed_marker_is_packaged() -> None:
    """PEP 561 consumers can detect the distribution's typing support."""
    package_path = Path(distributed_runtime.__file__).parent

    assert (package_path / "py.typed").is_file()
