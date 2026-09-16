"""Deployable process entrypoints for the runtime.

The library ships as a wheel; the deploy package assembles the pieces into
long-running processes: ``worker`` (MIG instances), ``control`` (Cloud Run
control plane), and ``migrate`` (one-shot schema setup). Configuration is
environment-only so the same image serves every role.
"""

from distributed_runtime.deploy.config import DeployConfig

__all__ = ["DeployConfig"]
