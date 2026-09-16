#!/usr/bin/env bash
# Real-GCP E2E harness (implementation-plan.md #50-56).
#
# Requires: gcloud authenticated against a project you own, terraform >= 1.6,
# docker for image builds. Everything here costs real money — see
# docs/development/gcp-e2e.md for the cost estimate and teardown contract.
#
# Usage:
#   scripts/e2e/run_gcp_e2e.sh <project-id> [region]
#
# The script is intentionally linear and loud: each phase prints its gate
# name so a failure maps directly to an implementation-plan gate.

set -euo pipefail

PROJECT="${1:?usage: run_gcp_e2e.sh <project-id> [region]}"
REGION="${2:-us-central1}"
ENV_DIR="terraform/environments/local"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

phase() { printf '\n=== %s ===\n' "$1"; }

phase "preflight: credentials and tools"
command -v gcloud >/dev/null || { echo "gcloud not installed"; exit 1; }
command -v terraform >/dev/null || { echo "terraform not installed"; exit 1; }
gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q . \
  || { echo "no active gcloud account — run: gcloud auth login"; exit 1; }
gcloud config set project "$PROJECT"

phase "E2E-BOOT: terraform apply"
terraform -chdir="$ROOT/$ENV_DIR" init
terraform -chdir="$ROOT/$ENV_DIR" apply -auto-approve \
  -var="project=$PROJECT" -var="region=$REGION"

phase "image build and push"
WORKER_IMAGE="$REGION-docker.pkg.dev/$PROJECT/runtime/runtime-worker:e2e"
CONTROL_IMAGE="$REGION-docker.pkg.dev/$PROJECT/runtime/runtime-control:e2e"
# TODO(#50): build runtime-worker/runtime-control images once the worker
# entrypoint lands; until then the MIG/Cloud Run modules take image vars.

phase "E2E-FIN: finite example through the real path"
# TODO(#51): submit examples/finite_example via the runtime client against
# the deployed control service; verify results and teardown run state.

phase "E2E-CON: continuous example failover"
# TODO(#53): deploy examples/continuous_example, kill a worker instance,
# verify the reconciler reclaims its partitions under new fencing tokens.

phase "teardown"
terraform -chdir="$ROOT/$ENV_DIR" destroy -auto-approve \
  -var="project=$PROJECT" -var="region=$REGION"
echo "E2E complete — all resources destroyed."
