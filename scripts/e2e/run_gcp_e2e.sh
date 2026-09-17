#!/usr/bin/env bash
# Real-GCP E2E harness (implementation-plan.md §7).
#
# Provisions the temporary environment, deploys the runtime + example
# products, runs the in-VPC driver job (finite, fault injection, revision
# pin, duplicate dispatch, Pub/Sub DLQ, continuous spread/emissions,
# IAM separation, MIG scale, failover/fence, alert evidence), then
# destroys everything. See docs/development/gcp-e2e.md for the cost
# estimate and teardown contract.
#
# Usage:
#   scripts/e2e/run_gcp_e2e.sh <project-id> [region] [scenarios]
#
#   scenarios  optional RUNTIME_E2E_SCENARIOS comma-filter for the driver
#              (e.g. "finite,faults"); default runs every scenario.
#
# Requires: gcloud (authenticated, project owner/editor-equivalent),
# terraform >= 1.6, docker, python >= 3.12 with `build`.

set -euo pipefail

PROJECT="${1:?usage: run_gcp_e2e.sh <project-id> [region] [scenarios]}"
REGION="${2:-us-central1}"
SCENARIOS="${3:-}"
ENV_DIR="terraform/environments/local"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TAG="e2e-$(date +%Y%m%d%H%M)"
REPO="$REGION-docker.pkg.dev/$PROJECT/runtime"

phase() { printf '\n=== %s ===\n' "$1"; }

phase "preflight: credentials and tools"
command -v gcloud >/dev/null || { echo "gcloud not installed"; exit 1; }
command -v terraform >/dev/null || { echo "terraform not installed"; exit 1; }
command -v docker >/dev/null || { echo "docker not installed"; exit 1; }
gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q . \
  || { echo "no active gcloud account — run: gcloud auth login"; exit 1; }
gcloud config set project "$PROJECT"

cleanup() {
  phase "teardown"
  terraform -chdir="$ROOT/$ENV_DIR" destroy -auto-approve \
    -var="project=$PROJECT" -var="region=$REGION" \
    -var="worker_image=$REPO/runtime-e2e:$TAG" \
    -var="control_image=$REPO/runtime-e2e:$TAG" \
    -var="driver_image=$REPO/runtime-e2e-driver:$TAG" || true
}
trap cleanup EXIT

phase "wheels: runtime + example products"
cd "$ROOT"
rm -rf dist
python -m build --wheel --outdir dist .
python -m build --wheel --outdir dist examples/finite_example
python -m build --wheel --outdir dist examples/continuous_example

phase "images: build + push ($TAG)"
gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
docker build --platform linux/amd64 -t "$REPO/runtime-e2e:$TAG" .
docker build --platform linux/amd64 \
  --build-arg "BASE=runtime-e2e:$TAG" \
  -f scripts/e2e/Dockerfile.driver \
  -t "$REPO/runtime-e2e-driver:$TAG" scripts/e2e
docker push "$REPO/runtime-e2e:$TAG"
docker push "$REPO/runtime-e2e-driver:$TAG"

phase "terraform apply"
# DEPLOYER (optional): IAM member allowed to actAs the control SA — needed
# for Cloud Scheduler's oauth_token on the reconcile job. Full member
# string, e.g. DEPLOYER="user:you@example.com".
TF_VARS=(
  -var="project=$PROJECT" -var="region=$REGION"
  -var="worker_image=$REPO/runtime-e2e:$TAG"
  -var="control_image=$REPO/runtime-e2e:$TAG"
  -var="driver_image=$REPO/runtime-e2e-driver:$TAG"
)
if [[ -n "${DEPLOYER:-}" ]]; then
  TF_VARS+=(-var="deployer=$DEPLOYER")
fi
terraform -chdir="$ROOT/$ENV_DIR" init
terraform -chdir="$ROOT/$ENV_DIR" apply -auto-approve "${TF_VARS[@]}"

phase "schema migration (in-VPC job)"
gcloud run jobs execute runtime-control-migrate \
  --project "$PROJECT" --region "$REGION" --wait

phase "E2E driver job"
EXECUTION=$(gcloud run jobs execute runtime-control-e2e-driver \
  --project "$PROJECT" --region "$REGION" --async \
  ${SCENARIOS:+--update-env-vars "RUNTIME_E2E_SCENARIOS=$SCENARIOS"} \
  --format='value(metadata.name)')
echo "driver execution: $EXECUTION"

until gcloud run jobs executions describe "$EXECUTION" \
    --project "$PROJECT" --region "$REGION" \
    --format='value(status.conditions[0].status)' 2>/dev/null | grep -q True; do
  sleep 15
done

gcloud logging read \
  "resource.type=cloud_run_job AND resource.labels.job_name=runtime-control-e2e-driver AND labels.\"run.googleapis.com/execution_name\"=\"$EXECUTION\"" \
  --project "$PROJECT" --format='value(textPayload)' --freshness=2h \
  | sort -u | grep '\[e2e\]' || true

STATUS=$(gcloud run jobs executions describe "$EXECUTION" \
  --project "$PROJECT" --region "$REGION" \
  --format='value(status.completionTime,status.succeededCount)')
echo "driver execution finished: $STATUS"

if ! gcloud run jobs executions describe "$EXECUTION" \
    --project "$PROJECT" --region "$REGION" \
    --format='value(status.succeededCount)' | grep -q '1'; then
  echo "E2E FAILED — driver execution did not succeed" >&2
  exit 1
fi
echo "E2E PASSED — teardown runs via the EXIT trap"
