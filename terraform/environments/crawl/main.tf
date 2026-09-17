# Crawl-worker pool: IP-diverse spot workers that join a Tailscale
# tailnet and poll the operator's local Postgres for units.
#
# Topology: orchestrator + state DB run outside GCP (e.g. the operator's
# machine). Each spot VM gets its own ephemeral external IPv4 — that
# address is the crawl egress IP, and spot recreation rotates it for
# free. Workers reach the DB over the tailnet, so nothing here needs
# Cloud SQL, Pub/Sub, or a custom VPC.
#
# Quotas that bound the pool size: IN_USE_ADDRESSES (8/region),
# INSTANCES (24/region), CPUS_ALL_REGIONS (32 global). For more than
# ~8 workers per region, pass additional entries in `regions`.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0, < 7"
    }
  }
}

variable "project" {
  type = string
}
# One MIG per region — external-IP quota is regional, so spreading
# regions multiplies the distinct egress IPs available.
variable "regions" {
  type    = list(string)
  default = ["asia-northeast3"] # Seoul — Korean egress for naver targets
}
variable "size_per_region" {
  type    = number
  default = 4
}
variable "machine_type" {
  type    = string
  default = "e2-micro"
}
# Worker image bundling crawler_product (see examples/crawler_example).
variable "image" {
  type = string
}
# Workers only claim runs pinned to this execution revision — submit with
# execution_revision="crawl" and local/other pools won't race them.
variable "pool_revision" {
  type    = string
  default = "crawl"
}
variable "finite_app" {
  type    = string
  default = "crawler_product.app:build_application"
}
# Create the Artifact Registry repo. Set false when the repo is already
# managed by another root (e.g. environments/local).
variable "create_registry" {
  type    = bool
  default = true
}
variable "tailscale_version" {
  type    = string
  default = "1.102.4"
}

provider "google" {
  project = var.project
  region  = var.regions[0]
}

resource "google_project_service" "apis" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "compute.googleapis.com",
    "secretmanager.googleapis.com",
  ])
  project            = var.project
  service            = each.key
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "runtime" {
  count         = var.create_registry ? 1 : 0
  project       = var.project
  location      = "us-central1"
  repository_id = "runtime"
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}

# Secret containers only — the operator adds versions out-of-band:
#   echo -n 'tskey-auth-...' | gcloud secrets versions add crawl-tailscale-auth-key --data-file=-
#   echo -n 'postgresql://user:pass@<tailscale-ip>:55432/runtime' \
#     | gcloud secrets versions add crawl-worker-dsn --data-file=-
resource "google_secret_manager_secret" "tailscale_auth_key" {
  project   = var.project
  secret_id = "crawl-tailscale-auth-key"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret" "worker_dsn" {
  project   = var.project
  secret_id = "crawl-worker-dsn"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_service_account" "worker" {
  project      = var.project
  account_id   = "crawl-worker"
  display_name = "Crawl workers (tailnet, spot)"
  depends_on   = [google_project_service.apis]
}

resource "google_secret_manager_secret_iam_member" "worker_ts" {
  project   = var.project
  secret_id = google_secret_manager_secret.tailscale_auth_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_secret_manager_secret_iam_member" "worker_dsn" {
  project   = var.project
  secret_id = google_secret_manager_secret.worker_dsn.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_project_iam_member" "worker_registry" {
  project = var.project
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_project_iam_member" "worker_logging" {
  project = var.project
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.worker.email}"
}

locals {
  # COS has no package manager — run tailscaled from static binaries on the
  # persistent /var/lib partition. The daemon runs on the host so the worker
  # container (--network host) reaches the tailnet directly.
  tailscale_prelude = <<-EOT
    TS_DIR=/var/lib/tailscale
    if [ ! -x "$TS_DIR/tailscaled" ]; then
      mkdir -p "$TS_DIR"
      curl -fsSL "https://pkgs.tailscale.com/stable/tailscale_${var.tailscale_version}_amd64.tgz" \
        | tar -xz -C "$TS_DIR" --strip-components=1
    fi
    mkdir -p "$TS_DIR/state"
    (nohup "$TS_DIR/tailscaled" --statedir="$TS_DIR/state" >/var/log/tailscaled.log 2>&1 &)
    for i in $(seq 1 30); do
      if "$TS_DIR/tailscale" up \
          --authkey="$TAILSCALE_AUTH_KEY" \
          --hostname="crawl-$(hostname | cut -d. -f1)"; then
        break
      fi
      sleep 2
    done
    "$TS_DIR/tailscale" status || true
  EOT
}

module "workers" {
  source   = "../../modules/mig"
  for_each = toset(var.regions)

  project         = var.project
  region          = each.key
  name            = "crawl-worker"
  machine_type    = var.machine_type
  network         = "default"
  external_ip     = true # the crawl egress IP — one per instance
  spot            = true # cheap; recreation rotates the IP
  autoscaled      = false
  min_replicas    = var.size_per_region
  service_account = google_service_account.worker.email
  image           = var.image
  env = {
    RUNTIME_POOL_REVISION = var.pool_revision
    RUNTIME_FINITE_APP    = var.finite_app
    RUNTIME_POLL_SECONDS  = "2"
  }
  secret_env = {
    RUNTIME_DSN = "${google_secret_manager_secret.worker_dsn.id}/versions/latest"
  }
  secret_shell = {
    TAILSCALE_AUTH_KEY = "${google_secret_manager_secret.tailscale_auth_key.id}/versions/latest"
  }
  startup_prelude = local.tailscale_prelude
  depends_on      = [google_project_service.apis]
}

output "mig_names" { value = { for r, m in module.workers : r => m.mig_name } }
