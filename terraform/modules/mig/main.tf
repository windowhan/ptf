variable "project" {
  type = string
}
variable "region" {
  type = string
}
variable "name" {
  type    = string
  default = "runtime-worker"
}
variable "machine_type" {
  type    = string
  default = "e2-micro"
}
variable "subnet_id" {
  type    = string
  default = ""
}
variable "network" {
  type    = string
  default = ""
}
variable "service_account" {
  type = string
}
variable "image" {
  type = string
}
variable "min_replicas" {
  type    = number
  default = 2
}
variable "max_replicas" {
  type    = number
  default = 10
}
variable "env" {
  type    = map(string)
  default = {}
}
variable "role" {
  type    = string
  default = "worker"
}
# Spot/preemptible provisioning — cheaper, and each recreation lands a new
# ephemeral external IP (free egress-IP rotation for crawl workers).
variable "spot" {
  type    = bool
  default = false
}
# Attach an ephemeral external IPv4 — required when egress IP diversity is
# the point (crawling) and no Cloud NAT is in the path.
variable "external_ip" {
  type    = bool
  default = false
}
# Fixed-size pools (spot IP workers) skip the autoscaler entirely.
variable "autoscaled" {
  type    = bool
  default = true
}
# Extra shell run after secret fetch, before image pull — e.g. tailscale up.
variable "startup_prelude" {
  type    = string
  default = ""
}
# env name -> Secret Manager version resource; fetched at boot and passed
# to the container as -e NAME="$NAME".
variable "secret_env" {
  type    = map(string)
  default = {}
}
# env name -> Secret Manager version resource; fetched into shell vars for
# startup_prelude only — never passed to the container.
variable "secret_shell" {
  type    = map(string)
  default = {}
}

locals {
  registry_host = "${split("-docker.pkg.dev", var.image)[0]}-docker.pkg.dev"
  env_flags     = join(" ", [for k, v in var.env : "-e ${k}='${v}'"])
  # Bare `-e NAME` — docker reads the value from the exported shell env,
  # so the secret never appears in the process command line.
  secret_flags = join(" ", [for k in keys(var.secret_env) : "-e ${k}"])
  all_secrets  = merge(var.secret_shell, var.secret_env)
  # Pull each secret version over the metadata-token identity, into shell
  # vars — retried so a transient Secret Manager error at boot doesn't
  # leave the instance stranded without its runtime config.
  secret_helper = length(local.all_secrets) == 0 ? "" : <<-EOT2
    fetch_secret() {
      for i in $(seq 1 10); do
        # googleapis responses are pretty-printed multi-line JSON — sed -n
        # must drop non-matching lines or raw JSON reaches base64.
        V=$(curl -sf -H "Authorization: Bearer $TOKEN" \
          "https://secretmanager.googleapis.com/v1/$1:access" \
          | sed -nE 's/.*"data": *"([^"]+)".*/\1/p' | base64 -d)
        if [ -n "$V" ]; then echo "$V"; return 0; fi
        sleep 3
      done
      echo "fetch_secret failed for $1" >&2
      return 1
    }
  EOT2
  secret_fetch = join("\n    ", [
    for k, v in local.all_secrets : "export ${k}=$(fetch_secret \"${v}\")"
  ])
  startup_script = <<-EOT
    #!/bin/bash
    set -e
    # COS root fs is read-only; keep docker client config on the stateful partition
    export DOCKER_CONFIG=/var/lib/runtime-docker
    mkdir -p "$DOCKER_CONFIG"
    # Auth docker to Artifact Registry using the VM service account token.
    # Retry: the network path to *.pkg.dev can take a moment after boot.
    for i in $(seq 1 30); do
      TOKEN=$(curl -s -H "Metadata-Flavor: Google" \
        http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token \
        | sed -nE 's/.*"access_token": *"([^"]+)".*/\1/p')
      if [ -n "$TOKEN" ] && echo "$TOKEN" | docker login -u oauth2accesstoken \
        --password-stdin "https://${local.registry_host}"; then
        break
      fi
      sleep 5
    done
    ${local.secret_helper}
    ${local.secret_fetch}
    ${var.startup_prelude}
    until docker pull "${var.image}"; do sleep 5; done
    # The worker must tolerate booting before the schema migration job has
    # run (the MIG comes up during apply, migrate runs after) and transient
    # DB/control-plane outages — restart on exit instead of giving up.
    until docker run --rm --name worker --network host ${local.env_flags} ${local.secret_flags} "${var.image}" ${var.role}; do
      echo "worker exited; restarting in 10s"
      sleep 10
    done
  EOT
}

resource "google_compute_instance_template" "worker" {
  project     = var.project
  name_prefix = "${var.name}-"
  region      = var.region

  machine_type = var.machine_type

  disk {
    source_image = "projects/cos-cloud/global/images/family/cos-stable"
    auto_delete  = true
    boot         = true
    disk_size_gb = 20
  }

  network_interface {
    network    = var.subnet_id == "" ? var.network : null
    subnetwork = var.subnet_id != "" ? var.subnet_id : null
    dynamic "access_config" {
      for_each = var.external_ip ? [1] : []
      content {}
    }
  }

  scheduling {
    automatic_restart   = !var.spot
    on_host_maintenance = var.spot ? "TERMINATE" : "MIGRATE"
    preemptible         = var.spot
    provisioning_model  = var.spot ? "SPOT" : "STANDARD"
  }

  service_account {
    email  = var.service_account
    scopes = ["cloud-platform"]
  }

  metadata = {
    "google-logging-enabled" = "true"
  }

  metadata_startup_script = local.startup_script

  lifecycle { create_before_destroy = true }
}

resource "google_compute_region_instance_group_manager" "workers" {
  project            = var.project
  name               = var.name
  region             = var.region
  base_instance_name = var.name
  target_size        = var.min_replicas

  version {
    instance_template = google_compute_instance_template.worker.id
  }

  # Roll instances onto new templates without manual applyUpdates calls
  update_policy {
    type                           = "PROACTIVE"
    minimal_action                 = "REPLACE"
    most_disruptive_allowed_action = "REPLACE"
    # regional MIGs require these fixed values to be 0 or >= zone count
    max_surge_fixed       = 3
    max_unavailable_fixed = 0
    # Proactive zone rebalancing churns instances during stockouts — a
    # zone that can't provision gets retried forever. ANY lets the MIG
    # place workers wherever capacity exists.
    instance_redistribution_type = "NONE"
  }

  distribution_policy_target_shape = "ANY"

  # The autoscaler owns target_size after creation — resizing an autoscaled
  # MIG from terraform is rejected (412). Set it once at create time and
  # let the autoscaler (or the E2E driver's scale scenario) manage it.
  lifecycle { ignore_changes = [target_size] }
}

resource "google_compute_region_autoscaler" "workers" {
  count   = var.autoscaled ? 1 : 0
  project = var.project
  name    = "${var.name}-autoscaler"
  region  = var.region
  target  = google_compute_region_instance_group_manager.workers.id

  autoscaling_policy {
    min_replicas = var.min_replicas
    max_replicas = var.max_replicas
    cpu_utilization { target = 0.7 }
    cooldown_period = 120
    # Default scale-in stabilization is 600s — far longer than the E2E
    # scale-down gate. 60s keeps the scenario inside its wait budget while
    # still exercising the real autoscaler path.
    scale_in_control {
      time_window_sec = 60
      max_scaled_in_replicas { fixed = 1 }
    }
  }
}

output "mig_name" { value = google_compute_region_instance_group_manager.workers.name }
output "template_id" { value = google_compute_instance_template.worker.id }
output "startup_script" { value = local.startup_script }
