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
  type = string
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

locals {
  registry_host  = "${split("-docker.pkg.dev", var.image)[0]}-docker.pkg.dev"
  env_flags      = join(" ", [for k, v in var.env : "-e ${k}='${v}'"])
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
        | sed -E 's/.*"access_token": ?"([^"]+)".*/\1/')
      if [ -n "$TOKEN" ] && echo "$TOKEN" | docker login -u oauth2accesstoken \
        --password-stdin "https://${local.registry_host}"; then
        break
      fi
      sleep 5
    done
    until docker pull "${var.image}"; do sleep 5; done
    docker run --rm --name worker --network host ${local.env_flags} "${var.image}" ${var.role}
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
    subnetwork = var.subnet_id
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
  }
}

resource "google_compute_region_autoscaler" "workers" {
  project = var.project
  name    = "${var.name}-autoscaler"
  region  = var.region
  target  = google_compute_region_instance_group_manager.workers.id

  autoscaling_policy {
    min_replicas = var.min_replicas
    max_replicas = var.max_replicas
    cpu_utilization { target = 0.7 }
    cooldown_period = 120
  }
}

output "mig_name" { value = google_compute_region_instance_group_manager.workers.name }
output "template_id" { value = google_compute_instance_template.worker.id }
