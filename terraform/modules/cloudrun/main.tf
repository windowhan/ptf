variable "project" {
  type = string
}
variable "region" {
  type = string
}
variable "name" {
  type    = string
  default = "runtime-control"
}
variable "image" {
  type = string
}
variable "service_account" {
  type = string
}
variable "env" {
  type    = map(string)
  default = {}
}
# Direct VPC egress so the control plane reaches the Cloud SQL private IP;
# public API traffic (Pub/Sub, Secret Manager) uses the default egress.
variable "network_id" {
  type = string
}
variable "subnet_id" {
  type = string
}
variable "args" {
  type    = list(string)
  default = ["control"]
}
# The control process is a loop, not request-driven — it needs at least one
# instance running. Keep 0 for request-driven services; raise for control.
variable "min_instance_count" {
  type    = number
  default = 0
}

resource "google_cloud_run_v2_service" "control" {
  project             = var.project
  name                = var.name
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  deletion_protection = false

  template {
    service_account = var.service_account
    scaling {
      min_instance_count = var.min_instance_count
      max_instance_count = 3
    }
    vpc_access {
      network_interfaces {
        network    = var.network_id
        subnetwork = var.subnet_id
      }
      egress = "PRIVATE_RANGES_ONLY"
    }
    containers {
      image = var.image
      args  = var.args
      dynamic "env" {
        for_each = var.env
        content {
          name  = env.key
          value = env.value
        }
      }
    }
  }
}

# Schema migrations run inside the VPC as a one-shot job — the database is
# private-only, so applying them cannot be done from outside the network.
resource "google_cloud_run_v2_job" "migrate" {
  count               = var.migrate_job ? 1 : 0
  project             = var.project
  name                = "${var.name}-migrate"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = var.service_account
      max_retries     = 0
      vpc_access {
        network_interfaces {
          network    = var.network_id
          subnetwork = var.subnet_id
        }
        egress = "PRIVATE_RANGES_ONLY"
      }
      containers {
        image = var.image
        args  = ["migrate"]
        dynamic "env" {
          for_each = var.env
          content {
            name  = env.key
            value = env.value
          }
        }
      }
    }
  }
}

variable "migrate_job" {
  type    = bool
  default = true
}
# Optional E2E driver job — same VPC/env as control, image carries the
# driver script. Empty disables it (production deployments never set this).
variable "driver_image" {
  type    = string
  default = ""
}
# Extra env for the driver container only (API service names to resolve,
# scenario flags) — keeps test wiring out of the control env.
variable "driver_env" {
  type    = map(string)
  default = {}
}
# Separate identity for the E2E driver so scenario permissions (MIG resize,
# monitoring read, Pub/Sub publish) never land on the control plane SA.
# Empty falls back to the control service account.
variable "driver_service_account" {
  type    = string
  default = ""
}

resource "google_cloud_run_v2_job" "driver" {
  count               = var.driver_image != "" ? 1 : 0
  project             = var.project
  name                = "${var.name}-e2e-driver"
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1
    template {
      service_account = coalesce(var.driver_service_account, var.service_account)
      max_retries     = 0
      timeout         = "3600s"
      vpc_access {
        network_interfaces {
          network    = var.network_id
          subnetwork = var.subnet_id
        }
        egress = "PRIVATE_RANGES_ONLY"
      }
      containers {
        image = var.driver_image
        dynamic "env" {
          for_each = merge(var.env, var.driver_env)
          content {
            name  = env.key
            value = env.value
          }
        }
      }
    }
  }
}

# Client/admin JSON APIs — separate services so IAM can grant invoker per
# surface. `api` serves run submit/inspect/results/cancel; `admin` serves
# deployment lifecycle. Both stay internal-ingress only.
variable "api_services" {
  type    = list(string)
  default = []
  validation {
    condition     = alltrue([for r in var.api_services : contains(["api", "admin"], r)])
    error_message = "api_services may only contain \"api\" or \"admin\""
  }
}
# Full IAM member strings ("serviceAccount:...", "user:...") allowed to call
# each API surface. Keep admin_invokers tight — it controls deployments.
variable "api_invokers" {
  type    = list(string)
  default = []
}
variable "admin_invokers" {
  type    = list(string)
  default = []
}

locals {
  api_invokers = {
    api   = var.api_invokers
    admin = var.admin_invokers
  }
}

# API services are the client-facing surface — IAM (run.invoker) is the
# boundary, so ingress defaults to ALL. Internal callers inside the VPC still
# reach them; external clients get 403 without invoker, not a network error.
variable "api_ingress" {
  type    = string
  default = "INGRESS_TRAFFIC_ALL"
}

resource "google_cloud_run_v2_service" "api" {
  for_each            = toset(var.api_services)
  project             = var.project
  name                = "${var.name}-${each.value}"
  location            = var.region
  ingress             = var.api_ingress
  deletion_protection = false

  template {
    service_account = var.service_account
    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }
    vpc_access {
      network_interfaces {
        network    = var.network_id
        subnetwork = var.subnet_id
      }
      egress = "PRIVATE_RANGES_ONLY"
    }
    containers {
      image = var.image
      args  = [each.value]
      dynamic "env" {
        for_each = var.env
        content {
          name  = env.key
          value = env.value
        }
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "api_invoker" {
  for_each = toset(flatten([
    for role in var.api_services : [
      for member in local.api_invokers[role] : "${role}|${member}"
    ]
  ]))
  project  = var.project
  location = var.region
  name     = google_cloud_run_v2_service.api[split("|", each.value)[0]].name
  role     = "roles/run.invoker"
  member   = split("|", each.value)[1]
}

# One-shot reconcile pass driven by Cloud Scheduler instead of an in-process
# loop — survives control-service scale-to-zero and matches the plan's
# scheduled-reconciliation topology.
variable "reconcile_job" {
  type    = bool
  default = false
}
variable "reconcile_schedule" {
  type    = string
  default = "* * * * *"
}

resource "google_cloud_run_v2_job" "reconcile" {
  count               = var.reconcile_job ? 1 : 0
  project             = var.project
  name                = "${var.name}-reconcile"
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1
    template {
      service_account = var.service_account
      max_retries     = 0
      vpc_access {
        network_interfaces {
          network    = var.network_id
          subnetwork = var.subnet_id
        }
        egress = "PRIVATE_RANGES_ONLY"
      }
      containers {
        image = var.image
        args  = ["reconcile"]
        dynamic "env" {
          for_each = var.env
          content {
            name  = env.key
            value = env.value
          }
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job_iam_member" "reconcile_invoker" {
  count    = var.reconcile_job ? 1 : 0
  project  = var.project
  location = var.region
  name     = google_cloud_run_v2_job.reconcile[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.service_account}"
}

resource "google_cloud_scheduler_job" "reconcile" {
  count    = var.reconcile_job ? 1 : 0
  project  = var.project
  name     = "${var.name}-reconcile"
  region   = var.region
  schedule = var.reconcile_schedule

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project}/locations/${var.region}/jobs/${google_cloud_run_v2_job.reconcile[0].name}:run"
    oauth_token {
      service_account_email = var.service_account
    }
  }
}

output "service_url" { value = google_cloud_run_v2_service.control.uri }
output "api_urls" { value = { for role, s in google_cloud_run_v2_service.api : role => s.uri } }
output "migrate_job_name" { value = try(google_cloud_run_v2_job.migrate[0].name, null) }
output "driver_job_name" { value = try(google_cloud_run_v2_job.driver[0].name, null) }
output "reconcile_job_name" { value = try(google_cloud_run_v2_job.reconcile[0].name, null) }
