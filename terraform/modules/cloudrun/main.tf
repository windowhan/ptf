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
  project  = var.project
  name     = var.name
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

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
  count    = var.migrate_job ? 1 : 0
  project  = var.project
  name     = "${var.name}-migrate"
  location = var.region

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

resource "google_cloud_run_v2_job" "driver" {
  count    = var.driver_image != "" ? 1 : 0
  project  = var.project
  name     = "${var.name}-e2e-driver"
  location = var.region

  template {
    task_count = 1
    template {
      service_account = var.service_account
      max_retries     = 0
      timeout         = "1200s"
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

output "service_url" { value = google_cloud_run_v2_service.control.uri }
output "migrate_job_name" { value = try(google_cloud_run_v2_job.migrate[0].name, null) }
output "driver_job_name" { value = try(google_cloud_run_v2_job.driver[0].name, null) }
