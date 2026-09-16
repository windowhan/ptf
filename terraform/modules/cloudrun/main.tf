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

resource "google_cloud_run_v2_service" "control" {
  project  = var.project
  name     = var.name
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = var.service_account
    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }
    containers {
      image = var.image
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

output "service_url" { value = google_cloud_run_v2_service.control.uri }
