variable "project" {
  type = string
}

variable "services" {
  type = list(string)
  default = [
    "cloudresourcemanager.googleapis.com",
    "servicenetworking.googleapis.com",
    "sqladmin.googleapis.com",
    "pubsub.googleapis.com",
    "compute.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
  ]
}

resource "google_project_service" "enabled" {
  for_each           = toset(var.services)
  project            = var.project
  service            = each.value
  disable_on_destroy = false
}

output "services" { value = [for s in google_project_service.enabled : s.service] }
