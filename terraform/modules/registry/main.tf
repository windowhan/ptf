variable "project" {
  type = string
}
variable "region" {
  type = string
}
variable "name" {
  type    = string
  default = "runtime"
}
# Service-account emails allowed to pull images (worker + control SAs)
variable "readers" {
  type    = list(string)
  default = []
}

resource "google_artifact_registry_repository" "images" {
  project       = var.project
  location      = var.region
  repository_id = var.name
  format        = "DOCKER"
}

# Project-level reader grant: repo-level setIamPolicy needs repoAdmin, which
# the deployer identity may not hold; project IAM bindings only need
# resourcemanager.projectIamAdmin and cover every repo in the project.
resource "google_project_iam_member" "readers" {
  for_each = toset(var.readers)
  project  = var.project
  role     = "roles/artifactregistry.reader"
  member   = "serviceAccount:${each.value}"
}

output "repository" {
  value = "${var.region}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.images.repository_id}"
}
