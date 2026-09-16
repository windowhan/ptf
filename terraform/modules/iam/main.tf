variable "project" {
  type = string
}
variable "name" {
  type    = string
  default = "runtime"
}

# Worker instances: claim units, write state, publish/ack Pub/Sub
resource "google_service_account" "worker" {
  project      = var.project
  account_id   = "${var.name}-worker"
  display_name = "Runtime worker instances"
}

# Control plane: submit/plan/dispatch/admin API
resource "google_service_account" "control" {
  project      = var.project
  account_id   = "${var.name}-control"
  display_name = "Runtime control plane"
}

resource "google_project_iam_member" "worker_sql" {
  project = var.project
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_project_iam_member" "worker_pubsub" {
  project = var.project
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_project_iam_member" "control_sql" {
  project = var.project
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.control.email}"
}

resource "google_project_iam_member" "control_pubsub" {
  project = var.project
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:${google_service_account.control.email}"
}

# DB password lives in Secret Manager; only the runtime identities read it
resource "google_secret_manager_secret" "db_password" {
  project   = var.project
  secret_id = "${var.name}-db-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "worker_db" {
  project   = var.project
  secret_id = google_secret_manager_secret.db_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_secret_manager_secret_iam_member" "control_db" {
  project   = var.project
  secret_id = google_secret_manager_secret.db_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.control.email}"
}

output "worker_email" { value = google_service_account.worker.email }
output "control_email" { value = google_service_account.control.email }
output "db_password_secret" { value = google_secret_manager_secret.db_password.id }
