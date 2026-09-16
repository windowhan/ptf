variable "project" {
  type = string
}
variable "region" {
  type = string
}
variable "name" {
  type    = string
  default = "runtime-state"
}
variable "tier" {
  type    = string
  default = "db-f1-micro"
}
variable "network_id" {
  type = string
}
variable "ha" {
  type    = bool
  default = false
}
variable "pitr_enabled" {
  type    = bool
  default = true
}
variable "deletion_protection" {
  type    = bool
  default = true
}
variable "db_user" {
  type    = string
  default = "runtime"
}
# google_secret_manager_secret.id that receives the generated password
variable "password_secret_id" {
  type = string
}

resource "google_sql_database_instance" "state" {
  project          = var.project
  name             = var.name
  region           = var.region
  database_version = "POSTGRES_16"

  settings {
    tier              = var.tier
    edition           = "ENTERPRISE"
    availability_type = var.ha ? "REGIONAL" : "ZONAL"

    ip_configuration {
      ipv4_enabled    = false
      private_network = var.network_id
    }

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = var.pitr_enabled
      start_time                     = "03:00"
    }
  }

  deletion_protection = var.deletion_protection
}

resource "google_sql_database" "runtime" {
  project  = var.project
  instance = google_sql_database_instance.state.name
  name     = "runtime"
}

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "google_sql_user" "runtime" {
  project  = var.project
  instance = google_sql_database_instance.state.name
  name     = var.db_user
  password = random_password.db.result
}

resource "google_secret_manager_secret_version" "db_password" {
  secret      = var.password_secret_id
  secret_data = random_password.db.result
}

output "instance_connection_name" {
  value = google_sql_database_instance.state.connection_name
}
output "database" { value = google_sql_database.runtime.name }
output "private_ip" { value = google_sql_database_instance.state.private_ip_address }
output "db_user" { value = google_sql_user.runtime.name }
