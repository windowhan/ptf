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

resource "google_sql_database_instance" "state" {
  project          = var.project
  name             = var.name
  region           = var.region
  database_version = "POSTGRES_16"

  settings {
    tier              = var.tier
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

  deletion_protection = true
}

resource "google_sql_database" "runtime" {
  project  = var.project
  instance = google_sql_database_instance.state.name
  name     = "runtime"
}

output "instance_connection_name" {
  value = google_sql_database_instance.state.connection_name
}
output "database" { value = google_sql_database.runtime.name }
output "private_ip" { value = google_sql_database_instance.state.private_ip_address }
