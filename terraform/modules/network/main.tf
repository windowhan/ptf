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

resource "google_compute_network" "this" {
  project                 = var.project
  name                    = "${var.name}-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "workers" {
  project                  = var.project
  name                     = "${var.name}-workers"
  region                   = var.region
  network                  = google_compute_network.this.id
  ip_cidr_range            = "10.10.0.0/24"
  private_ip_google_access = true
}

# Private services access for Cloud SQL
resource "google_compute_global_address" "private_services" {
  project       = var.project
  name          = "${var.name}-psa"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.this.id
}

resource "google_service_networking_connection" "private" {
  network                 = google_compute_network.this.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_services.name]
}

output "network_id" { value = google_compute_network.this.id }
output "subnet_id" { value = google_compute_subnetwork.workers.id }
output "network_name" { value = google_compute_network.this.name }
