# Compose all modules for one environment. `terraform validate` only —
# real apply requires credentials and is deferred to the E2E milestone.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0, < 7"
    }
  }
}

variable "project" {
  type = string
}
variable "region" {
  type    = string
  default = "us-central1"
}
variable "worker_image" {
  type    = string
  default = "us-central1-docker.pkg.dev/placeholder/runtime-worker:latest"
}
variable "control_image" {
  type    = string
  default = "us-central1-docker.pkg.dev/placeholder/runtime-control:latest"
}

provider "google" {
  project = var.project
  region  = var.region
}

module "apis" {
  source  = "../../modules/apis"
  project = var.project
}

module "network" {
  source  = "../../modules/network"
  project = var.project
  region  = var.region
}

module "iam" {
  source  = "../../modules/iam"
  project = var.project
}

module "cloudsql" {
  source     = "../../modules/cloudsql"
  project    = var.project
  region     = var.region
  network_id = module.network.network_id
  tier       = "db-f1-micro"
  ha         = false
  depends_on = [module.apis]
}

module "pubsub" {
  source         = "../../modules/pubsub"
  project        = var.project
  pool_revisions = ["rev-a"]
  depends_on     = [module.apis]
}

module "mig" {
  source          = "../../modules/mig"
  project         = var.project
  region          = var.region
  subnet_id       = module.network.subnet_id
  service_account = module.iam.worker_email
  image           = var.worker_image
  min_replicas    = 2
  max_replicas    = 5
  env = {
    RUNTIME_DSN_HOST = module.cloudsql.private_ip
    RUNTIME_DB       = module.cloudsql.database
  }
  depends_on = [module.apis]
}

module "cloudrun" {
  source          = "../../modules/cloudrun"
  project         = var.project
  region          = var.region
  image           = var.control_image
  service_account = module.iam.control_email
  env = {
    RUNTIME_DSN_HOST = module.cloudsql.private_ip
    RUNTIME_DB       = module.cloudsql.database
  }
  depends_on = [module.apis]
}

module "observability" {
  source     = "../../modules/observability"
  project    = var.project
  depends_on = [module.apis]
}
