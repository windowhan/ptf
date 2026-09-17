# Compose all modules for one environment. `terraform validate` only —
# real apply requires credentials and is deferred to the E2E milestone.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0, < 7"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5, < 4"
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
# E2E-only: driver job image carrying scripts/e2e/gcp_driver.py
variable "driver_image" {
  type    = string
  default = ""
}
# Product app factories the deploy image bundles (module:callable specs)
variable "finite_app" {
  type    = string
  default = "example_product.app:build_application"
}
variable "continuous_app" {
  type    = string
  default = "example_stream.app:build_application"
}
variable "pool_revision" {
  type    = string
  default = "rev-a"
}
# Additional pool revisions to keep subscriptions alive for — runs pinned to
# an older execution revision keep dispatching while workers roll over.
variable "extra_pool_revisions" {
  type    = list(string)
  default = []
}
# Deployer identity allowed to act as the control service account — Cloud
# Scheduler's oauth_token requires iam.serviceAccounts.actAs at creation.
variable "deployer_sa" {
  type    = string
  default = ""
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
  source     = "../../modules/network"
  project    = var.project
  region     = var.region
  depends_on = [module.apis]
}

module "iam" {
  source         = "../../modules/iam"
  project        = var.project
  control_act_as = var.deployer_sa == "" ? [] : ["serviceAccount:${var.deployer_sa}"]
  depends_on     = [module.apis]
}

module "registry" {
  source     = "../../modules/registry"
  project    = var.project
  region     = var.region
  readers    = [module.iam.worker_email, module.iam.control_email]
  depends_on = [module.apis]
}

module "cloudsql" {
  source              = "../../modules/cloudsql"
  project             = var.project
  region              = var.region
  network_id          = module.network.network_id
  tier                = "db-f1-micro"
  ha                  = false
  deletion_protection = false
  password_secret_id  = module.iam.db_password_secret
  depends_on          = [module.apis]
}

module "pubsub" {
  source         = "../../modules/pubsub"
  project        = var.project
  pool_revisions = concat([var.pool_revision], var.extra_pool_revisions)
  depends_on     = [module.apis]
}

# Shared runtime env for both roles — secret values stay in Secret Manager
locals {
  runtime_env = {
    RUNTIME_PROJECT            = var.project
    RUNTIME_POOL_REVISION      = var.pool_revision
    RUNTIME_DB_HOST            = module.cloudsql.private_ip
    RUNTIME_DB_NAME            = module.cloudsql.database
    RUNTIME_DB_USER            = module.cloudsql.db_user
    RUNTIME_DB_PASSWORD_SECRET = "${module.iam.db_password_secret}/versions/latest"
    RUNTIME_DISPATCH_TOPIC     = module.pubsub.dispatch_topic
    RUNTIME_EVENTS_TOPIC       = module.pubsub.events_topic
    RUNTIME_FINITE_APP         = var.finite_app
    RUNTIME_CONTINUOUS_APP     = var.continuous_app
  }
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
  env = merge(local.runtime_env, {
    RUNTIME_PUBSUB_SUBSCRIPTION = module.pubsub.worker_subscriptions[var.pool_revision]
  })
  depends_on = [module.apis]
}

module "cloudrun" {
  source          = "../../modules/cloudrun"
  project         = var.project
  region          = var.region
  image           = var.control_image
  service_account = module.iam.control_email
  network_id      = module.network.network_id
  subnet_id       = module.network.subnet_id
  env = merge(local.runtime_env, {
    # reconcile is driven by the Scheduler-triggered job, not the control loop
    RUNTIME_CONTROL_RECONCILER = "off"
  })
  min_instance_count = 1 # control loops must always run in this env
  driver_image       = var.driver_image
  driver_env = {
    RUNTIME_API_NAME   = "runtime-control-api"
    RUNTIME_ADMIN_NAME = "runtime-control-admin"
  }
  api_services   = ["api", "admin"]
  api_invokers   = ["serviceAccount:${module.iam.control_email}"]
  admin_invokers = ["serviceAccount:${module.iam.control_email}"]
  reconcile_job  = true
  depends_on     = [module.apis]
}

output "api_urls" { value = module.cloudrun.api_urls }

module "observability" {
  source     = "../../modules/observability"
  project    = var.project
  depends_on = [module.apis]
}

output "repository" { value = module.registry.repository }
output "instance_connection_name" { value = module.cloudsql.instance_connection_name }
output "db_password_secret" { value = module.iam.db_password_secret }
output "dispatch_topic" { value = module.pubsub.dispatch_topic }
output "worker_subscription" { value = module.pubsub.worker_subscriptions[var.pool_revision] }
output "events_subscription" { value = module.pubsub.events_subscription }
