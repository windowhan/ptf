variable "project" {
  type = string
}
variable "name" {
  type    = string
  default = "runtime"
}
# Revision filter: workers subscribe only to messages for their pool revision
variable "pool_revisions" {
  type    = list(string)
  default = ["local"]
}

resource "google_pubsub_topic" "unit_dispatch" {
  project = var.project
  name    = "${var.name}-unit-dispatch"
}

resource "google_pubsub_topic" "dead_letter" {
  project = var.project
  name    = "${var.name}-dead-letter"
}

# Continuous emissions/checkpoints land here for downstream consumers
resource "google_pubsub_topic" "events" {
  project = var.project
  name    = "${var.name}-events"
}

# Pull-everything subscription for verification and debugging
resource "google_pubsub_subscription" "events_all" {
  project = var.project
  name    = "${var.name}-events-all"
  topic   = google_pubsub_topic.events.id

  expiration_policy { ttl = "" }
}

resource "google_pubsub_subscription" "workers" {
  for_each = toset(var.pool_revisions)
  project  = var.project
  name     = "${var.name}-units-${each.value}"
  topic    = google_pubsub_topic.unit_dispatch.id

  # each pool revision pulls only its own generation's messages
  filter = "attributes.runtime_pool_revision=\"${each.value}\""

  ack_deadline_seconds       = 30
  message_retention_duration = "600s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 10
  }

  expiration_policy { ttl = "" }
}

# Pull-everything subscription on the dead-letter topic — verification and
# operators inspect poisoned deliveries here instead of losing them.
resource "google_pubsub_subscription" "dead_letter_all" {
  project = var.project
  name    = "${var.name}-dead-letter-all"
  topic   = google_pubsub_topic.dead_letter.id

  expiration_policy { ttl = "" }
}

# Dead-letter forwarding is performed by the Pub/Sub service account: it
# needs publish on the DLQ topic and subscriber on each source subscription,
# otherwise poisoned messages are dropped silently after max attempts.
data "google_project" "this" {
  project_id = var.project
}

resource "google_pubsub_topic_iam_member" "dead_letter_publish" {
  project = var.project
  topic   = google_pubsub_topic.dead_letter.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "dead_letter_source" {
  for_each     = google_pubsub_subscription.workers
  project      = var.project
  subscription = each.value.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

output "dispatch_topic" { value = google_pubsub_topic.unit_dispatch.name }
output "dead_letter_topic" { value = google_pubsub_topic.dead_letter.name }
output "dead_letter_subscription" { value = google_pubsub_subscription.dead_letter_all.name }
output "events_topic" { value = google_pubsub_topic.events.name }
output "events_subscription" { value = google_pubsub_subscription.events_all.name }
output "worker_subscriptions" { value = { for k, s in google_pubsub_subscription.workers : k => s.name } }
