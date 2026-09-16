variable "project" {
  type = string
}
variable "name" {
  type    = string
  default = "runtime"
}
variable "notification_channels" {
  type    = list(string)
  default = []
}

# Alert: dead-lettered units accumulate
resource "google_monitoring_alert_policy" "dead_letters" {
  project      = var.project
  display_name = "${var.name}: dead-lettered units present"
  combiner     = "OR"

  conditions {
    display_name = "dead-letter queue depth"
    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"pubsub_topic\"",
        "resource.labels.topic_id = \"${var.name}-dead-letter\"",
        "metric.type = \"pubsub.googleapis.com/topic/send_message_operation_count\"",
      ])
      duration        = "60s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  notification_channels = var.notification_channels
}

# Alert: no worker heartbeats
resource "google_monitoring_alert_policy" "no_workers" {
  project      = var.project
  display_name = "${var.name}: no active workers"
  combiner     = "OR"

  conditions {
    display_name = "worker heartbeat absence"
    condition_absent {
      duration = "300s"
      filter = join(" AND ", [
        "resource.type = \"gce_instance\"",
        "metric.type = \"compute.googleapis.com/instance/cpu/utilization\"",
      ])
    }
  }

  notification_channels = var.notification_channels
}
