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
    condition_monitoring_query_language {
      query    = <<-EOT
        fetch pubsub_topic
        | metric 'pubsub.googleapis.com/topic/send_message_operation_count'
        | filter resource.topic_id == '${var.name}-dead-letter'
        | group_by 5m, [value_count_aggregate: aggregate(value.count)]
        | condition val() > 0
      EOT
      duration = "60s"
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
      filter   = "resource.type = \"gce_instance\""
    }
  }

  notification_channels = var.notification_channels
}
