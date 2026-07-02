# Dead letter topic
resource "google_pubsub_topic" "dlq" {
  name    = "${var.topic_prefix}-dlq"
  project = var.project_id
}

resource "google_pubsub_subscription" "dlq_sub" {
  name    = "${var.topic_prefix}-dlq-sub"
  topic   = google_pubsub_topic.dlq.name
  project = var.project_id
  message_retention_duration = "604800s"
}

# Inbound job topic (from generator)
resource "google_pubsub_topic" "jobs" {
  name    = "${var.topic_prefix}-jobs"
  project = var.project_id
}

resource "google_pubsub_subscription" "jobs_sub" {
  name    = "${var.topic_prefix}-jobs-sub"
  topic   = google_pubsub_topic.jobs.name
  project = var.project_id
  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }
}

# Internal processing topic (admission controller → worker)
# Only created for Mumbai (Montreal worker consumes directly from jobs topic)
resource "google_pubsub_topic" "process" {
  count   = var.create_process_topic ? 1 : 0
  name    = "${var.topic_prefix}-process"
  project = var.project_id
}

resource "google_pubsub_subscription" "process_sub" {
  count   = var.create_process_topic ? 1 : 0
  name    = "${var.topic_prefix}-process-sub"
  topic   = google_pubsub_topic.process[0].name
  project = var.project_id
  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }
}
