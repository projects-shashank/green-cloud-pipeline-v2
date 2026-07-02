# Dead letter topics first — main topics reference them
resource "google_pubsub_topic" "dlq" {
  name    = "${var.topic_prefix}-dlq"
  project = var.project_id
}

resource "google_pubsub_subscription" "dlq_sub" {
  name    = "${var.topic_prefix}-dlq-sub"
  topic   = google_pubsub_topic.dlq.name
  project = var.project_id

  # Keep failed messages for 7 days for inspection
  message_retention_duration = "604800s"
}

# Main job topic
resource "google_pubsub_topic" "jobs" {
  name    = "${var.topic_prefix}-jobs"
  project = var.project_id

  # Dead letter policy wired in at topic level
}

# Main subscription — this is what the admission controller pulls from
resource "google_pubsub_subscription" "jobs_sub" {
  name    = "${var.topic_prefix}-jobs-sub"
  topic   = google_pubsub_topic.jobs.name
  project = var.project_id

  # At-least-once delivery — messages redelivered if not acknowledged
  ack_deadline_seconds = 60

  # Dead letter policy — after 5 failed attempts, move to DLQ
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }

  # Keep undelivered messages for 24 hours
  message_retention_duration = "86400s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }
}
