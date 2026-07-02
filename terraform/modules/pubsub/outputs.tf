output "topic_name" {
  value = google_pubsub_topic.jobs.name
}

output "subscription_name" {
  value = google_pubsub_subscription.jobs_sub.name
}

output "dlq_topic_name" {
  value = google_pubsub_topic.dlq.name
}
