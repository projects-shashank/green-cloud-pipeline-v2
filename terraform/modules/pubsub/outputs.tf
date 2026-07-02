output "topic_name" {
  value = google_pubsub_topic.jobs.name
}

output "subscription_name" {
  value = google_pubsub_subscription.jobs_sub.name
}

output "dlq_topic_name" {
  value = google_pubsub_topic.dlq.name
}

output "process_topic_name" {
  value = var.create_process_topic ? google_pubsub_topic.process[0].name : null
}

output "process_subscription_name" {
  value = var.create_process_topic ? google_pubsub_subscription.process_sub[0].name : null
}
