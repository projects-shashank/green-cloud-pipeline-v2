output "dataset_id" {
  value = google_bigquery_dataset.main.dataset_id
}

output "jobs_table_id" {
  value = google_bigquery_table.jobs.table_id
}
