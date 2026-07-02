output "bigquery_dataset" {
  value = module.bigquery.dataset_id
}

output "gcs_bucket" {
  value = module.gcs.bucket_name
}
