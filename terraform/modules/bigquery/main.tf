resource "google_bigquery_dataset" "main" {
  dataset_id  = var.dataset_id
  project     = var.project_id
  location    = var.location
  description = "Green Cloud Pipeline — job telemetry and carbon accounting"

  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "jobs" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = "jobs"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "job_id",               type = "STRING",    mode = "REQUIRED" },
    { name = "job_type",             type = "STRING",    mode = "REQUIRED" },
    { name = "task",                 type = "STRING",    mode = "NULLABLE" },
    { name = "model_name",           type = "STRING",    mode = "REQUIRED" },
    { name = "priority_tier",        type = "STRING",    mode = "REQUIRED" },
    { name = "origin_region",        type = "STRING",    mode = "REQUIRED" },
    { name = "processed_region",     type = "STRING",    mode = "REQUIRED" },
    { name = "was_redirected",       type = "BOOLEAN",   mode = "REQUIRED" },
    { name = "admission_mode",       type = "STRING",    mode = "REQUIRED" },
    { name = "arrival_timestamp",    type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "processed_timestamp",  type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "latency_ms",           type = "FLOAT",     mode = "REQUIRED" },
    { name = "input_token_count",    type = "INTEGER",   mode = "NULLABLE" },
    { name = "output_token_count",   type = "INTEGER",   mode = "NULLABLE" },
    { name = "num_images",           type = "INTEGER",   mode = "NULLABLE" },
    { name = "energy_kwh",           type = "FLOAT",     mode = "REQUIRED" },
    { name = "carbon_intensity",     type = "FLOAT",     mode = "REQUIRED" },
    { name = "carbon_emitted_g",     type = "FLOAT",     mode = "REQUIRED" },
    { name = "carbon_saved_g",       type = "FLOAT",     mode = "REQUIRED" },
    { name = "sla_violated",         type = "BOOLEAN",   mode = "REQUIRED" },
  ])

  time_partitioning {
    type  = "DAY"
    field = "arrival_timestamp"
  }
}
