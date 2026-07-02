resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  project       = var.project_id
  location      = var.location
  force_destroy = true

  # Lifecycle: move objects older than 30 days to cheaper storage
  lifecycle_rule {
    condition { age = 30 }
    action    { type = "SetStorageClass", storage_class = "NEARLINE" }
  }

  # Lifecycle: delete objects older than 90 days
  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }

  uniform_bucket_level_access = true
}
