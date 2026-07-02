terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = "us-central1"
}

module "bigquery" {
  source     = "../../modules/bigquery"
  project_id = var.project_id
  dataset_id = "green_cloud_pipeline"
  location   = "US"
}

module "gcs" {
  source      = "../../modules/gcs"
  project_id  = var.project_id
  bucket_name = "${var.project_id}-data-lake"
  location    = "US"
}
