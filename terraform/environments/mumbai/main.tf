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
  region  = var.region
}

module "gke" {
  source             = "../../modules/gke-cluster"
  project_id         = var.project_id
  region             = var.region
  cluster_name       = var.cluster_name
  machine_type       = "e2-medium"
  enable_autoscaling = false
  node_count         = 1
}
