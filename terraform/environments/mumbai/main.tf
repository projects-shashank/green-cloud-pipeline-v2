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
  region             = var.zone
  cluster_name       = var.cluster_name
  machine_type       = "e2-medium"
  enable_autoscaling = false
  node_count         = 1
}

module "artifact_registry" {
  source        = "../../modules/artifact-registry"
  project_id    = var.project_id
  region        = var.region
  repository_id = "green-cloud-pipeline"
}

module "pubsub" {
  source               = "../../modules/pubsub"
  project_id           = var.project_id
  topic_prefix         = "mumbai"
  create_process_topic = true
}
