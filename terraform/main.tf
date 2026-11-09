terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.20.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. Enable Required GCP APIs
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudfunctions.googleapis.com",
    "compute.googleapis.com"
  ])
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# 2. IAM & Service Accounts Module
module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id
  depends_on = [google_project_service.enabled_apis]
}

# 3. Cloud Storage (Staging & Temporary Buckets)
module "storage" {
  source     = "./modules/storage"
  project_id = var.project_id
  region     = var.region
  depends_on = [google_project_service.enabled_apis]
}

# 4. Cloud Pub/Sub Module (Main Topic, Subscriptions, Dead-Letter Queue)
module "pubsub" {
  source     = "./modules/pubsub"
  project_id = var.project_id
  depends_on = [google_project_service.enabled_apis]
}

# 5. BigQuery Module (Datasets, Tables, Partitioning, Clustering)
module "bigquery" {
  source     = "./modules/bigquery"
  project_id = var.project_id
  region     = var.region
  depends_on = [google_project_service.enabled_apis]
}
