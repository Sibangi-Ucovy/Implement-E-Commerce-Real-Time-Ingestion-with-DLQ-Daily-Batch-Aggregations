variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

# Unique suffix using project id
resource "google_storage_bucket" "dataflow_staging" {
  name                        = "${var.project_id}-dataflow-staging"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "data_lake_landing" {
  name                        = "${var.project_id}-data-lake-landing"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
}

output "staging_bucket_name" {
  value = google_storage_bucket.dataflow_staging.name
}

output "landing_bucket_name" {
  value = google_storage_bucket.data_lake_landing.name
}
