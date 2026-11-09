variable "project_id" {
  type = string
}

# Dedicated Service Account for Dataflow Workers (Least-Privilege Principle)
resource "google_service_account" "dataflow_worker" {
  project      = var.project_id
  account_id   = "dataflow-pipeline-worker"
  display_name = "Dataflow Pipeline Worker Service Account"
}

# 1. Dataflow Worker Role
resource "google_project_iam_member" "dataflow_worker_role" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# 2. BigQuery Data Editor (Write to analytical tables)
resource "google_project_iam_member" "bq_editor_role" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# 3. BigQuery Job User (Required to run query/load jobs)
resource "google_project_iam_member" "bq_job_user_role" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# 4. Pub/Sub Subscriber & Publisher (Main Sub & DLQ Topic)
resource "google_project_iam_member" "pubsub_subscriber_role" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_project_iam_member" "pubsub_publisher_role" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# 5. GCS Object Admin for Staging/Temp files
resource "google_project_iam_member" "storage_admin_role" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

output "dataflow_worker_sa_email" {
  value = google_service_account.dataflow_worker.email
}
