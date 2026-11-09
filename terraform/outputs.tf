output "pubsub_main_topic" {
  description = "The Pub/Sub topic ID for streaming orders"
  value       = module.pubsub.main_topic_id
}

output "pubsub_dlq_topic" {
  description = "The Dead-Letter Queue Pub/Sub topic ID"
  value       = module.pubsub.dlq_topic_id
}

output "bigquery_dataset_id" {
  description = "The BigQuery dataset ID"
  value       = module.bigquery.dataset_id
}

output "gcs_staging_bucket" {
  description = "The GCS bucket used for Dataflow staging & temp storage"
  value       = module.storage.staging_bucket_name
}

output "dataflow_service_account" {
  description = "Service Account email used for Dataflow worker execution"
  value       = module.iam.dataflow_worker_sa_email
}
