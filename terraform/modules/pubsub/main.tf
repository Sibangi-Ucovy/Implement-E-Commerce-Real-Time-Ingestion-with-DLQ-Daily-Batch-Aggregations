variable "project_id" {
  type = string
}

# 1. Dead-Letter Queue Topic
resource "google_pubsub_topic" "orders_dlq" {
  name    = "orders-stream-dlq"
  project = var.project_id

  message_retention_duration = "604800s" # 7 days
}

# 2. Main Ingestion Topic
resource "google_pubsub_topic" "orders_main" {
  name    = "orders-stream-topic"
  project = var.project_id

  message_retention_duration = "604800s" # 7 days
}

# 3. Main Ingestion Subscription with Dead-Letter Policy
resource "google_pubsub_subscription" "orders_sub" {
  name    = "orders-stream-sub"
  project = var.project_id
  topic   = google_pubsub_topic.orders_main.name

  ack_deadline_seconds       = 60
  retain_acked_messages      = false
  message_retention_duration = "604800s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.orders_dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# Grant Pub/Sub Service Identity permission to forward to DLQ
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.orders_dlq.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

data "google_project" "current" {
  project_id = var.project_id
}

output "main_topic_id" {
  value = google_pubsub_topic.orders_main.id
}

output "main_subscription_id" {
  value = google_pubsub_subscription.orders_sub.id
}

output "dlq_topic_id" {
  value = google_pubsub_topic.orders_dlq.id
}
