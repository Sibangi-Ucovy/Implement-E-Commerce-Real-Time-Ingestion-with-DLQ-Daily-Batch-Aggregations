variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

# 1. Dataset
resource "google_bigquery_dataset" "ecommerce" {
  dataset_id                  = "ecommerce_analytics"
  friendly_name               = "E-Commerce Real-Time & Batch Analytics"
  description                 = "Warehouse dataset for streaming ingestion and analytical modeling"
  location                    = var.region
  default_table_expiration_ms = null
  project                     = var.project_id
}

# 2. Orders Real-time Table (Partitioned by Date, Clustered by Customer ID & Status)
resource "google_bigquery_table" "orders_realtime" {
  dataset_id          = google_bigquery_dataset.ecommerce.dataset_id
  table_id            = "orders_realtime"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "order_timestamp"
  }

  clustering = ["customer_id", "status"]

  schema = jsonencode([
    {
      name        = "order_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique UUID of the order"
    },
    {
      name        = "customer_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Customer ID"
    },
    {
      name        = "amount"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Total transaction amount in USD"
    },
    {
      name        = "items"
      type        = "RECORD"
      mode        = "REPEATED"
      description = "Array of purchased line items"
      fields = [
        { name = "item_id", type = "STRING", mode = "REQUIRED" },
        { name = "qty", type = "INT64", mode = "REQUIRED" },
        { name = "price", type = "NUMERIC", mode = "REQUIRED" }
      ]
    },
    {
      name        = "order_timestamp"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Event occurrence timestamp"
    },
    {
      name        = "status"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Order status: PENDING, COMPLETED, CANCELLED"
    }
  ])
}

# 3. Orders DLQ Table (Stores Poison Pills for Triage)
resource "google_bigquery_table" "orders_dlq" {
  dataset_id          = google_bigquery_dataset.ecommerce.dataset_id
  table_id            = "orders_dlq"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "insertion_timestamp"
  }

  schema = jsonencode([
    {
      name        = "raw_payload"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Original string/byte payload received"
    },
    {
      name        = "error_message"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Reason for failure (e.g., JSONDecodeError, ValidationError)"
    },
    {
      name        = "insertion_timestamp"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Time routed to DLQ"
    }
  ])
}

output "dataset_id" {
  value = google_bigquery_dataset.ecommerce.dataset_id
}

output "orders_realtime_table_id" {
  value = google_bigquery_table.orders_realtime.table_id
}
