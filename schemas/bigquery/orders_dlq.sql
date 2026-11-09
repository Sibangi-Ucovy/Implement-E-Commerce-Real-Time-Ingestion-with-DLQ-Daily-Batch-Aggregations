-- DDL: Dead-Letter Queue (DLQ) Table for Poison Pills
-- Stores unparseable JSON, type mismatches, and schema violations for automated triage

CREATE OR REPLACE TABLE `ecommerce_analytics.orders_dlq` (
  raw_payload STRING OPTIONS(description="Original unparsed string or byte payload"),
  error_message STRING OPTIONS(description="Exception stacktrace or reason for validation rejection"),
  insertion_timestamp TIMESTAMP OPTIONS(description="Time when the event was routed to the DLQ")
)
PARTITION BY DATE(insertion_timestamp)
OPTIONS (
  description = "Dead-letter repository for failed streaming events"
);
