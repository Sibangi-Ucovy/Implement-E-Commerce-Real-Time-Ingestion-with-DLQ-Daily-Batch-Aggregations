-- DDL: Destination table for validated streaming orders
-- Partitioned by date for cost optimization; Clustered by customer_id and status for sub-second query pruning.

CREATE OR REPLACE TABLE `ecommerce_analytics.orders_realtime` (
  order_id STRING OPTIONS(description="Unique UUID identifier for the purchase order"),
  customer_id STRING OPTIONS(description="Customer identifier"),
  amount NUMERIC OPTIONS(description="Total transaction amount in USD"),
  items ARRAY<STRUCT<
    item_id STRING,
    qty INT64,
    price NUMERIC
  >> OPTIONS(description="Array of purchased line items"),
  order_timestamp TIMESTAMP OPTIONS(description="UTC timestamp when the order occurred"),
  status STRING OPTIONS(description="Lifecycle status: PENDING, COMPLETED, CANCELLED")
)
PARTITION BY DATE(order_timestamp)
CLUSTER BY customer_id, status
OPTIONS (
  description = "Real-time streaming ingestion table populated by Cloud Dataflow",
  require_partition_filter = false
);
