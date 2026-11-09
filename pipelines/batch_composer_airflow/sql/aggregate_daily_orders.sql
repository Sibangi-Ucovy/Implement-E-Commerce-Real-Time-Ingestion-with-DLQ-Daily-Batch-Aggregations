-- SQL Transformation script for daily batch aggregation
-- Uses window functions and deduplication logic

MERGE INTO `ecommerce_analytics.daily_orders_summary` T
USING (
  WITH ranked_orders AS (
    SELECT
      DATE(order_timestamp) AS order_date,
      customer_id,
      status,
      amount,
      ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_timestamp DESC) AS rn
    FROM
      `ecommerce_analytics.orders_realtime`
    WHERE
      DATE(order_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
  )
  SELECT
    order_date,
    customer_id,
    status,
    COUNT(1) AS total_orders,
    SUM(amount) AS gross_revenue_usd,
    AVG(amount) AS average_order_value,
    CURRENT_TIMESTAMP() AS updated_at
  FROM
    ranked_orders
  WHERE
    rn = 1
  GROUP BY
    order_date,
    customer_id,
    status
) S
ON T.order_date = S.order_date
   AND T.customer_id = S.customer_id
   AND T.status = S.status
WHEN MATCHED THEN
  UPDATE SET
    total_orders = S.total_orders,
    gross_revenue_usd = S.gross_revenue_usd,
    average_order_value = S.average_order_value,
    updated_at = S.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_date, customer_id, status, total_orders, gross_revenue_usd, average_order_value, updated_at)
  VALUES (S.order_date, S.customer_id, S.status, S.total_orders, S.gross_revenue_usd, S.average_order_value, S.updated_at);
