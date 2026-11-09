"""
Cloud Composer / Apache Airflow DAG: Daily E-Commerce Analytics Aggregation.
Orchestrates:
1. GCS File Sensor (verifies upstream files landed)
2. BigQuery Batch Deduplication & Partition Aggregation
3. Data Quality Audit check (BigQueryCheckOperator)
4. Alerting on failure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ID = "{{ var.value.get('gcp_project_id', 'my-gcp-project') }}"
DATASET_NAME = "ecommerce_analytics"

with DAG(
    dag_id="ecommerce_daily_batch_aggregation",
    default_args=DEFAULT_ARGS,
    description="Daily ETL pipeline aggregating real-time streaming data for BI & Looker",
    schedule_interval="0 2 * * *",  # Daily at 02:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "bigquery", "production"],
) as dag:

    # Step 1: Optional check for batch manifest in Cloud Storage
    check_landing_manifest = GCSObjectExistenceSensor(
        task_id="check_landing_manifest",
        bucket=f"{PROJECT_ID}-data-lake-landing",
        object="batch_uploads/{{ ds }}/ready.marker",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # Step 2: Execute BigQuery Aggregation & Deduplication
    aggregate_daily_metrics = BigQueryInsertJobOperator(
        task_id="aggregate_daily_metrics",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.daily_orders_summary`
                PARTITION BY order_date
                CLUSTER BY top_category AS
                SELECT
                    DATE(order_timestamp) AS order_date,
                    customer_id,
                    status,
                    COUNT(DISTINCT order_id) AS total_orders,
                    SUM(amount) AS gross_revenue_usd,
                    AVG(amount) AS average_order_value,
                    CURRENT_TIMESTAMP() AS updated_at
                FROM
                    `{PROJECT_ID}.{DATASET_NAME}.orders_realtime`
                WHERE
                    DATE(order_timestamp) = DATE_SUB(DATE('{{{{ ds }}}}'), INTERVAL 1 DAY)
                GROUP BY
                    1, 2, 3;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Step 3: Data Quality Gate (Verify no negative revenue or empty records)
    data_quality_audit = BigQueryCheckOperator(
        task_id="data_quality_audit",
        sql=f"""
        SELECT COUNT(1)
        FROM `{PROJECT_ID}.{DATASET_NAME}.daily_orders_summary`
        WHERE gross_revenue_usd < 0 OR total_orders <= 0;
        """,
        use_legacy_sql=False,
        # BigQueryCheckOperator expects the query to return 0 or pass condition
    )

    check_landing_manifest >> aggregate_daily_metrics >> data_quality_audit
