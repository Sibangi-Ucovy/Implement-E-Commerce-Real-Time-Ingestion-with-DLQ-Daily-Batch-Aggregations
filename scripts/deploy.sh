#!/usr/bin/env bash
# End-to-End GCP Pipeline Deployment Automation Script
set -e

# Configuration
PROJECT_ID=${1:-"your-gcp-project-id"}
REGION=${2:-"us-central1"}

echo "============================================================"
echo "🚀 Deploying GCP End-to-End Stack for Project: $PROJECT_ID"
echo "============================================================"

# Step 1: Set gcloud context
gcloud config set project "$PROJECT_ID"

# Step 2: Initialize & Apply Terraform
echo "📦 Provisioning Cloud Infrastructure via Terraform..."
cd "$(dirname "$0")/../terraform"

cat <<EOF > terraform.tfvars
project_id = "$PROJECT_ID"
region     = "$REGION"
environment = "prod"
EOF

terraform init
terraform apply -auto-approve

# Step 3: Run Unit Tests for Dataflow Pipeline
echo "🧪 Running Apache Beam unit tests..."
cd ../pipelines/streaming_beam_dataflow
python -m unittest tests/test_pipeline.py

echo "============================================================"
echo "✅ Infrastructure provisioned and tests passed!"
echo "To launch the Dataflow job, run:"
echo "python pipelines/streaming_beam_dataflow/pipeline.py \\"
echo "  --project=$PROJECT_ID \\"
echo "  --region=$REGION \\"
echo "  --input_subscription=projects/$PROJECT_ID/subscriptions/orders-stream-sub \\"
echo "  --output_table=$PROJECT_ID:ecommerce_analytics.orders_realtime \\"
echo "  --dlq_topic=projects/$PROJECT_ID/topics/orders-stream-dlq \\"
echo "  --temp_location=gs://$PROJECT_ID-dataflow-staging/temp \\"
echo "  --staging_location=gs://$PROJECT_ID-dataflow-staging/staging \\"
echo "  --runner=DataflowRunner"
echo "============================================================"
