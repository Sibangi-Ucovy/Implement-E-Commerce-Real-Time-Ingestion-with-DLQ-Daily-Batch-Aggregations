# GCP Production Incident & Troubleshooting Playbook

This playbook outlines real-world diagnostics steps for common failure scenarios across GCP streaming and batch data architectures.

---

## Incident 1: Dataflow Pipeline Stuck / System Lag Increasing

### Severity: High
### Symptoms:
- Pub/Sub metric `subscription/oldest_unacked_message_age` steadily increases.
- Cloud Monitoring alert `System Lag > 300s` fires.
- BigQuery table has no new records for the last 20 minutes.

### Diagnostic Flow:
1. **Check Dataflow Job Status**:
   - Go to **Dataflow** $\to$ **Jobs** $\to$ Select Job.
   - Inspect the **Job Graph**. Which transform has high execution time?
2. **Identify Worker Autoscaling Limits**:
   - Check if workers hit `--maxNumWorkers`. If the worker count is flatlined at max and CPU utilization is > 90%, the cluster cannot keep pace with throughput.
   - **Fix**: Update the job with a higher worker cap or larger worker machine type (`n2-standard-4`).
3. **Inspect Hot Keys**:
   - Look for step execution wall time in `GroupByKey` or window aggregations.
   - If one key has 100x more elements than others (e.g. `customer_id = 'guest'`), one worker is overloaded while others sit idle.
   - **Fix**: Implement **key salting** (appending random integer 1-10 to the key) before grouping, then perform a two-stage aggregation.
4. **Inspect Worker Logs for OOM (Out Of Memory)**:
   - Filter logs: `resource.type="dataflow_step" severity>=ERROR`.
   - Search for `java.lang.OutOfMemoryError` or Python `MemoryError` caused by loading massive objects in memory inside a `DoFn`.

---

## Incident 2: Pub/Sub Messages Rejected / Dead-Letter Queue Flooding

### Severity: Medium to High
### Symptoms:
- Alert: `orders-stream-dlq` message rate > 10 messages/minute.
- Downstream metrics indicate missing transactions.

### Diagnostic Flow:
1. **Pull Messages from DLQ Subscription**:
   ```bash
   gcloud pubsub subscriptions pull orders-stream-dlq-sub --limit=5 --auto-ack=false
   ```
2. **Inspect Error Payload**:
   - The DLQ record contains `raw_payload` and `error_message`.
   - Look for:
     - Missing mandatory field (e.g. Upstream microservice deployed a new version that renamed `order_id` to `id`).
     - Timestamp format mismatch (e.g. Unix epoch sent instead of ISO-8601 string).
3. **Immediate Mitigation**:
   - Notify producer team to fix contract.
   - If backward-compatible schema adjustment is possible, update `ParseAndValidateOrderFn` in the Dataflow pipeline and redeploy using `--update` pipeline flag without draining messages.
4. **Replay DLQ Messages**:
   - Once the pipeline code is fixed, run a DLQ drain script to read messages from the DLQ topic and re-publish them back to `orders-stream-topic`.

---

## Incident 3: BigQuery Streaming Write API Quota & Permission Failures

### Severity: High
### Symptoms:
- Dataflow logs show: `google.api_core.exceptions.PermissionDenied: 403 Access Denied: Table ecommerce_analytics.orders_realtime`
- Or: `429 RESOURCE_EXHAUSTED` / `rateLimitExceeded`.

### Diagnostic Flow:
1. **Check Service Account Permissions**:
   - Verify the exact identity used by Dataflow workers:
     ```bash
     gcloud projects get-iam-policy $PROJECT_ID \
       --flatten="bindings[].members" \
       --format="table(bindings.role)" \
       --filter="bindings.members:serviceAccount:dataflow-pipeline-worker@$PROJECT_ID.iam.gserviceaccount.com"
     ```
   - Must contain:
     - `roles/bigquery.dataEditor`
     - `roles/bigquery.jobUser`
2. **Storage Write API Rate Limits**:
   - BigQuery Storage Write API default project quota is 3 GB/minute default commit stream quota.
   - If exceeding quota:
     - Consolidate writes with batch streaming buffers (e.g. micro-batching 1,000 records).
     - Request a quota increase in GCP Console: **IAM & Admin** $\to$ **Quotas** $\to$ `BigQuery Storage Write API throughput`.

---

## Incident 4: Private IP Worker Network Isolation (No External IP)

### Severity: High
### Symptoms:
- Dataflow job immediately fails during provisioning with:
  `Workflow failed. Causes: The worker VMs failed to start or connect to the controller.`

### Diagnostic Flow:
1. By default, secure enterprise pipelines use `--disablePublicIps` to satisfy security compliance.
2. In private mode, VMs **must** be able to reach Google APIs (storage.googleapis.com, pubsub.googleapis.com).
3. **Verification**:
   - Check if **Private Google Access** is enabled on the target subnet:
     ```bash
     gcloud compute networks subnets describe <SUBNET_NAME> \
       --region=<REGION> \
       --format="value(privateIpGoogleAccess)"
     ```
   - If `False`, enable it:
     ```bash
     gcloud compute networks subnets update <SUBNET_NAME> \
       --region=<REGION> \
       --enable-private-ip-google-access
     ```
4. **External Dependency Resolution**:
   - If the pipeline requires downloading third-party Python packages from PyPI at worker startup, a **Cloud NAT** gateway must be associated with the VPC router.
