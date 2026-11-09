# GCP System Design & Technical Interview Deep Dive

This document contains targeted, scenario-based questions and high-impact answers frequently asked in senior GCP Data Engineer, Cloud Architect, and DevOps interviews.

---

## 1. Batch vs. Streaming Architecture Trade-Offs

### Scenario:
*"The business wants real-time revenue dashboards for Black Friday. Should we build a Lambda or Kappa architecture, and how does GCP support this?"*

### Key Answer Points:
- **Kappa Architecture over Lambda**: With GCP, the **Kappa Architecture** (single streaming pipeline for both real-time and historical replay) is preferred over dual-path Lambda architecture.
  - **Unified Beam Model**: Cloud Dataflow (Apache Beam) uses the exact same API and transform logic for batch and streaming (`PCollection` vs bounded/unbounded streams).
  - **Replayability**: If downstream logic or schema changes, create a new BigQuery table, snapshot the Pub/Sub subscription or replay historical files stored in GCS through the same Beam pipeline without maintaining two disparate codebases (e.g. Spark for batch + Flink for stream).
  - **Storage Write API**: Modern BigQuery ingestion uses the Storage Write API with commit streams, achieving sub-5-second query latency with exactly-once delivery guarantees at fraction of streaming insert costs.

---

## 2. Choosing Storage Engines on GCP

| Workload Requirement | Chosen GCP Service | Why? |
| :--- | :--- | :--- |
| High QPS (>50,000 writes/sec), sub-10ms latency, key-based lookups | **Cloud Bigtable** | Wide-column NoSQL store built on Colossus; linear scaling with nodes. |
| Global ACID relational transactions, multi-region replication | **Cloud Spanner** | Uses TrueTime API (atomic clocks + GPS) for external consistency without locks across continents. |
| Enterprise OLAP aggregations, SQL ad-hoc queries, BI dashboards | **BigQuery** | Serverless Dremel execution engine with columnar capacitor storage; zero operational overhead. |
| Unstructured raw archives, audio/image assets, staging files | **Cloud Storage (GCS)** | Object storage with tiered lifecycle policies (Standard $\to$ Nearline $\to$ Coldline $\to$ Archive). |

---

## 3. Exactly-Once Processing Guarantees

### Question:
*"Pub/Sub guarantees at-least-once delivery. How do you prevent duplicate records in your BigQuery analytical tables?"*

### Three-Layer Deduplication Strategy:
1. **Pub/Sub Message Deduplication**:
   - Pub/Sub can enforce deduplication based on `message_id` within a 10-minute window if ordering keys or deduplication settings are configured.
2. **Dataflow Checkpoints & Watermarks**:
   - Apache Beam uses watermarking to track event-time vs. processing-time and manages checkpointed state. It de-duplicates elements within bounded tumbling or sliding windows using stateful `DoFn` or `Distinct` transforms.
3. **BigQuery Storage Write API Exactly-Once Delivery**:
   - The pipeline writes using default or committed streams with stream offsets. If a worker retries writing a batch of records, BigQuery detects identical stream offsets and prevents duplicate rows.
4. **Batch Layer Deduplication (Idempotent MERGE)**:
   - For historical loads or backfills, run an Airflow DAG with SQL `MERGE INTO` using `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_timestamp DESC) = 1`.

---

## 4. Cost Optimization Mastery in BigQuery

### What to state in the interview:
1. **Partitioning**:
   - Always partition by Date or Timestamp (`PARTITION BY DATE(order_timestamp)`).
   - Enforce `require_partition_filter = true` on production tables so runaway analyst queries without date filters get rejected automatically.
2. **Clustering**:
   - Cluster by high-cardinality search columns (e.g., `customer_id, status`). Unlike partitions (limited to 4,000 partitions), clustering has no limits and automatically sorts co-located blocks.
3. **Slot Reservations vs On-Demand**:
   - *On-Demand Pricing*: \$6.25 per TB scanned. Great for spiky, ad-hoc workloads.
   - *Capacity / Editions (Standard, Enterprise)*: Billed per slot-hour with autoscaling. Predictable budgeting for steady enterprise workloads.
4. **BigQuery BI Engine**:
   - Enable an in-memory execution cache for connected Looker Studio dashboards. Queries are served directly from RAM without scanning table bytes repeatedly.
