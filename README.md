# NYC Taxi ETL Pipeline — Medallion Architecture (Databricks + Microsoft Fabric)

End-to-end ETL pipeline ingesting NYC Yellow Taxi trip data through a Bronze → Silver Medallion architecture, implemented in parallel on **Databricks Free Edition** and **Microsoft Fabric**, with full audit logging, rejected-records quarantine, and observable failure handling.

---

## Problem

Build a production-shape ETL pipeline that:
1. Ingests raw trip data with full lineage tracking,
2. Cleans and validates the data without silently dropping bad rows,
3. Logs every run (success or failure) into an audit table,
4. Runs identically on two different lakehouse platforms — proving the pipeline logic is portable.

**Source:** [NYC TLC Yellow Taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), January 2024 (~3M rows, Parquet).

---

## Architecture

```
                        ┌──────────────┐
                        │  NYC TLC     │
                        │  Parquet     │
                        └──────┬───────┘
                               │
                               ▼
                ┌──────────────────────────────┐
                │  Landing                     │  raw file, untouched
                │  - Databricks: UC Volume     │
                │  - Fabric: Lakehouse Files   │
                └──────────────┬───────────────┘
                               │
                               ▼
                ┌──────────────────────────────┐
                │  Bronze (Delta)              │  + ingestion_timestamp
                │  bronze_yellow_taxi          │  + source_file
                │  ~2.96M rows                 │  schema preserved
                └──────────────┬───────────────┘
                               │
                               ▼
                ┌──────────────────────────────┐
                │  Silver (Delta)              │  cleaned, typed, validated
                │  silver_yellow_taxi          │  business rules applied
                │  rejected_records ◄──────────┤  (quarantined, not dropped)
                └──────────────────────────────┘

   pipeline_log  (Delta)   ◄── every stage appends one row per run
     run_id, stage, rows_in, rows_out, status, error_message, run_timestamp
```

---

## Tech stack

| Layer | Databricks | Microsoft Fabric |
|---|---|---|
| Compute | Serverless (Photon) | Spark 3.5 (OSS) |
| Storage | Unity Catalog managed tables | OneLake / Lakehouse |
| Format | Delta Lake (zstd, deletion vectors) | Delta Lake |
| Code | PySpark | PySpark (identical) |
| Governance | Unity Catalog | Workspace + Fabric permissions |

The pipeline logic is identical on both platforms — only the source paths and table-naming conventions differ.

---

## Repository layout

```
.
├── README.md
├── databricks/
│   ├── 01_landing.ipynb
│   ├── 02_bronze.ipynb
│   ├── 03_silver.ipynb         # Day 2
│   └── 04_audit_check.ipynb    # Day 2
├── fabric/
│   ├── fabric_01_landing.ipynb
│   ├── fabric_02_bronze.ipynb
│   ├── fabric_03_silver.ipynb  # Day 2
│   └── fabric_04_audit_check.ipynb # Day 2
└── docs/
    ├── architecture.md
    └── screenshots/            # demo evidence
```

---

## Key design decisions

- **Medallion (Bronze/Silver/Gold):** clear separation of concerns. Bronze is loyal to source + lineage metadata; Silver is the cleaned source-of-truth; Gold (out of scope) would hold business aggregations.
- **Delta over plain Parquet:** ACID transactions, schema enforcement, time travel (`DESCRIBE HISTORY`), and `MERGE INTO` for incremental loads. Plain Parquet gives none of that.
- **Rejected records, not dropped:** bad rows go to `rejected_records` with the rule that flagged them. Surfaces data quality issues instead of hiding them; allows backfill once root cause is fixed.
- **Append-only audit log:** every notebook run appends one row per stage to `pipeline_log` with `run_id`, `rows_in`, `rows_out`, `status`, `error_message`. Failures are observable, not silent.
- **Try/except at the stage level:** errors are logged with `status=FAILED` and the actual exception message *before* re-raising. Real failure during development was captured and persisted — see Day 1 audit log.
- **Dual platform:** demonstrates that Spark code is portable; differences between Databricks and Fabric are governance and operations, not compute logic.

---

## Reproduce the pipeline

### Prerequisites
- Databricks Free Edition account
- Microsoft Fabric trial workspace
- The Jan 2024 yellow taxi Parquet from TLC

### Databricks
1. Catalog: create schema `taxi` under the `workspace` catalog.
2. Inside `taxi`, create a managed Volume named `raw`.
3. Upload `yellow_tripdata_2024-01.parquet` into `/Volumes/workspace/taxi/raw/`.
4. Import notebooks from `/databricks/`, run them in numeric order.

### Fabric
1. Create a workspace assigned to Fabric Trial capacity.
2. Create a Lakehouse named `taxi_lh`.
3. Upload `yellow_tripdata_2024-01.parquet` into the Lakehouse `Files/` folder.
4. Import notebooks from `/fabric/`, attach them to `taxi_lh`, run in numeric order.

---

## Status

- [x] Day 1: Landing + Bronze layer + audit log on both platforms
- [ ] Day 2: Silver transforms, validation, rejected-records, audit closure
- [ ] Stretch: Gold layer with star schema, incremental loads with `MERGE INTO`, scheduled orchestration

---

## What's deliberately out of scope (and how I'd extend)

- **Gold layer / star schema** — `fact_trips` + `dim_zone` + `dim_payment_type` for analytics.
- **Incremental loads** — replace `mode("overwrite")` with `MERGE INTO` keyed on a synthetic `trip_id`, plus Delta CDF (Change Data Feed) for downstream.
- **Orchestration** — Databricks Workflows or Fabric Pipelines with retries, SLA alerts, and dependency DAGs.
- **Data quality framework** — graduate from inline asserts to Delta Live Tables expectations (Databricks) or Great Expectations (portable), with the same rejected-records contract.
- **Streaming** — Structured Streaming over the same Bronze table for near-real-time Silver materialization.
