# NYC Taxi ETL — 2-Minute Demo Script

A tight cue-card script for interviews and portfolio walkthroughs. ~280 words, ~120 seconds at conversational pace. Numbers are from the actual January 2024 run.

---

## Opening (10 sec)

"This is a portfolio ETL project — NYC Yellow Taxi data, **~3 million rows for January 2024**, run through a **Bronze → Silver Medallion pipeline** on both **Databricks and Microsoft Fabric**. Same PySpark code on both platforms; the cross-platform comparison is itself part of the demo."

---

## Architecture (30 sec)

"Three layers.

- **Landing** is the raw Parquet file in cloud storage — untouched, the audit point that confirms what arrived.
- **Bronze** ingests it into a Delta table and tags every row with `ingestion_timestamp` and `source_file` for lineage — no business transforms. Bronze must stay loyal to source so we can re-derive Silver if a rule changes.
- **Silver** applies validation rules: `fare_amount > 0`, `passenger_count BETWEEN 1 AND 6`, chronological pickup before dropoff. Rows that fail get tagged with the rule names and **quarantined to a `rejected_records` table — not dropped**. Dropping hides quality issues; quarantining makes them recoverable."

---

## Demo (50 sec — show screenshots)

> **Show screenshot 1 — `pipeline_log`**

"Here's the audit log. Nine rows, every pipeline run captured. **Notice the first row — that's a real failure caught during development with the actual exception text.** Most student projects only show the happy path; mine captures both successes and failures with full forensic detail."

> **Show screenshot 2 — reconciliation**

"Here's the row reconciliation. **Bronze 2.96 million equals Silver 2.76 million, plus 68 thousand rejected, plus 140 thousand structural nulls — math closes exactly.** There is no silent data loss in this pipeline. That `assert` line is intentional — it stops the pipeline if the math breaks."

> **Show screenshot 3 — rejection breakdown**

"Rejection breakdown by rule. `fare_not_positive` is the biggest bucket — mostly $0 refund codes. `passenger_count_out_of_range` is second — drivers logging 0 passengers on real trips. **That second category is recoverable revenue** — fix the upstream rule, re-process from `rejected_records`. That's the whole argument for quarantining over dropping."

---

## Senior signals (20 sec)

"A few design decisions worth calling out:

- **Hard vs soft checks** — reconciliation `raise`s; drift threshold logs `WARN`. Different kinds of failure get different escalation.
- **The audit job writes to the same `pipeline_log` it monitors**, so the monitoring is itself observable.
- **Dual-platform implementation proves the architecture is portable** — Spark code is identical between Databricks and Fabric; the platforms differ in storage, governance, and the namespace model. Databricks uses Unity Catalog three-level naming; Fabric is Lakehouse-scoped."

---

## Closing (10 sec)

"For production I'd add a **Gold layer** with a star schema for analytics consumers, `MERGE INTO` for incremental loads keyed on a synthetic `trip_id`, and graduate the inline validation asserts to **Great Expectations** or **Delta Live Tables expectations** — same quarantine pattern, more rules, version-controlled."

---

# Anticipated Q&A

Use these if interviewers probe deeper. Confidence here separates "did the project" from "understood the project."

### "Why Delta over plain Parquet?"
ACID transactions, schema enforcement, time travel via `DESCRIBE HISTORY`, and `MERGE INTO` for upserts. Plain Parquet gives me none of that — concurrent writes can corrupt the dataset.

### "Why a separate `rejected_records` table instead of a `is_valid` boolean column?"
Two reasons. First, an array column for `validation_failures` lets a single row fail multiple rules at once and still be queryable per-rule via `explode`. Second, separating valid from invalid means downstream consumers point at `silver_yellow_taxi` and never accidentally pull in rejected data. Defense in depth.

### "Why is the audit job a separate notebook?"
Separation of concerns. Pipeline notebooks write data; audit jobs read data and decide whether the run was healthy enough to publish downstream. Mixing them means a failing audit blocks the data write — wrong dependency direction.

### "What happens if a Bronze write succeeds but the audit log write fails?"
Right now we'd have a successful Bronze table with no log entry — observability blind spot. In production I'd wrap them in a single transaction or use a write-ahead pattern: log `IN_PROGRESS` first, then update to `SUCCESS` or `FAILED`. For this project it was an accepted tradeoff.

### "Why is the drift threshold 5%?"
Arbitrary, configurable, justified empirically for this dataset. In production I'd derive it from a rolling 30-day baseline rather than hardcode it — sudden 2x drift spikes are the real signal, not the absolute number.

### "Databricks vs Fabric — when would you pick each?"
Databricks for heavy data engineering workloads and ML — Photon engine, Unity Catalog governance, mature tooling, more granular cost control. Fabric when the org is already heavily invested in the Microsoft stack — direct Power BI integration via OneLake shortcuts, unified identity through Entra ID, simpler procurement for Microsoft customers.

### "What was the hardest part?"
Honestly, the platform-specific namespace differences. Databricks' UC three-level naming is explicit; Fabric's Lakehouse binding is implicit and has to be configured per-notebook. I hit a `DoesNotExistException` in Fabric that took a while to trace back to the Lakehouse-attachment model — not a code bug, an operational difference between platforms.
