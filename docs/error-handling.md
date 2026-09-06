# Failure Recovery & Error Handling

This pipeline is designed for idempotency and resilient execution, minimizing the need for manual intervention during transient failures.

## Idempotency (The Core Defense)
Because the PySpark logic relies on Delta Lake's `MERGE INTO` (UPSERT) instead of naive appends, the entire pipeline is **idempotent**. 
If a Databricks job fails midway, you do not need to manually delete half-written data. You simply re-run the pipeline. The same data will be safely merged without creating duplicates.

## Pipeline-Level Resilience (Azure Data Factory)

1. **Activity Retries**
   - The ingestion `Copy Activity` is configured with a retry policy (3 retries, 30-second interval) to handle transient ADLS network blips or temporary read locks on landing files.
   - The Databricks notebook execution activity is also configured with a single retry to handle transient cluster startup issues.

2. **Timeout Constraints**
   - Long-running hanging jobs are killed automatically using a 12-hour timeout on all major activities.

## Common Failure Scenarios

### 1. Missing Source Files
- **Detection**: ADF Copy Activity fails instantly.
- **Recovery**: Auto-retries wait 30 seconds. If upstream systems are delayed, manual triggering is required after files land.

### 2. Schema Drift / Mismatch
- **Detection**: Databricks throws an exception during Bronze ingestion due to unexpected schema changes.
- **Recovery**: Depending on business rules, either enable Delta Schema Evolution (`mergeSchema = true`) for expected changes, or halt processing to manually correct the upstream producer.

### 3. Cluster Failures
- **Detection**: Databricks activity fails in ADF.
- **Recovery**: ADF auto-retries. If the cluster configuration is invalid, manual intervention is required.
