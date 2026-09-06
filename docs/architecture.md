# Azure Lakehouse Architecture

## Overview
This repository implements a Medallion Architecture (Bronze, Silver, Gold) over an Azure Data Lake Storage (ADLS) Gen2 using Databricks and PySpark. 

## End-to-End Pipeline Flow

1. **Source / Landing (Raw)**
   - External data or daily dumps are placed into a `landing` container via an external process or simulated data generator.
   - Azure Data Factory (ADF) uses a Copy Activity to ingest this data into the `raw` container, ensuring the ingestion mechanism is decoupled from the transformation logic.

2. **Bronze Layer (Raw Ingestion)**
   - **Tool**: Databricks / PySpark
   - **Purpose**: Retain a raw, immutable history of ingested data.
   - **Process**: PySpark reads the CSV files from `raw`. It appends ingestion metadata (timestamps, run IDs, row hashes) and UPSERTs (MERGE) the data into a Delta table.

3. **Silver Layer (Cleansed & Standardized)**
   - **Tool**: Databricks / PySpark
   - **Purpose**: Provide a cleansed, validated, and highly performant query layer.
   - **Process**: Reads incrementally from Bronze. Performs data quality checks, handles NULLs, enforces uppercase standardization on string columns, deduplicates rows based on ingestion timestamps, and enforces schema types. Writes the output into the Silver Delta table.

4. **Gold Layer (Analytics & KPIs)**
   - **Tool**: Databricks / PySpark
   - **Purpose**: Serve business-level aggregations and KPIs ready for BI tools (e.g., PowerBI).
   - **Process**: Reads from Silver and calculates daily metrics, customer lifetime value, category performance, and regional sales dashboards.

## Core Technologies
- **Azure Data Factory**: Pipeline orchestration and scheduling.
- **Azure Data Lake Storage Gen2**: Scalable raw and structured storage.
- **Azure Databricks**: Distributed data processing and transformation.
- **Delta Lake**: ACID transactions, time travel, and UPSERT capabilities on data lakes.
- **PySpark**: Distributed data processing engine.
