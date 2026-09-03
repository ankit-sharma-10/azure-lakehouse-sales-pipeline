# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Azure Lakehouse Sales Pipeline - Main Orchestrator
# MAGIC This notebook orchestrates the Medallion Architecture (Bronze -> Silver -> Gold).
# MAGIC It is designed to be triggered by Azure Data Factory.

# COMMAND ----------
from src.transformations.bronze_ingestion import ingest_to_bronze, update_watermark
from src.transformations.silver_cleansing import process_silver, optimize_silver
from src.transformations.gold_aggregations import process_gold
from datetime import datetime

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Widget Parameters
# MAGIC Capture parameters passed from Azure Data Factory

# COMMAND ----------
dbutils.widgets.text("trigger_date", datetime.now().strftime("%Y-%m-%d"), "Trigger Date (YYYY-MM-DD)")
dbutils.widgets.text("pipeline_run_id", f"manual_run_{datetime.now().strftime('%Y%m%d%H%M%S')}", "Pipeline Run ID")
dbutils.widgets.dropdown("processing_mode", "incremental", ["incremental", "full"], "Processing Mode")

trigger_date = dbutils.widgets.get("trigger_date")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
processing_mode = dbutils.widgets.get("processing_mode")

print(f"==================================================")
print(f"Starting Sales Pipeline (Run ID: {pipeline_run_id})")
print(f"Mode: {processing_mode.upper()} | Trigger Date: {trigger_date}")
print(f"==================================================")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Execute Bronze Layer (Ingestion)

# COMMAND ----------
# In Databricks, the `spark` session object is available natively in the global context.
records_processed = ingest_to_bronze(spark, trigger_date, pipeline_run_id, processing_mode=processing_mode)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Execute Silver & Gold Layers (Transformations)

# COMMAND ----------
if records_processed > 0:
    # Silver Layer
    process_silver(spark, trigger_date, processing_mode=processing_mode)
    optimize_silver(spark)
    
    # Gold Layer
    process_gold(spark)
    
    # Update Watermark on Success (for incremental runs)
    if processing_mode == "incremental":
        current_ts = datetime.now()
        update_watermark(spark, pipeline_run_id, current_ts)
        
    print("Pipeline executed successfully.")
    dbutils.notebook.exit("SUCCESS")
else:
    print("No data processed. Pipeline completed.")
    dbutils.notebook.exit("NO_DATA")
