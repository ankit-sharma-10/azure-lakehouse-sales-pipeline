# Databricks notebook source
import time
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, 
    sha2, concat_ws, max as spark_max
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable

from src.config.settings import RAW_DATA_PATH, BRONZE_PATH, WATERMARK_PATH

def get_last_watermark(spark):
    """Get the last processed timestamp from the watermark table."""
    try:
        if DeltaTable.isDeltaTable(spark, WATERMARK_PATH):
            watermark_df = spark.read.format("delta").load(WATERMARK_PATH)
            return watermark_df.select(spark_max("last_processed_timestamp")).collect()[0][0]
        return None
    except Exception as e:
        print(f"Warning: Could not read watermark table. Proceeding with full load. Details: {e}")
        return None

def update_watermark(spark, pipeline_run_id, processed_timestamp):
    """Update the watermark after successful processing."""
    watermark_schema = StructType([
        StructField("pipeline_run_id", StringType(), False),
        StructField("last_processed_timestamp", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])
    watermark_df = spark.createDataFrame(
        [(pipeline_run_id, processed_timestamp, current_timestamp())], 
        watermark_schema
    )
    watermark_df.write.format("delta").mode("append").save(WATERMARK_PATH)
    print(f"Watermark updated to {processed_timestamp}")

def ingest_to_bronze(spark, trigger_date, pipeline_run_id, processing_mode="incremental"):
    """
    Reads raw CSV data, filters based on watermark (if incremental),
    adds metadata, and UPSERTs into the Bronze Delta table.
    """
    print(f"--- Starting Bronze Ingestion (Mode: {processing_mode}) ---")
    bronze_start_time = time.time()
    
    # 1. Read Raw Data
    raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_DATA_PATH)
    
    # 2. Apply Incremental Filtering
    last_watermark = get_last_watermark(spark)
    if processing_mode == "incremental" and last_watermark:
        print(f"Incremental Load: Processing data after {last_watermark}")
        raw_df = raw_df.filter(col("order_date") >= trigger_date)
    else:
        print("Full Load: Processing all available data in Raw.")
    
    record_count = raw_df.count()
    print(f"Raw records fetched: {record_count}")
    
    if record_count == 0:
        print("No new records to process. Exiting Bronze ingestion.")
        return 0

    # 3. Add Bronze Auditing Metadata
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", to_date(current_timestamp())) \
        .withColumn("pipeline_run_id", lit(pipeline_run_id)) \
        .withColumn("source_file", lit(RAW_DATA_PATH)) \
        .withColumn("record_hash", sha2(concat_ws("||", *raw_df.columns), 256))
    
    # 4. UPSERT (MERGE) into Bronze Delta Table
    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        bronze_delta = DeltaTable.forPath(spark, BRONZE_PATH)
        bronze_delta.alias("target").merge(
            bronze_df.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdate(
            condition="target.record_hash != source.record_hash",
            set={col_name: f"source.{col_name}" for col_name in bronze_df.columns}
        ).whenNotMatchedInsertAll().execute()
        print("Bronze MERGE complete.")
    else:
        # First time load
        bronze_df.write.format("delta").mode("overwrite").partitionBy("ingestion_date").save(BRONZE_PATH)
        print("Bronze initial OVERWRITE complete.")
        
    duration = time.time() - bronze_start_time
    print(f"Bronze Ingestion finished in {duration:.2f}s")
    return record_count
