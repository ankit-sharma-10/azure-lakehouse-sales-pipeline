# Databricks notebook source
import time
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, year, month, dayofmonth, 
    row_number, coalesce, trim, upper
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType
from delta.tables import DeltaTable

from src.config.settings import BRONZE_PATH, SILVER_PATH

def process_silver(spark, trigger_date, processing_mode="incremental"):
    """
    Reads Bronze data, cleanses, standardizes, deduplicates, 
    and UPSERTs into the Silver Delta table.
    """
    print(f"--- Starting Silver Cleansing (Mode: {processing_mode}) ---")
    silver_start_time = time.time()

    # 1. Read Bronze Data
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    
    # 2. Filter for Incremental
    if processing_mode == "incremental":
        bronze_df = bronze_df.filter(col("ingestion_date") == trigger_date)
    
    record_count = bronze_df.count()
    print(f"Bronze records fetched for Silver processing: {record_count}")
    
    if record_count == 0:
        print("No new records to process. Exiting Silver processing.")
        return 0

    # 3. Cleansing & Type Casting
    silver_df = bronze_df \
        .withColumn("customer_id", coalesce(col("customer_id"), lit("UNKNOWN"))) \
        .withColumn("category", coalesce(col("category"), lit("Uncategorized"))) \
        .withColumn("region", coalesce(col("region"), lit("Unknown"))) \
        .withColumn("status", coalesce(col("status"), lit("Unknown"))) \
        .withColumn("quantity", col("quantity").cast(IntegerType())) \
        .withColumn("unit_price", col("unit_price").cast(DoubleType())) \
        .withColumn("order_date", to_date(col("order_date"))) \
        .withColumn("category", upper(trim(col("category")))) \
        .withColumn("region", upper(trim(col("region")))) \
        .withColumn("status", upper(trim(col("status")))) \
        .withColumn("total_amount", col("quantity") * col("unit_price")) \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("order_day", dayofmonth(col("order_date")))

    # 4. Deduplication (keep latest per order_id based on ingestion timestamp)
    window_spec = Window.partitionBy("order_id").orderBy(col("ingestion_timestamp").desc())
    silver_df = silver_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1).drop("row_num") \
        .filter((col("quantity") > 0) & (col("unit_price") > 0)) \
        .withColumn("silver_processed_timestamp", current_timestamp()) \
        .withColumn("is_valid", lit(True))

    # 5. UPSERT (MERGE) into Silver Delta Table
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_delta = DeltaTable.forPath(spark, SILVER_PATH)
        silver_delta.alias("target").merge(
            silver_df.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdate(
            condition="target.record_hash != source.record_hash",
            set={col_name: f"source.{col_name}" for col_name in silver_df.columns}
        ).whenNotMatchedInsertAll().execute()
        print("Silver MERGE complete.")
    else:
        # First time load
        silver_df.write.format("delta").mode("overwrite").partitionBy("order_year", "order_month").save(SILVER_PATH)
        print("Silver initial OVERWRITE complete.")

    duration = time.time() - silver_start_time
    print(f"Silver Cleansing finished in {duration:.2f}s")
    
    return record_count

def optimize_silver(spark):
    """Runs OPTIMIZE and Z-ORDER on the Silver table to improve query performance."""
    print("--- Running Performance Optimizations (Silver) ---")
    start_time = time.time()
    try:
        spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}` ZORDER BY (region, category, order_date)")
        print(f"OPTIMIZE + Z-Order completed in {time.time() - start_time:.2f}s")
    except Exception as e:
        print(f"Optimization failed: {e}")
