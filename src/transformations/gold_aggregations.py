# Databricks notebook source
import time
from pyspark.sql.functions import (
    col, current_timestamp, sum as spark_sum, count, avg,
    max as spark_max, min as spark_min, when
)
from pyspark.sql.types import DoubleType

from src.config.settings import SILVER_PATH, GOLD_PATH

def process_gold(spark):
    """
    Reads Silver data and aggregates into business-level Gold tables
    (Region, Category, Customer, Daily dashboards).
    """
    print("--- Starting Gold Aggregations ---")
    gold_start_time = time.time()
    
    # 1. Read Silver Data
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    
    # --- Aggregation 1: Sales by Region ---
    gold_region_df = silver_df.groupBy("region", "order_year", "order_month").agg(
        count("order_id").alias("total_orders"),
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value")
    ).withColumn("gold_processed_timestamp", current_timestamp())
    
    GOLD_REGION_PATH = f"{GOLD_PATH}/sales_by_region"
    gold_region_df.write.format("delta").mode("overwrite").partitionBy("order_year", "order_month").save(GOLD_REGION_PATH)
    
    # --- Aggregation 2: Sales by Category ---
    gold_category_df = silver_df.groupBy("category", "order_year", "order_month").agg(
        count("order_id").alias("total_orders"),
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("total_amount").alias("total_revenue"),
        count(when(col("status") == "COMPLETED", 1)).alias("completed_orders"),
        count(when(col("status") == "PENDING", 1)).alias("pending_orders")
    ).withColumn("completion_rate", (col("completed_orders") / col("total_orders") * 100).cast(DoubleType())) \
     .withColumn("gold_processed_timestamp", current_timestamp())
    
    GOLD_CATEGORY_PATH = f"{GOLD_PATH}/sales_by_category"
    gold_category_df.write.format("delta").mode("overwrite").partitionBy("order_year", "order_month").save(GOLD_CATEGORY_PATH)
    
    # --- Aggregation 3: Customer Analytics ---
    gold_customer_df = silver_df.groupBy("customer_id").agg(
        count("order_id").alias("total_orders"),
        spark_sum("total_amount").alias("lifetime_value"),
        avg("total_amount").alias("avg_order_value"),
        spark_max("order_date").alias("last_order_date"),
        spark_min("order_date").alias("first_order_date")
    ).withColumn("customer_segment", 
        when(col("lifetime_value") > 5000, "PLATINUM")
        .when(col("lifetime_value") > 2000, "GOLD")
        .when(col("lifetime_value") > 500, "SILVER")
        .otherwise("BRONZE")
    ).withColumn("gold_processed_timestamp", current_timestamp())
    
    GOLD_CUSTOMER_PATH = f"{GOLD_PATH}/customer_analytics"
    gold_customer_df.write.format("delta").mode("overwrite").save(GOLD_CUSTOMER_PATH)
    
    # --- Aggregation 4: Daily Sales Dashboard ---
    gold_daily_df = silver_df.groupBy("order_date", "region", "category").agg(
        count("order_id").alias("daily_orders"),
        spark_sum("total_amount").alias("daily_revenue"),
        spark_sum("quantity").alias("daily_quantity")
    ).withColumn("gold_processed_timestamp", current_timestamp())
    
    GOLD_DAILY_PATH = f"{GOLD_PATH}/daily_sales_dashboard"
    gold_daily_df.write.format("delta").mode("overwrite").partitionBy("order_date").save(GOLD_DAILY_PATH)
    
    duration = time.time() - gold_start_time
    print(f"Gold Aggregations finished in {duration:.2f}s")
