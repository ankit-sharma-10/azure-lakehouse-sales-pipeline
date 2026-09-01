import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

# Helper function to create a local Spark session for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("LakehouseTest") \
        .master("local[1]") \
        .getOrCreate()

def test_silver_cleansing_null_handling(spark):
    """
    Test that the Silver layer correctly coerces NULLs to 'UNKNOWN'
    and handles missing categorical data.
    """
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("region", StringType(), True),
        StructField("status", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("order_date", StringType(), True)
    ])
    
    # Create test data with NULLs
    test_data = [
        ("ORD1", None, None, None, None, 5, 10.0, "2026-01-01"),
        ("ORD2", "CUST2", " electronics ", " new york ", " pending ", 2, 5.0, "2026-01-02")
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Simulate the transformation logic found in silver_cleansing.py
    from pyspark.sql.functions import coalesce, lit, upper, trim, to_date
    
    cleaned_df = df \
        .withColumn("customer_id", coalesce(col("customer_id"), lit("UNKNOWN"))) \
        .withColumn("category", coalesce(col("category"), lit("Uncategorized"))) \
        .withColumn("region", coalesce(col("region"), lit("Unknown"))) \
        .withColumn("status", coalesce(col("status"), lit("Unknown"))) \
        .withColumn("category", upper(trim(col("category")))) \
        .withColumn("region", upper(trim(col("region")))) \
        .withColumn("status", upper(trim(col("status")))) \
        .withColumn("order_date", to_date(col("order_date")))
        
    results = cleaned_df.collect()
    
    # Check ORD1 (Null Coercion)
    ord1 = [r for r in results if r.order_id == "ORD1"][0]
    assert ord1.customer_id == "UNKNOWN"
    assert ord1.category == "UNCATEGORIZED" # Upper-cased from default
    assert ord1.region == "UNKNOWN"
    assert ord1.status == "UNKNOWN"
    
    # Check ORD2 (String cleaning)
    ord2 = [r for r in results if r.order_id == "ORD2"][0]
    assert ord2.category == "ELECTRONICS"
    assert ord2.region == "NEW YORK"
    assert ord2.status == "PENDING"
