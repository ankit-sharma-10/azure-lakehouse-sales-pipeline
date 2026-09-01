# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Simulate API Ingestion (Sales Data)
# MAGIC This notebook acts as a mock API ingestion script. In a real-world scenario, this would use `requests` to fetch JSON/CSV data from an external API (like a CRM or ERP system) and save it to the ADLS Gen2 `landing` container.

# COMMAND ----------
import csv
import random
from datetime import datetime, timedelta

def simulate_api_fetch(num_records=500):
    """Generates synthetic sales data simulating an API response."""
    categories = ["Electronics", "Clothing", "Home & Garden", "Toys", "Sports & Outdoors"]
    regions = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle", "Miami"]
    statuses = ["Completed", "Completed", "Completed", "Pending", "Shipped", "Cancelled"]
    
    target_date = datetime.now().strftime("%Y-%m-%d")
    data = []
    
    for _ in range(num_records):
        order_id = f"ORD{random.randint(10000, 99999)}"
        product_id = f"PROD{random.randint(1, 50):03d}"
        
        # Inject intentional NULLs (10% chance)
        if random.random() > 0.10:
            customer_id = f"CUST{random.randint(1, 100):03d}"
        else:
            customer_id = ""
            
        category = random.choice(categories)
        quantity = random.randint(1, 15)
        unit_price = round(random.uniform(10.0, 2999.99), 2)
        region = random.choice(regions)
        status = random.choice(statuses)
        
        # Date distribution for incremental simulation
        if random.random() > 0.30:
            order_date = target_date
        else:
            days_ago = random.randint(1, 30)
            order_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            
        data.append([order_id, customer_id, product_id, category, quantity, unit_price, order_date, region, status])
    
    # Introduce a duplicate for testing
    if len(data) > 0:
        data.append(data[0])
        
    return data

# COMMAND ----------
# In Databricks, we write directly to the DBFS mount point for the landing zone
try:
    landing_path = "/mnt/landing/sales_api_data.csv"
    
    # Simulate API fetch
    print("Fetching data from mock Sales API...")
    api_data = simulate_api_fetch(500)
    
    # Write to ADLS via PySpark
    schema = ["order_id", "customer_id", "product_id", "category", "quantity", "unit_price", "order_date", "region", "status"]
    
    df_api = spark.createDataFrame(api_data, schema=schema)
    df_api.write.mode("overwrite").option("header", "true").csv(landing_path)
    
    print(f"Successfully ingested {df_api.count()} records to {landing_path}")
    
except Exception as e:
    print(f"Could not execute simulated API script on DBFS. If running locally, this is expected: {e}")
