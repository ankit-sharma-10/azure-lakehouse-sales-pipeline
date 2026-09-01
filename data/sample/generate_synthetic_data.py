import csv
import random
from datetime import datetime, timedelta
import os

def generate_synthetic_data(num_records=1000, output_path="synthetic_sales_data.csv"):
    categories = ["Electronics", "Clothing", "Home & Garden", "Toys", "Sports & Outdoors"]
    regions = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle", "Miami"]
    statuses = ["Completed", "Completed", "Completed", "Pending", "Shipped", "Cancelled"]
    
    target_date = datetime.now().strftime("%Y-%m-%d")
    data = []
    
    for _ in range(num_records):
        order_id = f"ORD{random.randint(10000, 99999)}"
        product_id = f"PROD{random.randint(1, 50):03d}"
        
        # Inject intentional NULLs (10% chance) for data quality testing
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
    
    # Introduce a duplicate for deduplication logic testing
    if len(data) > 0:
        data.append(data[0])
        
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "category", "quantity", "unit_price", "order_date", "region", "status"])
        writer.writerows(data)
        
    print(f"Generated {len(data)} synthetic records at {output_path}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(current_dir, "synthetic_sales_data.csv")
    generate_synthetic_data(num_records=500, output_path=output_file)
