import os

# Storage Account Details
# IMPORTANT: In a production environment, use Azure Key Vault or environment variables.
# Do not hardcode access keys here.
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME", "<STORAGE_ACCOUNT>")

# Container Paths
RAW_CONTAINER = "raw"
GOLD_CONTAINER = "gold"
LANDING_CONTAINER = "landing"

# Mount Points (Databricks)
RAW_MOUNT = f"/mnt/{RAW_CONTAINER}"
GOLD_MOUNT = f"/mnt/{GOLD_CONTAINER}"
LANDING_MOUNT = f"/mnt/{LANDING_CONTAINER}"

# Data Paths
RAW_DATA_PATH = f"{RAW_MOUNT}/sales_data"
BRONZE_PATH = f"{RAW_MOUNT}/bronze/sales"
SILVER_PATH = f"{RAW_MOUNT}/silver/sales"
GOLD_PATH = f"{GOLD_MOUNT}/sales"

# Checkpoint/Metadata Paths
CHECKPOINT_PATH = f"{RAW_MOUNT}/checkpoints/sales"
WATERMARK_PATH = f"{RAW_MOUNT}/metadata/watermark"
