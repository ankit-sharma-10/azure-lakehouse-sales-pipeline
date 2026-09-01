# Databricks notebook source
from src.config.settings import (
    STORAGE_ACCOUNT_NAME,
    RAW_CONTAINER,
    GOLD_CONTAINER,
    LANDING_CONTAINER,
    RAW_MOUNT,
    GOLD_MOUNT,
    LANDING_MOUNT,
)

def mount_container(spark_dbutils, container_name, mount_point):
    """
    Mounts an ADLS Gen2 container to Databricks if not already mounted.
    
    Args:
        spark_dbutils: The Databricks dbutils instance.
        container_name (str): The name of the ADLS container.
        mount_point (str): The Databricks File System (DBFS) mount point path.
    """
    try:
        # Securely fetch the Storage Account Key from Azure Key Vault via Databricks Secrets
        # Replace 'sales-kv-scope' with your actual Databricks secret scope name
        storage_account_key = spark_dbutils.secrets.get(scope="sales-kv-scope", key="storage-account-key")
    except Exception as e:
        print("WARNING: Could not fetch storage account key from Databricks secrets. Mount may fail in a real environment.")
        storage_account_key = "<STORAGE_ACCOUNT_KEY>"

    configs = {
        f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net": storage_account_key
    }
    
    try:
        # Check if already mounted
        if any(mount.mountPoint == mount_point for mount in spark_dbutils.fs.mounts()):
            print(f"{mount_point} is already mounted.")
            return True
        
        print(f"Mounting {container_name} to {mount_point}...")
        spark_dbutils.fs.mount(
            source=f"wasbs://{container_name}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"Successfully mounted {container_name} to {mount_point}!")
        return True
    except Exception as e:
        print(f"Mount failed for {container_name}. Error: {str(e)[:200]}")
        return False

def mount_all(spark_dbutils):
    """Mounts all required data containers for the Lakehouse pipeline."""
    mounts = [
        (RAW_CONTAINER, RAW_MOUNT),
        (GOLD_CONTAINER, GOLD_MOUNT),
        (LANDING_CONTAINER, LANDING_MOUNT)
    ]
    
    success = True
    for container, mount_point in mounts:
        if not mount_container(spark_dbutils, container, mount_point):
            success = False
            
    return success

# Example Usage in Databricks:
# mount_all(dbutils)
