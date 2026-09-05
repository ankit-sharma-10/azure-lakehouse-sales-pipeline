# Terraform infrastructure configuration for Azure Lakehouse Sales Pipeline

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Resource Group
resource "azurerm_resource_group" "lakehouse_rg" {
  name     = "rg-sales-lakehouse-prod"
  location = "East US"
}

# Azure Data Lake Storage Gen2 (Storage Account)
resource "azurerm_storage_account" "lakehouse_adls" {
  name                     = "stsaleslakehouseprod"
  resource_group_name      = azurerm_resource_group.lakehouse_rg.name
  location                 = azurerm_resource_group.lakehouse_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Essential for ADLS Gen2 / Lakehouse
}

# ADLS Containers (Landing, Raw, Gold)
resource "azurerm_storage_data_lake_gen2_filesystem" "container_landing" {
  name               = "landing"
  storage_account_id = azurerm_storage_account.lakehouse_adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "container_raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.lakehouse_adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "container_gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.lakehouse_adls.id
}

# Azure Databricks Workspace
resource "azurerm_databricks_workspace" "databricks_ws" {
  name                = "dbw-sales-lakehouse-prod"
  resource_group_name = azurerm_resource_group.lakehouse_rg.name
  location            = azurerm_resource_group.lakehouse_rg.location
  sku                 = "standard"
}

# Azure Data Factory
resource "azurerm_data_factory" "adf" {
  name                = "adf-sales-lakehouse-prod"
  location            = azurerm_resource_group.lakehouse_rg.location
  resource_group_name = azurerm_resource_group.lakehouse_rg.name
}

# Azure Key Vault (For Secret Management)
resource "azurerm_key_vault" "keyvault" {
  name                = "kv-sales-lakehouse-prod"
  location            = azurerm_resource_group.lakehouse_rg.location
  resource_group_name = azurerm_resource_group.lakehouse_rg.name
  tenant_id           = "<YOUR_TENANT_ID>"
  sku_name            = "standard"
}
