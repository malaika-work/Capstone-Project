# azure_core.tf
# Core Azure infrastructure ONLY (no secrets, no RBAC, no linked services)
# Everything security/config related will be handled manually via Azure Portal

# ── Resource Group ─────────────────────────────────────────────
resource "azurerm_resource_group" "ecom" {
  name     = "${local.resource_prefix}-rg"
  location = "East US 2"
  tags     = local.tags
}

# ── ADLS Gen2 Storage Account ─────────────────────────────────
resource "azurerm_storage_account" "adls" {
  name                     = "${local.project}adls${local.environment}"
  resource_group_name      = azurerm_resource_group.ecom.name
  location                 = azurerm_resource_group.ecom.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  tags = local.tags
}

# ── Medallion Containers ──────────────────────────────────────
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "archive" {
  name                  = "archive"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# ── Synapse Filesystem ─────────────────────────────────────────
resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  name               = "synapse"
  storage_account_id = azurerm_storage_account.adls.id
}

# ── Azure Data Factory (No linked services) ────────────────────
resource "azurerm_data_factory" "ecom" {
  name                = "${local.resource_prefix}-capstone-adf"
  location            = azurerm_resource_group.ecom.location
  resource_group_name = azurerm_resource_group.ecom.name

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# ── Synapse Workspace ──────────────────────────────────────────
resource "azurerm_synapse_workspace" "ecom" {
  name                                 = "${local.resource_prefix}-synapse"
  resource_group_name                  = azurerm_resource_group.ecom.name
  location                             = "Central India"
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse.id

  sql_administrator_login          = var.synapse_sql_admin_login
  sql_administrator_login_password = var.synapse_sql_admin_password

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# ── Event Hub Namespace ───────────────────────────────────────
resource "azurerm_eventhub_namespace" "ecom" {
  name                 = "${local.resource_prefix}-ehns"
  location             = azurerm_resource_group.ecom.location
  resource_group_name  = azurerm_resource_group.ecom.name
  sku                  = "Standard"
  capacity             = 1
  auto_inflate_enabled = false

  tags = local.tags
}

# ── Event Hub ─────────────────────────────────────────────────
resource "azurerm_eventhub" "weather" {
  name                = "weather-stream"
  namespace_name      = azurerm_eventhub_namespace.ecom.name
  resource_group_name = azurerm_resource_group.ecom.name
  partition_count     = 2
  message_retention   = 1
}

# ── Consumer Groups ───────────────────────────────────────────
resource "azurerm_eventhub_consumer_group" "adf" {
  name                = "adf-consumer"
  namespace_name      = azurerm_eventhub_namespace.ecom.name
  eventhub_name       = azurerm_eventhub.weather.name
  resource_group_name = azurerm_resource_group.ecom.name
}

resource "azurerm_eventhub_consumer_group" "synapse" {
  name                = "synapse-consumer"
  namespace_name      = azurerm_eventhub_namespace.ecom.name
  eventhub_name       = azurerm_eventhub.weather.name
  resource_group_name = azurerm_resource_group.ecom.name
}

resource "azurerm_eventhub_consumer_group" "databricks" {
  name                = "databricks-consumer"
  namespace_name      = azurerm_eventhub_namespace.ecom.name
  eventhub_name       = azurerm_eventhub.weather.name
  resource_group_name = azurerm_resource_group.ecom.name
}

# ── Event Hub Auth Rules (NO secret storage here) ─────────────
resource "azurerm_eventhub_authorization_rule" "producer" {
  name                = "weather-producer"
  namespace_name      = azurerm_eventhub_namespace.ecom.name
  eventhub_name       = azurerm_eventhub.weather.name
  resource_group_name = azurerm_resource_group.ecom.name
  send                = true
}

resource "azurerm_eventhub_authorization_rule" "adf_listener" {
  name                = "adf-listener"
  namespace_name      = azurerm_eventhub_namespace.ecom.name
  eventhub_name       = azurerm_eventhub.weather.name
  resource_group_name = azurerm_resource_group.ecom.name
  listen              = true
}

# ── Outputs (used for manual setup) ───────────────────────────
output "adls_account_name" {
  value = azurerm_storage_account.adls.name
}

output "adf_principal_id" {
  value = azurerm_data_factory.ecom.identity[0].principal_id
}

output "synapse_principal_id" {
  value = azurerm_synapse_workspace.ecom.identity[0].principal_id
}

output "eventhub_producer_connection_string" {
  value     = azurerm_eventhub_authorization_rule.producer.primary_connection_string
  sensitive = true
}

output "eventhub_adf_connection_string" {
  value     = azurerm_eventhub_authorization_rule.adf_listener.primary_connection_string
  sensitive = true
}