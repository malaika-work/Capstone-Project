# environments/dev.tfvars
environment                     = "dev"
location                        = "East US 2"
synapse_location                = "Central India"
synapse_sql_admin_login         = "sqladminuser"
eventhub_partition_count        = 2
eventhub_message_retention_days = 1
# synapse_sql_admin_password passed via GitHub Actions secret, not committed
