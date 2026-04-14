# azure_synapse_pool.tf

# ── Synapse Dedicated SQL Pool ─────────────────────────────────────────────────
# DW100c is the smallest (cheapest) dedicated pool SKU — use only for dev/staging.
# For prod, bump to DW200c or higher depending on query concurrency needs.
#
# prevent_destroy = true is a safety net: even if someone removes this block
# from the config or runs terraform destroy, Terraform will error out instead
# of deleting the pool. Remove the lifecycle block ONLY when intentionally
# decommissioning (after backing up all data).
resource "azurerm_synapse_sql_pool" "ecom" {
  name                 = "EcomPool"
  synapse_workspace_id = azurerm_synapse_workspace.ecom.id
  sku_name             = "DW100c"
  create_mode          = "Default"

  tags = local.tags

  lifecycle {
    prevent_destroy = true
  }
}
