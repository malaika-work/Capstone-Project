provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "tfstate" {
  name     = "ecom-tfstate-rg"
  location = "East US 2"
}

resource "azurerm_storage_account" "tfstate" {
  name                     = "ecomtfstate12345" #must be globally unique
  resource_group_name      = azurerm_resource_group.tfstate.name
  location                 = azurerm_resource_group.tfstate.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_name  = azurerm_storage_account.tfstate.name
  container_access_type = "private"
}
