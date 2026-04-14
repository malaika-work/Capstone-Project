# providers.tf

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }

  # Remote backend — one state file per environment
  # CI/CD overrides `key` per environment:
  #   dev:     -backend-config="key=dev.tfstate"
  #   staging: -backend-config="key=staging.tfstate"
  #   prod:    -backend-config="key=prod.tfstate"
  backend "azurerm" {
    resource_group_name  = "ecom-tfstate-rg"
    storage_account_name = "ecomtfstate"
    container_name       = "tfstate"
    key                  = "ecom.terraform.tfstate"
  }
}

# Reads credentials from environment variables:
# ARM_CLIENT_ID, ARM_CLIENT_SECRET, ARM_TENANT_ID, ARM_SUBSCRIPTION_ID
provider "azurerm" {
  features {
    key_vault {
      # Allows Terraform to fully delete Key Vault secrets during destroy.
      # Safe for dev/staging — in prod, set purge_soft_deleted_secrets_on_destroy = false
      purge_soft_deleted_secrets_on_destroy = true
      recover_soft_deleted_secrets          = true
    }
  }
}
