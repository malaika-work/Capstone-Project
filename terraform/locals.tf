# locals.tf

locals {
  project     = "ecom"
  environment = var.environment
  created_date = formatdate("YYYY-MM-DD", timestamp())

  tags = {
    project     = local.project
    environment = local.environment
    managed_by  = "terraform"
    created_date = local.created_date
  }

  # Naming convention: rideco-dev, rideco-staging, rideco-prod
  resource_prefix = "${local.project}-${local.environment}"
}
