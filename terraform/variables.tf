# variables.tf

variable "environment" {
  description = "Deployment environment: dev, staging, or prod"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "location" {
  description = "Primary Azure region for most resources"
  type        = string
  default     = "East US 2"
}

variable "synapse_location" {
  description = "Azure region for Synapse Workspace (can differ from primary location)"
  type        = string
  default     = "Central India"
}

variable "synapse_sql_admin_login" {
  description = "Synapse SQL administrator username"
  type        = string
  default     = "sqladminuser"
}

variable "synapse_sql_admin_password" {
  description = "Synapse SQL administrator password"
  type        = string
  sensitive   = true
}

variable "eventhub_partition_count" {
  description = "Number of Event Hub partitions. Min 2 for Standard tier. Increase for higher throughput in prod."
  type        = number
  default     = 2

  validation {
    condition     = var.eventhub_partition_count >= 2 && var.eventhub_partition_count <= 32
    error_message = "Partition count must be between 2 and 32."
  }
}

variable "eventhub_message_retention_days" {
  description = "How many days Event Hub retains messages. 1 = cheapest for dev, 7 = safer for debugging."
  type        = number
  default     = 1

  validation {
    condition     = var.eventhub_message_retention_days >= 1 && var.eventhub_message_retention_days <= 7
    error_message = "Retention must be between 1 and 7 days on Standard tier."
  }
}
