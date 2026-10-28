variable "aws_region" {
  type        = string
  description = "AWS region for the dev foundation."
  default     = "us-east-1"
}

variable "aws_account_id" {
  type        = string
  description = "Explicit dev account guardrail; supplied at runtime, never inferred silently."
  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "Supply a 12-digit dev AWS account ID."
  }
}

variable "budget_email" {
  type        = string
  sensitive   = true
  description = "Budget notification recipient; supply with TF_VAR_budget_email."
  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.budget_email))
    error_message = "Supply a valid budget notification email."
  }
}

variable "monthly_budget_usd" {
  type        = number
  default     = 5
  description = "Account-wide monthly cost alert threshold in USD; alerts do not cap spending."
  validation {
    condition     = var.monthly_budget_usd > 0 && var.monthly_budget_usd <= 10
    error_message = "Keep the demo budget above zero and at most 10 USD."
  }
}

variable "name_prefix" {
  type        = string
  default     = "paysim-dev"
  description = "Short lowercase resource prefix; account and region disambiguate buckets."
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,19}$", var.name_prefix))
    error_message = "Use 3–20 lowercase letters, digits, or hyphens, starting with a letter."
  }
}

locals {
  tags = {
    Project     = "PaySim"
    Environment = "dev"
    ManagedBy   = "Terraform"
    Task        = "T1"
  }
  catalog_name = replace(var.name_prefix, "-", "_")
}
