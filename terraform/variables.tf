variable "project_id" {
  description = "The Google Cloud Project ID where infrastructure will be provisioned."
  type        = string
}

variable "region" {
  description = "Default GCP region for resources (e.g., us-central1)."
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment identifier (dev, staging, prod)."
  type        = string
  default     = "prod"
}
