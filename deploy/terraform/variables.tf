variable "project" {
  description = "Project slug used to name resources."
  type        = string
  default     = "cdc-streaming-pipeline"
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "subnet_ids" {
  description = "Private subnet IDs for the MSK Serverless cluster."
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "Security group IDs for the MSK Serverless cluster."
  type        = list(string)
  default     = []
}
