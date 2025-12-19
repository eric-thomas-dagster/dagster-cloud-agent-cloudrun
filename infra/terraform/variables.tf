variable "project_id" {
  description = "GCP project ID where resources will be created"
  type        = string
}

variable "region" {
  description = "GCP region for Cloud Run services"
  type        = string
  default     = "us-central1"
}

variable "agent_service_name" {
  description = "Name of the Cloud Run service for the agent"
  type        = string
  default     = "dagster-agent"
}

variable "agent_service_account_name" {
  description = "Name of the service account for the agent"
  type        = string
  default     = "dagster-agent"
}

variable "workloads_service_account_name" {
  description = "Name of the service account for code servers and run jobs"
  type        = string
  default     = "dagster-workloads"
}

variable "agent_image" {
  description = "Docker image for the Dagster agent (e.g., ghcr.io/username/dagster-cloudrun-agent:latest)"
  type        = string
}

variable "agent_cpu" {
  description = "CPU allocation for the agent"
  type        = string
  default     = "1"
}

variable "agent_memory" {
  description = "Memory allocation for the agent"
  type        = string
  default     = "1Gi"
}

variable "dagster_cloud_api_token" {
  description = "Dagster Cloud API token (sensitive)"
  type        = string
  sensitive   = true
}

variable "dagster_cloud_org_id" {
  description = "Dagster Cloud organization ID"
  type        = string
}

variable "dagster_cloud_deployment_name" {
  description = "Dagster Cloud deployment name"
  type        = string
  default     = "prod"
}

variable "dagster_token_secret_name" {
  description = "Name of the Secret Manager secret for the Dagster token"
  type        = string
  default     = "dagster-cloud-api-token"
}

variable "dagster_org_id_secret_name" {
  description = "Name of the Secret Manager secret for the org ID"
  type        = string
  default     = "dagster-cloud-org-id"
}

variable "dagster_deployment_secret_name" {
  description = "Name of the Secret Manager secret for the deployment name"
  type        = string
  default     = "dagster-cloud-deployment"
}

variable "enable_artifact_registry_access" {
  description = "Grant workloads service account access to Artifact Registry (for pulling private images)"
  type        = bool
  default     = false
}
