output "agent_service_name" {
  description = "Name of the Cloud Run service for the agent"
  value       = google_cloud_run_v2_service.dagster_agent.name
}

output "agent_service_url" {
  description = "URL of the Cloud Run agent service"
  value       = google_cloud_run_v2_service.dagster_agent.uri
}

output "agent_service_account_email" {
  description = "Email of the agent service account"
  value       = google_service_account.dagster_agent.email
}

output "workloads_service_account_email" {
  description = "Email of the workloads service account"
  value       = google_service_account.dagster_workloads.email
}

output "project_id" {
  description = "GCP project ID"
  value       = var.project_id
}

output "region" {
  description = "GCP region"
  value       = var.region
}

output "console_url" {
  description = "Google Cloud Console URL for the agent service"
  value       = "https://console.cloud.google.com/run/detail/${var.region}/${google_cloud_run_v2_service.dagster_agent.name}/metrics?project=${var.project_id}"
}
