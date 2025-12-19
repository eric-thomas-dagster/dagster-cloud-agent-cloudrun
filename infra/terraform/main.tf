terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Service account for the Dagster agent
resource "google_service_account" "dagster_agent" {
  account_id   = var.agent_service_account_name
  display_name = "Dagster Cloud Agent"
  description  = "Service account for Dagster Cloud hybrid agent on Cloud Run"
}

# Service account for code servers and run jobs
resource "google_service_account" "dagster_workloads" {
  account_id   = var.workloads_service_account_name
  display_name = "Dagster Workloads"
  description  = "Service account for Dagster code servers and run jobs"
}

# Grant agent SA permissions to create/manage Cloud Run services and jobs
resource "google_project_iam_member" "agent_run_admin" {
  project = var.project_id
  role    = "roles/run.admin"
  member  = "serviceAccount:${google_service_account.dagster_agent.email}"
}

# Grant agent SA permissions to act as the workloads service account
resource "google_service_account_iam_member" "agent_sa_user" {
  service_account_id = google_service_account.dagster_workloads.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.dagster_agent.email}"
}

# Grant agent SA permissions to read secrets
resource "google_secret_manager_secret_iam_member" "agent_secret_accessor" {
  for_each = toset([
    google_secret_manager_secret.dagster_token.id,
    google_secret_manager_secret.dagster_org_id.id,
    google_secret_manager_secret.dagster_deployment.id,
  ])

  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dagster_agent.email}"
}

# Grant workloads SA permissions to pull images (if using GCR/Artifact Registry)
resource "google_project_iam_member" "workloads_artifact_reader" {
  count   = var.enable_artifact_registry_access ? 1 : 0
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.dagster_workloads.email}"
}

# Secret for Dagster Cloud API token
resource "google_secret_manager_secret" "dagster_token" {
  secret_id = var.dagster_token_secret_name

  replication {
    auto {}
  }

  labels = {
    managed-by = "terraform"
    purpose    = "dagster-agent"
  }
}

resource "google_secret_manager_secret_version" "dagster_token" {
  secret = google_secret_manager_secret.dagster_token.id

  secret_data = var.dagster_cloud_api_token
}

# Secret for Dagster organization ID
resource "google_secret_manager_secret" "dagster_org_id" {
  secret_id = var.dagster_org_id_secret_name

  replication {
    auto {}
  }

  labels = {
    managed-by = "terraform"
    purpose    = "dagster-agent"
  }
}

resource "google_secret_manager_secret_version" "dagster_org_id" {
  secret = google_secret_manager_secret.dagster_org_id.id

  secret_data = var.dagster_cloud_org_id
}

# Secret for Dagster deployment name
resource "google_secret_manager_secret" "dagster_deployment" {
  secret_id = var.dagster_deployment_secret_name

  replication {
    auto {}
  }

  labels = {
    managed-by = "terraform"
    purpose    = "dagster-agent"
  }
}

resource "google_secret_manager_secret_version" "dagster_deployment" {
  secret = google_secret_manager_secret.dagster_deployment.id

  secret_data = var.dagster_cloud_deployment_name
}

# Cloud Run service for the agent
resource "google_cloud_run_v2_service" "dagster_agent" {
  name     = var.agent_service_name
  location = var.region

  template {
    service_account = google_service_account.dagster_agent.email

    scaling {
      min_instance_count = 1  # Always keep agent running
      max_instance_count = 1  # Only need one agent instance
    }

    timeout = "3600s"

    containers {
      image = var.agent_image

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "GCP_REGION"
        value = var.region
      }

      env {
        name  = "SECRET_NAMES"
        value = "${var.dagster_token_secret_name}:DAGSTER_CLOUD_API_TOKEN,${var.dagster_org_id_secret_name}:DAGSTER_CLOUD_ORG_ID,${var.dagster_deployment_secret_name}:DAGSTER_CLOUD_DEPLOYMENT_NAME"
      }

      env {
        name  = "CODE_SERVER_SERVICE_ACCOUNT"
        value = google_service_account.dagster_workloads.email
      }

      env {
        name  = "RUN_SERVICE_ACCOUNT"
        value = google_service_account.dagster_workloads.email
      }

      resources {
        limits = {
          cpu    = var.agent_cpu
          memory = var.agent_memory
        }
      }
    }
  }

  labels = {
    managed-by = "terraform"
    purpose    = "dagster-agent"
  }

  depends_on = [
    google_project_iam_member.agent_run_admin,
    google_service_account_iam_member.agent_sa_user,
    google_secret_manager_secret_version.dagster_token,
    google_secret_manager_secret_version.dagster_org_id,
    google_secret_manager_secret_version.dagster_deployment,
  ]
}

# Note: The agent service doesn't need public access
# It only needs to call Google Cloud APIs
# Code servers will have public ingress enabled by the agent
