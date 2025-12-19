#!/usr/bin/env python3
"""
Entrypoint for Dagster Cloud Agent on Google Cloud Run

This script:
1. Fetches secrets from Google Secret Manager
2. Sets environment variables for the agent
3. Starts the Dagster Cloud agent

Environment Variables Required:
- GCP_PROJECT_ID: GCP project ID
- GCP_REGION: GCP region (default: us-central1)
- SECRET_NAMES: Comma-separated list of "secret-name:ENV_VAR" mappings
  Example: "dagster-token:DAGSTER_CLOUD_API_TOKEN,org-id:DAGSTER_CLOUD_ORG_ID"

Optional:
- DAGSTER_CLOUD_DEPLOYMENT_NAME: Deployment name (can also come from secret)
- DAGSTER_CLOUD_ORG_ID: Organization ID (can also come from secret)
"""

import os
import sys
import logging
from typing import Dict

from google.cloud import secretmanager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_secret(client: secretmanager.SecretManagerServiceClient, project_id: str, secret_name: str) -> str:
    """
    Fetch a secret value from Google Secret Manager.

    Args:
        client: Secret Manager client
        project_id: GCP project ID
        secret_name: Name of the secret

    Returns:
        Secret value as string
    """
    try:
        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

        # Access the secret version
        response = client.access_secret_version(request={"name": name})

        # Return the decoded payload
        payload = response.payload.data.decode("UTF-8")
        logger.info(f"Successfully fetched secret: {secret_name}")
        return payload

    except Exception as e:
        logger.error(f"Failed to fetch secret {secret_name}: {e}")
        raise


def load_secrets_from_secret_manager() -> Dict[str, str]:
    """
    Load secrets from Google Secret Manager based on SECRET_NAMES env var.

    Returns:
        Dictionary of environment variable name to secret value
    """
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        logger.error("GCP_PROJECT_ID environment variable is required")
        sys.exit(1)

    secret_names = os.environ.get("SECRET_NAMES", "")
    if not secret_names:
        logger.warning("SECRET_NAMES not set, skipping secret loading")
        return {}

    logger.info(f"Loading secrets from Secret Manager in project {project_id}")

    # Initialize Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    secrets = {}

    # Parse SECRET_NAMES: "secret-name:ENV_VAR,another-secret:ANOTHER_VAR"
    for mapping in secret_names.split(","):
        mapping = mapping.strip()
        if not mapping:
            continue

        try:
            secret_name, env_var = mapping.split(":", 1)
            secret_name = secret_name.strip()
            env_var = env_var.strip()

            logger.info(f"Fetching secret '{secret_name}' for env var '{env_var}'")
            secret_value = fetch_secret(client, project_id, secret_name)
            secrets[env_var] = secret_value

        except ValueError:
            logger.error(f"Invalid SECRET_NAMES format: {mapping} (expected 'secret:ENV_VAR')")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to load secret {mapping}: {e}")
            sys.exit(1)

    logger.info(f"Successfully loaded {len(secrets)} secrets")
    return secrets


def validate_environment():
    """Validate that required environment variables are set."""
    required_vars = [
        "GCP_PROJECT_ID",
        "DAGSTER_CLOUD_API_TOKEN",
        "DAGSTER_CLOUD_ORG_ID",
    ]

    missing = []
    for var in required_vars:
        if not os.environ.get(var):
            missing.append(var)

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    logger.info("Environment validation passed")


def main():
    """Main entrypoint."""
    logger.info("Starting Dagster Cloud Agent for Google Cloud Run")

    # Load secrets from Secret Manager
    secrets = load_secrets_from_secret_manager()

    # Set environment variables from secrets
    for env_var, value in secrets.items():
        os.environ[env_var] = value
        logger.info(f"Set environment variable: {env_var}")

    # Validate environment
    validate_environment()

    # Log configuration
    logger.info(f"GCP Project: {os.environ.get('GCP_PROJECT_ID')}")
    logger.info(f"GCP Region: {os.environ.get('GCP_REGION', 'us-central1')}")
    logger.info(f"Dagster Organization: {os.environ.get('DAGSTER_CLOUD_ORG_ID')}")
    logger.info(f"Dagster Deployment: {os.environ.get('DAGSTER_CLOUD_DEPLOYMENT_NAME', 'prod')}")

    # Start the Dagster Cloud agent
    logger.info("Starting Dagster Cloud agent...")

    # Import and run the agent
    # The agent will use the dagster.yaml configuration
    from dagster_cloud.cli.agent import agent_run_command

    sys.exit(agent_run_command())


if __name__ == "__main__":
    main()
