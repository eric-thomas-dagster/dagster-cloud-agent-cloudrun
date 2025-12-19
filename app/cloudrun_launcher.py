"""
Dagster Cloud Agent Launchers for Google Cloud Run

This module provides custom launchers for Dagster Cloud hybrid agents running on Google Cloud Run:
- CloudRunUserCodeLauncher: Creates Cloud Run services for code servers (one per code location)
- CloudRunRunLauncher: Creates ephemeral Cloud Run jobs for executing individual Dagster runs

Architecture:
- Agent: Runs as a Cloud Run service (min instances = 1, always-on)
- Code Servers: Created as Cloud Run services (auto-scale, handle gRPC requests from agent)
- Run Workers: Created as Cloud Run jobs (ephemeral, one per Dagster run, auto-cleanup)

Benefits vs GKE:
- No cluster management overhead
- Auto-scales to zero (code servers when idle)
- Pay only for actual usage
- Faster cold starts than GKE pods
- Simpler networking (built-in ingress)
"""

import logging
import os
import time
from typing import Dict, Iterator, Optional, Sequence

from dagster import Field, IntSource, StringSource, _check as check
from dagster._core.launcher import RunLauncher
from dagster._core.launcher.base import CheckRunHealthResult, WorkerStatus
from dagster._core.storage.dagster_run import DagsterRun
from dagster._serdes import ConfigurableClass
from dagster_cloud.workspace.user_code_launcher import (
    DagsterCloudUserCodeLauncher,
    DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
)
from dagster_cloud.workspace.user_code_launcher.user_code_launcher import (
    ServerEndpoint,
)

from google.cloud import run_v2
from google.cloud.run_v2 import Service, Job, ExecutionTemplate, Container, ResourceRequirements
from google.api_core import exceptions as gcp_exceptions

logger = logging.getLogger("cloudrun_launcher")


class CloudRunUserCodeLauncher(DagsterCloudUserCodeLauncher, ConfigurableClass):
    """
    Launches Dagster code servers as Google Cloud Run services.

    Each code location gets its own Cloud Run service that:
    - Runs the dagster gRPC server
    - Auto-scales based on load (can scale to zero)
    - Has a stable HTTPS endpoint for the agent to connect to
    """

    def __init__(
        self,
        project_id: str,
        region: str,
        agent_service_account: Optional[str] = None,
        code_server_service_account: Optional[str] = None,
        cpu: str = "1",
        memory: str = "2Gi",
        max_instances: int = 10,
        min_instances: int = 0,
        timeout: int = 3600,
        inst_data: Optional[Dict] = None,
    ):
        self._project_id = project_id
        self._region = region
        self._agent_service_account = agent_service_account
        self._code_server_service_account = code_server_service_account or agent_service_account
        self._cpu = cpu
        self._memory = memory
        self._max_instances = max_instances
        self._min_instances = min_instances
        self._timeout = timeout

        # Initialize Cloud Run client
        self._client = run_v2.ServicesClient()

        super().__init__(inst_data=inst_data)

        logger.info(
            f"CloudRunUserCodeLauncher initialized: project={project_id}, "
            f"region={region}, cpu={cpu}, memory={memory}"
        )

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "project_id": Field(
                StringSource,
                description="GCP project ID where Cloud Run services will be created",
            ),
            "region": Field(
                StringSource,
                description="GCP region (e.g., 'us-central1')",
            ),
            "agent_service_account": Field(
                StringSource,
                is_required=False,
                description="Service account email for the agent (optional)",
            ),
            "code_server_service_account": Field(
                StringSource,
                is_required=False,
                description="Service account email for code servers (defaults to agent SA)",
            ),
            "cpu": Field(
                StringSource,
                is_required=False,
                default_value="1",
                description="CPU limit for code servers (e.g., '1', '2', '4')",
            ),
            "memory": Field(
                StringSource,
                is_required=False,
                default_value="2Gi",
                description="Memory limit for code servers (e.g., '512Mi', '2Gi')",
            ),
            "max_instances": Field(
                IntSource,
                is_required=False,
                default_value=10,
                description="Maximum number of instances for auto-scaling",
            ),
            "min_instances": Field(
                IntSource,
                is_required=False,
                default_value=0,
                description="Minimum instances (0 = scale to zero)",
            ),
            "timeout": Field(
                IntSource,
                is_required=False,
                default_value=3600,
                description="Request timeout in seconds",
            ),
        }

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    def _get_service_name(self, deployment_name: str, location_name: str) -> str:
        """Generate Cloud Run service name for a code location."""
        # Cloud Run service names must be lowercase, alphanumeric + hyphens, max 63 chars
        safe_deployment = deployment_name.lower().replace("_", "-")[:20]
        safe_location = location_name.lower().replace("_", "-")[:30]
        return f"dagster-{safe_deployment}-{safe_location}"

    def _get_parent(self) -> str:
        """Get the parent path for Cloud Run resources."""
        return f"projects/{self._project_id}/locations/{self._region}"

    def launch_server(
        self,
        deployment_name: str,
        location_name: str,
        image: str,
        container_context: Optional[Dict] = None,
    ) -> ServerEndpoint:
        """
        Launch a Cloud Run service for a code location.

        Args:
            deployment_name: Dagster deployment name
            location_name: Code location name
            image: Docker image to run
            container_context: Optional overrides for CPU, memory, env vars, etc.

        Returns:
            ServerEndpoint with host, port, and metadata
        """
        service_name = self._get_service_name(deployment_name, location_name)
        parent = self._get_parent()
        full_name = f"{parent}/services/{service_name}"

        logger.info(f"Launching Cloud Run service: {service_name}")

        # Parse container context overrides
        cpu = container_context.get("cpu", self._cpu) if container_context else self._cpu
        memory = container_context.get("memory", self._memory) if container_context else self._memory
        env_vars = container_context.get("env_vars", {}) if container_context else {}

        # Build environment variables
        env_list = [
            run_v2.EnvVar(name="DAGSTER_CURRENT_IMAGE", value=image),
        ]
        for key, value in env_vars.items():
            env_list.append(run_v2.EnvVar(name=key, value=str(value)))

        # Create Cloud Run service configuration
        container = run_v2.Container(
            image=image,
            ports=[run_v2.ContainerPort(container_port=4000)],
            env=env_list,
            resources=run_v2.ResourceRequirements(
                limits={"cpu": cpu, "memory": memory},
            ),
        )

        template = run_v2.RevisionTemplate(
            containers=[container],
            service_account=self._code_server_service_account or "",
            timeout=f"{self._timeout}s",
            max_instance_request_concurrency=80,
        )

        service = run_v2.Service(
            template=template,
            ingress=run_v2.IngressTraffic.INGRESS_TRAFFIC_ALL,  # Allow external traffic
            labels={
                "dagster-deployment": deployment_name,
                "dagster-location": location_name,
                "managed-by": "dagster-cloud-agent",
            },
        )

        # Scaling configuration
        if self._max_instances or self._min_instances:
            service.scaling = run_v2.ServiceScaling(
                min_instance_count=self._min_instances,
                max_instance_count=self._max_instances,
            )

        try:
            # Check if service already exists
            try:
                existing = self._client.get_service(name=full_name)
                logger.info(f"Updating existing service: {service_name}")
                operation = self._client.update_service(service=service, update_mask=None)
            except gcp_exceptions.NotFound:
                logger.info(f"Creating new service: {service_name}")
                request = run_v2.CreateServiceRequest(
                    parent=parent,
                    service=service,
                    service_id=service_name,
                )
                operation = self._client.create_service(request=request)

            # Wait for operation to complete
            result = operation.result(timeout=300)

            # Get the service URL
            service_url = result.uri
            logger.info(f"Service ready: {service_url}")

            # Cloud Run URLs are HTTPS and use port 443
            # But Dagster expects host:port, and the gRPC client needs to know it's HTTPS
            # We'll return the host without https:// and port 443
            host = service_url.replace("https://", "")

            return ServerEndpoint(
                host=host,
                port=443,
                socket=None,
                metadata={
                    "service_name": service_name,
                    "service_url": service_url,
                    "region": self._region,
                    "project": self._project_id,
                },
            )

        except Exception as e:
            logger.error(f"Failed to launch Cloud Run service {service_name}: {e}")
            raise

    def remove_server(
        self,
        deployment_name: str,
        location_name: str,
    ):
        """Remove a Cloud Run service for a code location."""
        service_name = self._get_service_name(deployment_name, location_name)
        full_name = f"{self._get_parent()}/services/{service_name}"

        logger.info(f"Removing Cloud Run service: {service_name}")

        try:
            operation = self._client.delete_service(name=full_name)
            operation.result(timeout=180)
            logger.info(f"Service deleted: {service_name}")
        except gcp_exceptions.NotFound:
            logger.warning(f"Service not found (already deleted?): {service_name}")
        except Exception as e:
            logger.error(f"Failed to delete service {service_name}: {e}")
            raise

    def get_server_handle_status(
        self,
        deployment_name: str,
        location_name: str,
    ) -> str:
        """Get the status of a Cloud Run service."""
        service_name = self._get_service_name(deployment_name, location_name)
        full_name = f"{self._get_parent()}/services/{service_name}"

        try:
            service = self._client.get_service(name=full_name)

            # Check if service is ready
            for condition in service.conditions:
                if condition.type == "Ready":
                    if condition.state == run_v2.Condition.State.CONDITION_SUCCEEDED:
                        return "RUNNING"
                    else:
                        return "PENDING"

            return "UNKNOWN"

        except gcp_exceptions.NotFound:
            return "NOT_FOUND"
        except Exception as e:
            logger.error(f"Error checking service status {service_name}: {e}")
            return "ERROR"


class CloudRunRunLauncher(RunLauncher, ConfigurableClass):
    """
    Launches Dagster runs as Google Cloud Run jobs.

    Each run executes in an ephemeral Cloud Run job that:
    - Executes the run using the code location's image
    - Streams events back to Dagster Cloud
    - Auto-deletes after completion (configurable retention)
    - Scales to zero cost when not running
    """

    def __init__(
        self,
        project_id: str,
        region: str,
        service_account: Optional[str] = None,
        cpu: str = "1",
        memory: str = "2Gi",
        timeout: int = 3600,
        inst_data: Optional[Dict] = None,
    ):
        self._project_id = project_id
        self._region = region
        self._service_account = service_account
        self._cpu = cpu
        self._memory = memory
        self._timeout = timeout
        self._inst_data = check.opt_dict_param(inst_data, "inst_data", key_type=str)

        # Initialize Cloud Run Jobs client
        self._client = run_v2.JobsClient()
        self._executions_client = run_v2.ExecutionsClient()

        super().__init__()

        logger.info(
            f"CloudRunRunLauncher initialized: project={project_id}, "
            f"region={region}, cpu={cpu}, memory={memory}"
        )

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "project_id": Field(
                StringSource,
                description="GCP project ID where Cloud Run jobs will be created",
            ),
            "region": Field(
                StringSource,
                description="GCP region (e.g., 'us-central1')",
            ),
            "service_account": Field(
                StringSource,
                is_required=False,
                description="Service account email for run jobs",
            ),
            "cpu": Field(
                StringSource,
                is_required=False,
                default_value="1",
                description="CPU limit for run workers",
            ),
            "memory": Field(
                StringSource,
                is_required=False,
                default_value="2Gi",
                description="Memory limit for run workers",
            ),
            "timeout": Field(
                IntSource,
                is_required=False,
                default_value=3600,
                description="Job timeout in seconds",
            ),
        }

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    def _get_job_name(self, run_id: str) -> str:
        """Generate Cloud Run job name for a run."""
        # Cloud Run job names: lowercase, alphanumeric + hyphens, max 63 chars
        # Use run- prefix + first 57 chars of run_id
        return f"run-{run_id[:57]}"

    def _get_parent(self) -> str:
        """Get the parent path for Cloud Run resources."""
        return f"projects/{self._project_id}/locations/{self._region}"

    def launch_run(self, context) -> None:
        """
        Launch a Cloud Run job to execute a Dagster run.

        Creates an ephemeral Cloud Run job that:
        1. Uses the code location's image
        2. Executes: dagster api execute_run
        3. Streams events to Dagster Cloud
        4. Auto-cleans up after completion
        """
        run = context.dagster_run
        run_id = run.run_id

        job_name = self._get_job_name(run_id)
        parent = self._get_parent()

        logger.info(f"Launching Cloud Run job for run {run_id}: {job_name}")

        # Get the image from the job origin
        job_origin = check.not_none(context.job_code_origin)
        repository_origin = job_origin.repository_origin
        image = repository_origin.container_image

        if not image:
            raise Exception(f"No container image found for run {run_id}")

        # Build the command for executing the run
        from dagster._core.execution.api import ExecuteRunArgs

        # Strip container context to reduce payload size
        stripped_repository_origin = repository_origin._replace(container_context={})

        # Fix entry_point to use executable_path with python -m dagster
        executable_path = repository_origin.executable_path
        if executable_path:
            fixed_entry_point = [executable_path, "-m", "dagster"]
            stripped_repository_origin = stripped_repository_origin._replace(entry_point=fixed_entry_point)
            logger.info(f"Using entry point: {fixed_entry_point}")

        stripped_job_origin = job_origin._replace(repository_origin=stripped_repository_origin)

        # Create ExecuteRunArgs with full instance ref
        args = ExecuteRunArgs(
            job_origin=stripped_job_origin,
            run_id=run_id,
            instance_ref=self._instance.get_ref(),
        )

        # Use get_command_args() to build the command (Dagster OSS pattern)
        command = args.get_command_args()

        logger.info(f"Run {run_id} command: {command}")

        # Create Cloud Run job configuration
        container = run_v2.Container(
            image=image,
            command=command,
            resources=run_v2.ResourceRequirements(
                limits={"cpu": self._cpu, "memory": self._memory},
            ),
        )

        template = run_v2.ExecutionTemplate(
            template=run_v2.TaskTemplate(
                containers=[container],
                service_account=self._service_account or "",
                max_retries=0,  # Don't retry failed runs
                timeout=f"{self._timeout}s",
            ),
        )

        job = run_v2.Job(
            template=template,
            labels={
                "dagster-run-id": run_id,
                "managed-by": "dagster-cloud-agent",
            },
        )

        try:
            # Create and execute the job
            request = run_v2.CreateJobRequest(
                parent=parent,
                job=job,
                job_id=job_name,
            )

            operation = self._client.create_job(request=request)
            created_job = operation.result(timeout=60)

            logger.info(f"Job created: {created_job.name}")

            # Execute the job
            exec_request = run_v2.RunJobRequest(name=created_job.name)
            exec_operation = self._client.run_job(request=exec_request)

            logger.info(f"Job execution started for run {run_id}")

        except Exception as e:
            logger.error(f"Failed to launch Cloud Run job for run {run_id}: {e}")
            raise

    def terminate(self, run_id: str) -> bool:
        """
        Terminate a running Cloud Run job.

        Returns True if termination was successful, False otherwise.
        """
        job_name = self._get_job_name(run_id)
        full_name = f"{self._get_parent()}/jobs/{job_name}"

        logger.info(f"Terminating Cloud Run job: {job_name}")

        try:
            # Delete the job (will terminate any running executions)
            operation = self._client.delete_job(name=full_name)
            operation.result(timeout=60)
            logger.info(f"Job terminated: {job_name}")
            return True

        except gcp_exceptions.NotFound:
            logger.warning(f"Job not found: {job_name}")
            return False
        except Exception as e:
            logger.error(f"Failed to terminate job {job_name}: {e}")
            return False

    def get_run_worker_debug_info(self, run_id: str) -> Optional[Dict]:
        """Get debug information about a run's Cloud Run job."""
        job_name = self._get_job_name(run_id)
        full_name = f"{self._get_parent()}/jobs/{job_name}"

        try:
            job = self._client.get_job(name=full_name)

            debug_info = {
                "job_name": job_name,
                "project": self._project_id,
                "region": self._region,
                "status": str(job.conditions[0].state) if job.conditions else "UNKNOWN",
                "console_url": f"https://console.cloud.google.com/run/jobs/details/{self._region}/{job_name}?project={self._project_id}",
            }

            return debug_info

        except gcp_exceptions.NotFound:
            return {"job_name": job_name, "status": "NOT_FOUND"}
        except Exception as e:
            logger.error(f"Error getting debug info for {job_name}: {e}")
            return {"job_name": job_name, "error": str(e)}

    def check_run_worker_health(self, run_id: str) -> CheckRunHealthResult:
        """Check the health of a run's Cloud Run job."""
        job_name = self._get_job_name(run_id)
        full_name = f"{self._get_parent()}/jobs/{job_name}"

        try:
            job = self._client.get_job(name=full_name)

            # Check job conditions
            for condition in job.conditions:
                if condition.type == "Ready":
                    if condition.state == run_v2.Condition.State.CONDITION_SUCCEEDED:
                        return CheckRunHealthResult(WorkerStatus.RUNNING)
                    elif condition.state == run_v2.Condition.State.CONDITION_FAILED:
                        return CheckRunHealthResult(WorkerStatus.FAILED, msg=condition.message)

            return CheckRunHealthResult(WorkerStatus.RUNNING)

        except gcp_exceptions.NotFound:
            return CheckRunHealthResult(WorkerStatus.NOT_FOUND)
        except Exception as e:
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, msg=str(e))
