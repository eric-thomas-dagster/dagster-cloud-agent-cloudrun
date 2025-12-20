"""
Dagster Cloud Agent Launchers for Google Cloud Run

This module provides custom launchers for Dagster Cloud hybrid agents running on Google Cloud Run:
- CloudRunUserCodeLauncher: Creates Cloud Run services for code servers (one per code location)
- CloudRunRunLauncher: Creates ephemeral Cloud Run jobs for executing individual Dagster runs

Architecture:
- Agent: Runs as a Cloud Run service (min instances = 1, always-on)
- Code Servers: Created as Cloud Run services (auto-scale, handle gRPC requests from agent)
- Run Workers: Created as Cloud Run jobs (ephemeral, one per Dagster run, auto-cleanup)

This implementation mirrors the AWS ECS agent with feature parity including:
- Health checks, sidecar containers, volume mounts
- Per-location resource overrides via container_context
- VPC networking, service accounts, secret management
- Comprehensive debugging and monitoring
"""

import logging
import os
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence

from dagster import Field, IntSource, StringSource, _check as check, Array, Noneable, ScalarUnion
from dagster._core.launcher import RunLauncher
from dagster._core.launcher.base import CheckRunHealthResult, WorkerStatus
from dagster._core.storage.dagster_run import DagsterRun
from dagster._serdes import ConfigurableClass
from dagster._utils.merger import merge_dicts
from dagster_cloud.workspace.user_code_launcher import (
    DagsterCloudUserCodeLauncher,
    DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
)
from dagster_cloud.workspace.user_code_launcher.user_code_launcher import (
    ServerEndpoint,
    UserCodeLauncherEntry,
)

from google.cloud import run_v2, secretmanager, logging as cloud_logging
from google.api_core import exceptions as gcp_exceptions
from google.api_core import retry

logger = logging.getLogger("cloudrun_launcher")

# Constants
CONTAINER_NAME = "dagster"
PORT = 4000
DEFAULT_TIMEOUT = 300  # 5 minutes
DEFAULT_GRACE_PERIOD = 60  # 1 minute


class CloudRunContainerContext:
    """
    Container context for Cloud Run, similar to EcsContainerContext.
    Holds configuration that can be overridden per code location.
    """

    def __init__(
        self,
        secrets: Optional[Sequence[Mapping[str, str]]] = None,
        secrets_tags: Optional[Sequence[str]] = None,
        env_vars: Optional[Sequence[str]] = None,
        server_resources: Optional[Mapping[str, Any]] = None,
        run_resources: Optional[Mapping[str, Any]] = None,
        service_account: Optional[str] = None,
        server_sidecar_containers: Optional[Sequence[Mapping[str, Any]]] = None,
        run_sidecar_containers: Optional[Sequence[Mapping[str, Any]]] = None,
        server_labels: Optional[Sequence[Mapping[str, Optional[str]]]] = None,
        run_labels: Optional[Sequence[Mapping[str, Optional[str]]]] = None,
        server_health_check: Optional[Mapping[str, Any]] = None,
        server_startup_probe: Optional[Mapping[str, Any]] = None,
        volumes: Optional[Sequence[Mapping[str, Any]]] = None,
        volume_mounts: Optional[Sequence[Mapping[str, Any]]] = None,
        vpc_connector: Optional[str] = None,
        vpc_egress: Optional[str] = None,
        repository_credentials: Optional[str] = None,
    ):
        self.secrets = list(secrets) if secrets else []
        self.secrets_tags = list(secrets_tags) if secrets_tags else []
        self.env_vars = list(env_vars) if env_vars else []
        self.server_resources = dict(server_resources) if server_resources else {}
        self.run_resources = dict(run_resources) if run_resources else {}
        self.service_account = service_account
        self.server_sidecar_containers = (
            list(server_sidecar_containers) if server_sidecar_containers else []
        )
        self.run_sidecar_containers = list(run_sidecar_containers) if run_sidecar_containers else []
        self.server_labels = list(server_labels) if server_labels else []
        self.run_labels = list(run_labels) if run_labels else []
        self.server_health_check = dict(server_health_check) if server_health_check else {}
        self.server_startup_probe = dict(server_startup_probe) if server_startup_probe else {}
        self.volumes = list(volumes) if volumes else []
        self.volume_mounts = list(volume_mounts) if volume_mounts else []
        self.vpc_connector = vpc_connector
        self.vpc_egress = vpc_egress
        self.repository_credentials = repository_credentials

    @staticmethod
    def create_from_config(config: Mapping[str, Any]) -> "CloudRunContainerContext":
        """Create container context from code location config."""
        run_config = config.get("run", {})
        return CloudRunContainerContext(
            secrets=run_config.get("secrets"),
            secrets_tags=run_config.get("secrets_tags"),
            env_vars=run_config.get("env_vars"),
            server_resources=run_config.get("server_resources"),
            run_resources=run_config.get("run_resources"),
            service_account=run_config.get("service_account"),
            server_sidecar_containers=run_config.get("server_sidecar_containers"),
            run_sidecar_containers=run_config.get("run_sidecar_containers"),
            server_labels=run_config.get("server_labels"),
            run_labels=run_config.get("run_labels"),
            server_health_check=run_config.get("server_health_check"),
            server_startup_probe=run_config.get("server_startup_probe"),
            volumes=run_config.get("volumes"),
            volume_mounts=run_config.get("volume_mounts"),
            vpc_connector=run_config.get("vpc_connector"),
            vpc_egress=run_config.get("vpc_egress"),
            repository_credentials=run_config.get("repository_credentials"),
        )

    def merge(self, other: "CloudRunContainerContext") -> "CloudRunContainerContext":
        """Merge two container contexts, with other taking precedence."""
        return CloudRunContainerContext(
            secrets=other.secrets or self.secrets,
            secrets_tags=other.secrets_tags or self.secrets_tags,
            env_vars=self.env_vars + other.env_vars,
            server_resources=merge_dicts(self.server_resources, other.server_resources),
            run_resources=merge_dicts(self.run_resources, other.run_resources),
            service_account=other.service_account or self.service_account,
            server_sidecar_containers=other.server_sidecar_containers or self.server_sidecar_containers,
            run_sidecar_containers=other.run_sidecar_containers or self.run_sidecar_containers,
            server_labels=self.server_labels + other.server_labels,
            run_labels=self.run_labels + other.run_labels,
            server_health_check=other.server_health_check or self.server_health_check,
            server_startup_probe=other.server_startup_probe or self.server_startup_probe,
            volumes=other.volumes or self.volumes,
            volume_mounts=other.volume_mounts or self.volume_mounts,
            vpc_connector=other.vpc_connector or self.vpc_connector,
            vpc_egress=other.vpc_egress or self.vpc_egress,
            repository_credentials=other.repository_credentials or self.repository_credentials,
        )

    def get_environment_dict(self) -> Dict[str, str]:
        """Parse env_vars into a dictionary."""
        env_dict = {}
        for env_var in self.env_vars:
            if "=" in env_var:
                key, value = env_var.split("=", 1)
                env_dict[key] = value
            else:
                # Pull from current process environment
                env_dict[env_var] = os.environ.get(env_var, "")
        return env_dict

    def get_secrets_dict(self, secrets_client: secretmanager.SecretManagerServiceClient, project_id: str) -> Dict[str, str]:
        """Fetch secrets from Secret Manager and return as dict."""
        secrets_dict = {}

        # Handle explicit secrets
        for secret in self.secrets:
            if isinstance(secret, dict):
                name = secret.get("name")
                value_from = secret.get("valueFrom")  # Secret Manager resource name
                if name and value_from:
                    try:
                        response = secrets_client.access_secret_version(request={"name": value_from})
                        secrets_dict[name] = response.payload.data.decode("UTF-8")
                    except Exception as e:
                        logger.error(f"Failed to fetch secret {name} from {value_from}: {e}")

        # Handle secrets by tag
        for tag in self.secrets_tags:
            try:
                # List secrets with this tag
                parent = f"projects/{project_id}"
                for secret in secrets_client.list_secrets(request={"parent": parent}):
                    if tag in secret.labels.values():
                        secret_name = secret.name.split("/")[-1]
                        version_name = f"{secret.name}/versions/latest"
                        response = secrets_client.access_secret_version(request={"name": version_name})
                        secrets_dict[secret_name] = response.payload.data.decode("UTF-8")
            except Exception as e:
                logger.error(f"Failed to fetch secrets by tag {tag}: {e}")

        return secrets_dict


class CloudRunUserCodeLauncher(DagsterCloudUserCodeLauncher, ConfigurableClass):
    """
    Launches Dagster code servers as Google Cloud Run services.

    Mirrors AWS ECS agent with full feature parity including:
    - Health checks and startup probes
    - Sidecar containers
    - Volume mounts (GCS FUSE, NFS, secrets, config maps)
    - VPC networking
    - Per-location resource overrides
    - Comprehensive monitoring and debugging
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
        server_process_startup_timeout: int = DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
        run_timeout: int = DEFAULT_TIMEOUT,
        run_grace_period: int = DEFAULT_GRACE_PERIOD,
        inst_data: Optional[Dict] = None,
        secrets: Optional[Sequence[Any]] = None,
        secrets_tags: Optional[Sequence[str]] = None,
        env_vars: Optional[Sequence[str]] = None,
        server_resources: Optional[Mapping[str, Any]] = None,
        run_resources: Optional[Mapping[str, Any]] = None,
        server_sidecar_containers: Optional[Sequence[Mapping[str, Any]]] = None,
        run_sidecar_containers: Optional[Sequence[Mapping[str, Any]]] = None,
        server_labels: Optional[Sequence[Mapping[str, Optional[str]]]] = None,
        run_labels: Optional[Sequence[Mapping[str, Optional[str]]]] = None,
        server_health_check: Optional[Mapping[str, Any]] = None,
        server_startup_probe: Optional[Mapping[str, Any]] = None,
        volumes: Optional[Sequence[Mapping[str, Any]]] = None,
        volume_mounts: Optional[Sequence[Mapping[str, Any]]] = None,
        vpc_connector: Optional[str] = None,
        vpc_egress: Optional[str] = None,
        enable_debug_logs: bool = True,
        **kwargs,
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
        self._server_process_startup_timeout = server_process_startup_timeout
        self._run_timeout = run_timeout
        self._run_grace_period = run_grace_period
        self._enable_debug_logs = enable_debug_logs

        # Initialize clients
        self._services_client = run_v2.ServicesClient()
        self._jobs_client = run_v2.JobsClient()
        self._secrets_client = secretmanager.SecretManagerServiceClient()

        # Container context configuration
        self._base_container_context = CloudRunContainerContext(
            secrets=secrets,
            secrets_tags=secrets_tags,
            env_vars=env_vars,
            server_resources=server_resources,
            run_resources=run_resources,
            service_account=code_server_service_account,
            server_sidecar_containers=server_sidecar_containers,
            run_sidecar_containers=run_sidecar_containers,
            server_labels=server_labels,
            run_labels=run_labels,
            server_health_check=server_health_check,
            server_startup_probe=server_startup_probe,
            volumes=volumes,
            volume_mounts=volume_mounts,
            vpc_connector=vpc_connector,
            vpc_egress=vpc_egress,
        )

        super().__init__(inst_data=inst_data, **kwargs)

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
                description="Service account email for the agent",
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
            "server_process_startup_timeout": Field(
                IntSource,
                is_required=False,
                default_value=DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
                description="Timeout when waiting for code server to be ready",
            ),
            "secrets": Field(
                Array(
                    ScalarUnion(
                        scalar_type=str,
                        non_scalar_schema={"name": StringSource, "valueFrom": StringSource},
                    )
                ),
                is_required=False,
                description="Array of Google Secret Manager secrets to mount as environment variables",
            ),
            "secrets_tags": Field(
                Array(StringSource),
                is_required=False,
                description="Secret Manager secrets with these labels will be mounted as environment variables",
            ),
            "env_vars": Field(
                Array(StringSource),
                is_required=False,
                description="List of environment variables (KEY=VALUE or KEY to inherit from agent)",
            ),
            "server_resources": Field(
                dict,
                is_required=False,
                description="Default resources for code servers (cpu, memory, replica_count)",
            ),
            "run_resources": Field(
                dict,
                is_required=False,
                description="Default resources for run workers (cpu, memory)",
            ),
            "server_sidecar_containers": Field(
                Array(dict),
                is_required=False,
                description="Sidecar containers to run alongside code servers",
            ),
            "run_sidecar_containers": Field(
                Array(dict),
                is_required=False,
                description="Sidecar containers to run alongside run workers",
            ),
            "server_labels": Field(
                Array(dict),
                is_required=False,
                description="Labels to apply to code server services",
            ),
            "run_labels": Field(
                Array(dict),
                is_required=False,
                description="Labels to apply to run worker jobs",
            ),
            "server_health_check": Field(
                dict,
                is_required=False,
                description="Health check configuration for code servers",
            ),
            "server_startup_probe": Field(
                dict,
                is_required=False,
                description="Startup probe configuration for code servers",
            ),
            "volumes": Field(
                Array(dict),
                is_required=False,
                description="Volumes to mount (GCS FUSE, NFS, secrets, config maps)",
            ),
            "volume_mounts": Field(
                Array(dict),
                is_required=False,
                description="Volume mount configurations",
            ),
            "vpc_connector": Field(
                StringSource,
                is_required=False,
                description="VPC connector for private networking",
            ),
            "vpc_egress": Field(
                StringSource,
                is_required=False,
                description="VPC egress setting (ALL_TRAFFIC or PRIVATE_RANGES_ONLY)",
            ),
            "enable_debug_logs": Field(
                bool,
                is_required=False,
                default_value=True,
                description="Enable detailed debug logging",
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

    def _build_container_spec(
        self,
        image: str,
        container_context: CloudRunContainerContext,
        env_vars: Dict[str, str],
        is_server: bool = True,
    ) -> run_v2.Container:
        """Build a Cloud Run container specification."""
        # Merge environment variables
        all_env = merge_dicts(container_context.get_environment_dict(), env_vars)

        # Add secrets
        secrets_dict = container_context.get_secrets_dict(self._secrets_client, self._project_id)
        all_env = merge_dicts(all_env, secrets_dict)

        # Convert to Cloud Run env vars
        env_list = [run_v2.EnvVar(name=k, value=str(v)) for k, v in all_env.items()]

        # Get resources
        resources_config = container_context.server_resources if is_server else container_context.run_resources
        cpu = resources_config.get("cpu", self._cpu if is_server else "1")
        memory = resources_config.get("memory", self._memory if is_server else "2Gi")

        # Build container
        container = run_v2.Container(
            image=image,
            ports=[run_v2.ContainerPort(container_port=PORT)],
            env=env_list,
            resources=run_v2.ResourceRequirements(
                limits={"cpu": cpu, "memory": memory},
            ),
        )

        # Add volume mounts
        if container_context.volume_mounts:
            container.volume_mounts = [
                run_v2.VolumeMount(
                    name=vm["name"],
                    mount_path=vm["mountPath"],
                )
                for vm in container_context.volume_mounts
            ]

        # Add startup probe
        if is_server and container_context.server_startup_probe:
            probe_config = container_context.server_startup_probe
            container.startup_probe = run_v2.Probe(
                initial_delay_seconds=probe_config.get("initialDelaySeconds", 0),
                timeout_seconds=probe_config.get("timeoutSeconds", 1),
                period_seconds=probe_config.get("periodSeconds", 10),
                failure_threshold=probe_config.get("failureThreshold", 3),
                http_get=run_v2.HTTPGetAction(
                    path=probe_config.get("path", "/health"),
                    port=probe_config.get("port", PORT),
                ) if probe_config.get("httpGet") else None,
            )

        # Add liveness probe (from health check)
        if is_server and container_context.server_health_check:
            health_config = container_context.server_health_check
            container.liveness_probe = run_v2.Probe(
                initial_delay_seconds=health_config.get("initialDelaySeconds", 0),
                timeout_seconds=health_config.get("timeoutSeconds", 1),
                period_seconds=health_config.get("periodSeconds", 10),
                failure_threshold=health_config.get("failureThreshold", 3),
                http_get=run_v2.HTTPGetAction(
                    path=health_config.get("path", "/health"),
                    port=health_config.get("port", PORT),
                ) if health_config.get("httpGet") else None,
            )

        return container

    def launch_server(
        self,
        deployment_name: str,
        location_name: str,
        image: str,
        container_context: Optional[Dict] = None,
    ) -> ServerEndpoint:
        """
        Launch a Cloud Run service for a code location.
        """
        service_name = self._get_service_name(deployment_name, location_name)
        parent = self._get_parent()
        full_name = f"{parent}/services/{service_name}"

        logger.info(f"Launching Cloud Run service: {service_name}")

        # Merge container contexts
        location_container_context = CloudRunContainerContext.create_from_config(
            container_context or {}
        )
        merged_context = self._base_container_context.merge(location_container_context)

        # Build environment variables
        env_vars = {
            "DAGSTER_CURRENT_IMAGE": image,
        }

        # Build main container
        main_container = self._build_container_spec(
            image=image,
            container_context=merged_context,
            env_vars=env_vars,
            is_server=True,
        )

        # Build sidecar containers
        sidecars = []
        for sidecar_config in merged_context.server_sidecar_containers:
            sidecar = run_v2.Container(
                name=sidecar_config["name"],
                image=sidecar_config["image"],
                env=[
                    run_v2.EnvVar(name=k, value=str(v))
                    for k, v in sidecar_config.get("env", {}).items()
                ],
            )
            sidecars.append(sidecar)

        # Build revision template
        template = run_v2.RevisionTemplate(
            containers=[main_container] + sidecars,
            service_account=merged_context.service_account or self._code_server_service_account or "",
            timeout=f"{self._timeout}s",
            max_instance_request_concurrency=80,
        )

        # Add volumes
        if merged_context.volumes:
            template.volumes = []
            for vol_config in merged_context.volumes:
                volume = run_v2.Volume(name=vol_config["name"])
                # Add volume source based on type
                if "gcs" in vol_config:
                    volume.gcs = run_v2.GCSVolumeSource(
                        bucket=vol_config["gcs"]["bucket"],
                        read_only=vol_config["gcs"].get("readOnly", False),
                    )
                elif "nfs" in vol_config:
                    volume.nfs = run_v2.NFSVolumeSource(
                        server=vol_config["nfs"]["server"],
                        path=vol_config["nfs"]["path"],
                        read_only=vol_config["nfs"].get("readOnly", False),
                    )
                elif "secret" in vol_config:
                    volume.secret = run_v2.SecretVolumeSource(
                        secret=vol_config["secret"]["secretName"],
                    )
                template.volumes.append(volume)

        # Add VPC connector
        if merged_context.vpc_connector:
            template.vpc_access = run_v2.VpcAccess(
                connector=merged_context.vpc_connector,
                egress=merged_context.vpc_egress or "PRIVATE_RANGES_ONLY",
            )

        # Create service
        service = run_v2.Service(
            template=template,
            ingress=run_v2.IngressTraffic.INGRESS_TRAFFIC_ALL,
            labels={
                "dagster-deployment": deployment_name.lower().replace("_", "-"),
                "dagster-location": location_name.lower().replace("_", "-"),
                "managed-by": "dagster-cloud-agent",
                **{label["key"]: label.get("value", "") for label in merged_context.server_labels},
            },
        )

        # Scaling configuration
        replica_count = merged_context.server_resources.get("replica_count")
        if replica_count or self._max_instances or self._min_instances:
            service.scaling = run_v2.ServiceScaling(
                min_instance_count=replica_count or self._min_instances,
                max_instance_count=replica_count or self._max_instances,
            )

        try:
            # Check if service exists
            try:
                existing = self._services_client.get_service(name=full_name)
                logger.info(f"Updating existing service: {service_name}")
                operation = self._services_client.update_service(service=service, update_mask=None)
            except gcp_exceptions.NotFound:
                logger.info(f"Creating new service: {service_name}")
                request = run_v2.CreateServiceRequest(
                    parent=parent,
                    service=service,
                    service_id=service_name,
                )
                operation = self._services_client.create_service(request=request)

            # Wait for operation
            result = operation.result(timeout=self._run_timeout)
            service_url = result.uri
            logger.info(f"Service ready: {service_url}")

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
            if self._enable_debug_logs:
                self._log_debug_info(service_name)
            raise

    def _log_debug_info(self, service_name: str):
        """Log debug information for troubleshooting."""
        try:
            full_name = f"{self._get_parent()}/services/{service_name}"
            service = self._services_client.get_service(name=full_name)
            logger.info(f"Service status: {service.conditions}")
            logger.info(f"Service URL: {service.uri}")
        except Exception as e:
            logger.error(f"Failed to get debug info: {e}")

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
            operation = self._services_client.delete_service(name=full_name)
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
            service = self._services_client.get_service(name=full_name)

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

    Mirrors AWS ECS run launcher with full feature parity.
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
        secrets: Optional[Sequence[Any]] = None,
        secrets_tags: Optional[Sequence[str]] = None,
        env_vars: Optional[Sequence[str]] = None,
        run_resources: Optional[Mapping[str, Any]] = None,
        run_sidecar_containers: Optional[Sequence[Mapping[str, Any]]] = None,
        run_labels: Optional[Sequence[Mapping[str, Optional[str]]]] = None,
        volumes: Optional[Sequence[Mapping[str, Any]]] = None,
        volume_mounts: Optional[Sequence[Mapping[str, Any]]] = None,
        vpc_connector: Optional[str] = None,
        vpc_egress: Optional[str] = None,
    ):
        self._project_id = project_id
        self._region = region
        self._service_account = service_account
        self._cpu = cpu
        self._memory = memory
        self._timeout = timeout
        self._inst_data = check.opt_dict_param(inst_data, "inst_data", key_type=str)

        # Initialize clients
        self._jobs_client = run_v2.JobsClient()
        self._executions_client = run_v2.ExecutionsClient()
        self._secrets_client = secretmanager.SecretManagerServiceClient()

        # Container context
        self._base_container_context = CloudRunContainerContext(
            secrets=secrets,
            secrets_tags=secrets_tags,
            env_vars=env_vars,
            run_resources=run_resources,
            service_account=service_account,
            run_sidecar_containers=run_sidecar_containers,
            run_labels=run_labels,
            volumes=volumes,
            volume_mounts=volume_mounts,
            vpc_connector=vpc_connector,
            vpc_egress=vpc_egress,
        )

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
            "project_id": Field(StringSource, description="GCP project ID"),
            "region": Field(StringSource, description="GCP region"),
            "service_account": Field(StringSource, is_required=False, description="Service account for run jobs"),
            "cpu": Field(StringSource, is_required=False, default_value="1", description="CPU limit"),
            "memory": Field(StringSource, is_required=False, default_value="2Gi", description="Memory limit"),
            "timeout": Field(IntSource, is_required=False, default_value=3600, description="Job timeout"),
            "secrets": Field(Array(ScalarUnion(scalar_type=str, non_scalar_schema={"name": StringSource, "valueFrom": StringSource})), is_required=False),
            "secrets_tags": Field(Array(StringSource), is_required=False),
            "env_vars": Field(Array(StringSource), is_required=False),
            "run_resources": Field(dict, is_required=False),
            "run_sidecar_containers": Field(Array(dict), is_required=False),
            "run_labels": Field(Array(dict), is_required=False),
            "volumes": Field(Array(dict), is_required=False),
            "volume_mounts": Field(Array(dict), is_required=False),
            "vpc_connector": Field(StringSource, is_required=False),
            "vpc_egress": Field(StringSource, is_required=False),
        }

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    def _get_job_name(self, run_id: str) -> str:
        """Generate Cloud Run job name for a run."""
        return f"run-{run_id[:57]}"

    def _get_parent(self) -> str:
        """Get the parent path for Cloud Run resources."""
        return f"projects/{self._project_id}/locations/{self._region}"

    def launch_run(self, context) -> None:
        """Launch a Cloud Run job to execute a Dagster run."""
        run = context.dagster_run
        run_id = run.run_id

        job_name = self._get_job_name(run_id)
        parent = self._get_parent()

        logger.info(f"Launching Cloud Run job for run {run_id}: {job_name}")

        # Get image
        job_origin = check.not_none(context.job_code_origin)
        repository_origin = job_origin.repository_origin
        image = repository_origin.container_image

        if not image:
            raise Exception(f"No container image found for run {run_id}")

        # Build command
        from dagster._core.execution.api import ExecuteRunArgs

        stripped_repository_origin = repository_origin._replace(container_context={})
        executable_path = repository_origin.executable_path
        if executable_path:
            fixed_entry_point = [executable_path, "-m", "dagster"]
            stripped_repository_origin = stripped_repository_origin._replace(entry_point=fixed_entry_point)

        stripped_job_origin = job_origin._replace(repository_origin=stripped_repository_origin)

        args = ExecuteRunArgs(
            job_origin=stripped_job_origin,
            run_id=run_id,
            instance_ref=self._instance.get_ref(),
        )

        command = args.get_command_args()
        logger.info(f"Run {run_id} command: {command}")

        # Merge container contexts
        location_container_context = CloudRunContainerContext.create_from_config(
            repository_origin.container_context or {}
        )
        merged_context = self._base_container_context.merge(location_container_context)

        # Build container
        all_env = merged_context.get_environment_dict()
        secrets_dict = merged_context.get_secrets_dict(self._secrets_client, self._project_id)
        all_env = merge_dicts(all_env, secrets_dict)

        container = run_v2.Container(
            image=image,
            command=command,
            env=[run_v2.EnvVar(name=k, value=str(v)) for k, v in all_env.items()],
            resources=run_v2.ResourceRequirements(
                limits={
                    "cpu": merged_context.run_resources.get("cpu", self._cpu),
                    "memory": merged_context.run_resources.get("memory", self._memory),
                },
            ),
        )

        # Add volume mounts
        if merged_context.volume_mounts:
            container.volume_mounts = [
                run_v2.VolumeMount(name=vm["name"], mount_path=vm["mountPath"])
                for vm in merged_context.volume_mounts
            ]

        # Build template
        template = run_v2.ExecutionTemplate(
            template=run_v2.TaskTemplate(
                containers=[container],
                service_account=merged_context.service_account or self._service_account or "",
                max_retries=0,
                timeout=f"{self._timeout}s",
            ),
        )

        # Add volumes
        if merged_context.volumes:
            template.template.volumes = []
            for vol_config in merged_context.volumes:
                volume = run_v2.Volume(name=vol_config["name"])
                if "gcs" in vol_config:
                    volume.gcs = run_v2.GCSVolumeSource(
                        bucket=vol_config["gcs"]["bucket"],
                        read_only=vol_config["gcs"].get("readOnly", False),
                    )
                elif "nfs" in vol_config:
                    volume.nfs = run_v2.NFSVolumeSource(
                        server=vol_config["nfs"]["server"],
                        path=vol_config["nfs"]["path"],
                        read_only=vol_config["nfs"].get("readOnly", False),
                    )
                template.template.volumes.append(volume)

        # Add VPC connector
        if merged_context.vpc_connector:
            template.template.vpc_access = run_v2.VpcAccess(
                connector=merged_context.vpc_connector,
                egress=merged_context.vpc_egress or "PRIVATE_RANGES_ONLY",
            )

        # Create job
        job = run_v2.Job(
            template=template,
            labels={
                "dagster-run-id": run_id,
                "managed-by": "dagster-cloud-agent",
                **{label["key"]: label.get("value", "") for label in merged_context.run_labels},
            },
        )

        try:
            request = run_v2.CreateJobRequest(parent=parent, job=job, job_id=job_name)
            operation = self._jobs_client.create_job(request=request)
            created_job = operation.result(timeout=60)

            logger.info(f"Job created: {created_job.name}")

            # Execute the job
            exec_request = run_v2.RunJobRequest(name=created_job.name)
            exec_operation = self._jobs_client.run_job(request=exec_request)

            logger.info(f"Job execution started for run {run_id}")

        except Exception as e:
            logger.error(f"Failed to launch Cloud Run job for run {run_id}: {e}")
            raise

    def terminate(self, run_id: str) -> bool:
        """Terminate a running Cloud Run job."""
        job_name = self._get_job_name(run_id)
        full_name = f"{self._get_parent()}/jobs/{job_name}"

        logger.info(f"Terminating Cloud Run job: {job_name}")

        try:
            operation = self._jobs_client.delete_job(name=full_name)
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
            job = self._jobs_client.get_job(name=full_name)
            return {
                "job_name": job_name,
                "project": self._project_id,
                "region": self._region,
                "status": str(job.conditions[0].state) if job.conditions else "UNKNOWN",
                "console_url": f"https://console.cloud.google.com/run/jobs/details/{self._region}/{job_name}?project={self._project_id}",
            }
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
            job = self._jobs_client.get_job(name=full_name)

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
