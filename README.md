# Dagster Cloud Agent for Google Cloud Run

**Deploy Dagster+ hybrid agents on Google Cloud Run with automatic code server and run job management.**

This repository provides a complete solution for running Dagster Cloud (Dagster+) on Google Cloud Run, including:
- ✅ Custom agent that automatically creates code servers on Cloud Run
- ✅ Ephemeral run workers as Cloud Run Jobs
- ✅ Ready-to-deploy Terraform templates
- ✅ Secure secrets management via Google Secret Manager
- ✅ Production-ready with auto-scaling and scale-to-zero

---

## 🚀 Quick Start

### Prerequisites

- Google Cloud Project with billing enabled
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) installed
- [Terraform](https://www.terraform.io/downloads) installed
- Docker installed
- Dagster Cloud account with an agent token

### Step 1: Enable Required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  secretmanager.googleapis.com \
  iam.googleapis.com \
  --project=YOUR_PROJECT_ID
```

### Step 2: Build and Push the Agent Image

```bash
# Authenticate with your container registry (GitHub Container Registry example)
echo $GITHUB_TOKEN | docker login ghcr.io -u YOUR_USERNAME --password-stdin

# Build the agent image
docker build -t dagster-cloudrun-agent:latest .

# Tag and push
docker tag dagster-cloudrun-agent:latest ghcr.io/YOUR_USERNAME/dagster-cloudrun-agent:latest
docker push ghcr.io/YOUR_USERNAME/dagster-cloudrun-agent:latest
```

### Step 3: Deploy with Terraform

```bash
cd infra/terraform

# Copy the example variables file
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars with your values
# - project_id
# - agent_image
# - dagster_cloud_api_token
# - dagster_cloud_org_id

# Initialize Terraform
terraform init

# Review the plan
terraform plan

# Deploy
terraform apply
```

### Step 4: Verify Agent Connection

Check your Dagster Cloud deployment to see the agent status:
- Go to https://YOUR_ORG.dagster.cloud/YOUR_DEPLOYMENT/agents
- You should see your Cloud Run agent connected

---

## 📁 Project Structure

```
├── app/
│   ├── cloudrun_launcher.py  # Custom Cloud Run launchers (code servers & jobs)
│   ├── dagster.yaml           # Agent configuration
│   └── entrypoint.py          # Fetches secrets, starts agent
├── infra/
│   └── terraform/
│       ├── main.tf            # Main Terraform configuration
│       ├── variables.tf       # Input variables
│       ├── outputs.tf         # Output values
│       └── terraform.tfvars.example  # Example configuration
├── Dockerfile                 # Builds custom agent image
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

---

## 🏗️ Architecture

**Components:**
- **Agent (Cloud Run)**: Runs 24/7 with min instances = 1 (~$15-20/month)
- **Code Servers (Cloud Run Services)**: Auto-created per code location, scale to zero when idle
- **Run Workers (Cloud Run Jobs)**: Ephemeral, created per job execution, auto-cleanup

**vs. GKE:** ~60-70% cost savings with no cluster management overhead!

**Key Features:**
- Custom `CloudRunUserCodeLauncher` creates code servers as Cloud Run services
- Custom `CloudRunRunLauncher` creates ephemeral run jobs
- Auto-scaling and scale-to-zero for cost optimization
- Built-in HTTPS ingress for code servers
- Service accounts with least-privilege IAM
- Secrets stored in Google Secret Manager

---

## 💰 Cost Estimate

**Example: 1 agent + 3 code locations + 10 runs/day**

| Component | Configuration | Monthly Cost* |
|-----------|--------------|---------------|
| Agent | 1 vCPU, 1GB RAM, always-on | ~$15-20 |
| Code Servers (3) | 1 vCPU, 2GB RAM each, scale to zero | ~$30-60 |
| Run Jobs | 1 vCPU, 2GB RAM, ephemeral | ~$5-10 |
| **Total** | | **~$50-90/month** |

**vs. GKE minimum:** ~$150-200/month (60-70% savings!)

*Assumes code servers are idle 80% of the time (scale to zero)
*Actual costs vary based on usage patterns

---

## 📦 Code Location Requirements

Your Dagster code locations (user code) must include both `dagster` and `dagster-cloud` with matching versions:

```toml
# pyproject.toml
dependencies = [
    "dagster==1.12.6",
    "dagster-cloud==1.12.6",
    # ... your other dependencies
]
```

**Why both?**
- `dagster`: Core framework for defining jobs and assets
- `dagster-cloud`: Required for run execution (GraphQL storage, instance config)

The versions should match the agent base image version (currently `1.12.6`).

---

## 🔒 Security

- **Secrets**: Stored in Google Secret Manager (never in code or Terraform state)
- **Service Accounts**: Separate SAs for agent and workloads with least-privilege IAM
- **Network**: Cloud Run services use HTTPS by default
- **Authentication**: Uses Google Cloud Application Default Credentials

### IAM Roles Required

**Agent Service Account:**
- `roles/run.admin` - Create/manage Cloud Run services and jobs
- `roles/iam.serviceAccountUser` - Act as workloads service account
- `roles/secretmanager.secretAccessor` - Read secrets

**Workloads Service Account:**
- `roles/artifactregistry.reader` - Pull private images (if using Artifact Registry)
- Custom roles as needed by your pipelines (e.g., BigQuery, GCS access)

---

## 🛠️ Configuration

### Agent Configuration

Edit `app/dagster.yaml` to customize:
- CPU/memory for code servers and run jobs
- Auto-scaling parameters (min/max instances)
- Timeouts
- Logging levels

### Terraform Configuration

Edit `infra/terraform/terraform.tfvars`:
- `project_id` - Your GCP project
- `region` - GCP region (e.g., `us-central1`)
- `agent_image` - Your agent Docker image
- `dagster_cloud_api_token` - Agent token from Dagster Cloud
- `dagster_cloud_org_id` - Your organization ID
- `dagster_cloud_deployment_name` - Deployment name (default: `prod`)

---

## 🔍 Monitoring and Troubleshooting

### View Agent Logs

```bash
# Via gcloud
gcloud run services logs read dagster-agent --region=us-central1

# Via Cloud Console
# https://console.cloud.google.com/run/detail/REGION/dagster-agent/logs
```

### View Code Server Logs

```bash
# List code servers
gcloud run services list --region=us-central1 | grep dagster

# View logs for a specific code server
gcloud run services logs read dagster-DEPLOYMENT-LOCATION --region=us-central1
```

### View Run Job Logs

```bash
# List jobs
gcloud run jobs list --region=us-central1 | grep run-

# View logs for a specific run
gcloud run jobs logs read run-XXXXXXXXX --region=us-central1
```

### Common Issues

**Agent not connecting:**
- Check agent logs for errors
- Verify Secret Manager secrets are populated
- Ensure agent service account has `secretAccessor` role

**Code servers not starting:**
- Check agent has `run.admin` IAM role
- Verify image exists and is accessible
- Check Cloud Run service creation logs

**Runs failing:**
- Ensure code location has `dagster-cloud` dependency
- Check run job logs in Cloud Console
- Verify workloads service account has necessary permissions

---

## 🚢 Deployment Strategies

### CI/CD Integration

Example GitHub Actions workflow:

```yaml
name: Deploy Agent

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v1
        with:
          credentials_json: ${{ secrets.GCP_CREDENTIALS }}

      - name: Build and push image
        run: |
          docker build -t gcr.io/${{ secrets.GCP_PROJECT }}/dagster-agent:${{ github.sha }} .
          docker push gcr.io/${{ secrets.GCP_PROJECT }}/dagster-agent:${{ github.sha }}

      - name: Deploy to Cloud Run
        run: |
          gcloud run services update dagster-agent \
            --image=gcr.io/${{ secrets.GCP_PROJECT }}/dagster-agent:${{ github.sha }} \
            --region=us-central1
```

### Multi-Region Deployment

Deploy agents in multiple regions for high availability:

```bash
# Deploy to us-central1
terraform apply -var="region=us-central1"

# Deploy to europe-west1
terraform apply -var="region=europe-west1"
```

---

## 🤝 Contributing

This is a community-maintained solution. Contributions welcome!

---

## 📚 Resources

- **Dagster Cloud Docs**: https://docs.dagster.io/dagster-cloud
- **Dagster Slack**: https://dagster.io/slack
- **Google Cloud Run Docs**: https://cloud.google.com/run/docs
- **Azure Container Apps Agent** (sister project): https://github.com/eric-thomas-dagster/aca-agent

---

## ⚖️ License

MIT License - See LICENSE file for details
