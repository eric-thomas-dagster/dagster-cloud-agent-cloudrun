# Google Cloud Deployment Manager Templates

This directory contains **Google Cloud Deployment Manager** templates for deploying the Dagster Cloud agent. Deployment Manager is Google's native Infrastructure as Code (IaC) solution, similar to Azure ARM/Bicep templates.

## 📋 Files

- **`dagster-agent.jinja`** - Main template (like ARM/Bicep template)
- **`dagster-agent.yaml`** - Configuration file with parameters (example)

## 🆚 Terraform vs Deployment Manager

| Feature | Terraform | Deployment Manager |
|---------|-----------|-------------------|
| **Popularity** | ⭐⭐⭐⭐⭐ Most popular | ⭐⭐ Less common |
| **Multi-cloud** | ✅ Yes | ❌ GCP only |
| **Community** | Large | Smaller |
| **Google's Recommendation** | ✅ (Cloud Foundation Toolkit uses Terraform) | ⚠️ Supported but not promoted |
| **State Management** | Excellent | Good |
| **Learning Curve** | Moderate | Moderate |

**Recommendation:** Use **Terraform** for new projects. Use **Deployment Manager** if:
- You need a native GCP solution
- You're migrating from ARM/Bicep and want similar syntax
- Your organization requires native cloud tools

## 🚀 Quick Start

### Prerequisites

```bash
# Enable required APIs
gcloud services enable \
  deploymentmanager.googleapis.com \
  run.googleapis.com \
  secretmanager.googleapis.com \
  iam.googleapis.com
```

### Step 1: Prepare Configuration

```bash
# Copy the example config
cp dagster-agent.yaml dagster-agent-config.yaml

# Edit with your values
nano dagster-agent-config.yaml
```

**Required values:**
- `projectId`: Your GCP project ID
- `agentImage`: Your agent Docker image URL
- `dagsterCloudApiToken`: Your Dagster Cloud agent token (base64 encoded)
- `dagsterCloudOrgId`: Your organization ID (base64 encoded)

**Encoding secrets to base64:**

```bash
# Encode your agent token
echo -n "YOUR_TOKEN_HERE" | base64

# Encode your org ID
echo -n "YOUR_ORG_ID" | base64

# Encode deployment name (default: prod)
echo -n "prod" | base64
```

### Step 2: Preview Deployment

```bash
gcloud deployment-manager deployments create dagster-agent \
  --config dagster-agent-config.yaml \
  --preview
```

### Step 3: Deploy

```bash
gcloud deployment-manager deployments create dagster-agent \
  --config dagster-agent-config.yaml
```

### Step 4: Verify

```bash
# Check deployment status
gcloud deployment-manager deployments describe dagster-agent

# View agent logs
gcloud run services logs read dagster-agent --region=us-central1
```

## 🔧 Management Commands

### Update Deployment

```bash
gcloud deployment-manager deployments update dagster-agent \
  --config dagster-agent-config.yaml
```

### Delete Deployment

```bash
gcloud deployment-manager deployments delete dagster-agent
```

### View Outputs

```bash
gcloud deployment-manager deployments describe dagster-agent \
  --format="value(outputs)"
```

## 📦 What Gets Created

The template creates:

1. **Service Accounts**
   - `dagster-agent` - For the agent service
   - `dagster-workloads` - For code servers and run jobs

2. **IAM Bindings**
   - Agent can manage Cloud Run services/jobs
   - Agent can access Secret Manager
   - Agent can act as workloads service account

3. **Secrets** (in Secret Manager)
   - Dagster Cloud API token
   - Organization ID
   - Deployment name

4. **Cloud Run Service**
   - Agent service (min instances = 1, always-on)

## 🔍 Troubleshooting

### Permission Denied Errors

Ensure your user account has these roles:
```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member=user:YOUR_EMAIL \
  --role=roles/deploymentmanager.editor

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member=user:YOUR_EMAIL \
  --role=roles/iam.serviceAccountAdmin
```

### Deployment Fails with "Resource already exists"

Delete the existing deployment first:
```bash
gcloud deployment-manager deployments delete dagster-agent
```

Or update it instead of creating:
```bash
gcloud deployment-manager deployments update dagster-agent \
  --config dagster-agent-config.yaml
```

### Base64 Encoding Issues

Secrets must be base64 encoded in the config file. Don't include newlines:
```bash
# Correct
echo -n "my-secret" | base64

# Wrong (includes newline)
echo "my-secret" | base64
```

## 📚 Resources

- [Deployment Manager Documentation](https://cloud.google.com/deployment-manager/docs)
- [Jinja2 Template Reference](https://cloud.google.com/deployment-manager/docs/configuration/templates/using-jinja)
- [Cloud Run Resource Type](https://cloud.google.com/deployment-manager/docs/configuration/supported-resource-types)

## 🔄 Migrating from Terraform

If you have an existing Terraform deployment:

1. **Export Terraform state** to understand current resources
2. **Delete Terraform resources** (or import them into Deployment Manager)
3. **Deploy with Deployment Manager** using the templates here

Note: Deployment Manager and Terraform manage state independently. Don't mix them for the same resources.

## 🆚 Comparison with Azure ARM/Bicep

| Feature | ARM/Bicep | Deployment Manager |
|---------|-----------|-------------------|
| **Language** | JSON/Bicep DSL | YAML/Python/Jinja2 |
| **Parameters** | Built-in | Via properties |
| **Outputs** | Native | Native |
| **Modules** | Native | Via imports |
| **What-If** | ✅ Yes | ✅ Yes (--preview) |
| **State Management** | Azure-managed | GCP-managed |

Both provide similar capabilities - this template mirrors the ARM/Bicep structure from the ACA agent!
