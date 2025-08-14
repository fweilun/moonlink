# Moonlink on kind — Setup & Cleanup

A tiny, reproducible workflow to **deploy** Moonlink to a local [kind](https://kind.sigs.k8s.io/) cluster and **tear it down** cleanly. Two scripts only:

- `local_setup_script.sh` — creates/ensures a kind cluster, loads the `moonlink:dev` image, applies manifests, and waits for rollout.
- `cleanup.sh` — deletes resources from your manifests, and can optionally nuke the namespace and/or the entire kind cluster.

---

## Prerequisites

- **Docker** (v20+)  
- **kind** (v0.20+)  
- **kubectl** (v1.27+) with access to your local kind cluster  
- **yq** (for reading the deployment name from YAML in the setup script)

> Verify:
> ```bash
> docker --version && kind --version && kubectl version --client=true --output=yaml && yq --version
> ```

---

## Quick Start

```bash
# 1) Create/ensure cluster, build & load image, deploy manifests, wait for rollout
./deploy/kind/local_setup_script.sh

# 2) See what's running
kubectl get pods,svc -n moonlink

# 3) Clean up (resources only)
./deploy/kind/cleanup.sh
```

## Additional Settings

Use these flags with the cleanup script:

```bash
# Delete the entire kind cluster (takes precedence if both are set)
NUKE_CLUSTER=true ./deploy/kind/cleanup.sh

# Delete only the 'moonlink' namespace
NUKE_NAMESPACE=true ./deploy/kind/cleanup.sh
```

---

## Manifest Structure

The deployment uses the following manifests:
- `deplyment/moonlink_deployment.yaml` - Main deployment with moonlink, nginx, and postgres containers
- `service/moonlink_service.yaml` - Service to expose the deployment ports

> **Note**: The service selector currently doesn't match the deployment labels. You may need to update the service selector from `app: moonlink-deployment` to `app: moonlink-dev` to match the deployment.