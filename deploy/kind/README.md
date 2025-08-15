# Moonlink on kind cluster — Setup & Cleanup

A tiny, reproducible workflow to **deploy** Moonlink to a local [kind](https://kind.sigs.k8s.io/) cluster and **tear it down** cleanly. Two scripts only:

- `local_setup_script.sh` — creates/ensures a kind cluster, loads the `moonlink:dev` image, applies manifests, and waits for rollout.
- `cleanup.sh` — deletes resources from your manifests, and can optionally nuke the namespace and/or the entire kind cluster.

---

## Prerequisites

- Docker
- kind
- kubectl
- yq

> Verify:
> ```bash
> docker --version && kind --version && kubectl version --client=true --output=yaml && yq --version
> ```

---

## Quick Start

```bash
# 1) Create/ensure cluster & namespace, build & load image, deploy manifests, wait for rollout
./deploy/kind/local_setup_script.sh

# 2) See what's running
kubectl get pods,svc -n moonlink

# 3) Clean up (resources only)
./deploy/kind/cleanup.sh
```

## Environment Variables - Setup & Cleanup

- **CLUSTER**  
  Name of the Kubernetes cluster.  
  *Default:* `kind-moonlink-dev`

- **NS**  
  Namespace in the cluster.  
  *Default:* `moonlink`

- **MANIFEST_DIR**  
  Directory containing Kubernetes manifests.  
  *Default:* `deploy/kind`

- **WAIT_TIMEOUT**  
  Maximum time to wait for deployment to be ready.  
  *Default:* `60s`

- **DEPLOYMENT_CONFIG_FILE**  
  Path to the deployment manifest file.  
  *Default:* `${MANIFEST_DIR}/deployment/moonlink_deployment.yaml`

- **SERVICE_CONFIG_FILE**  
  Path to the service manifest file.  
  *Default:* `${MANIFEST_DIR}/service/moonlink_service.yaml`

## Additional Settings

Use these flags with the cleanup script:

```bash
# Delete the entire kind cluster
NUKE_CLUSTER=true ./deploy/kind/cleanup.sh

# Delete the 'moonlink' namespace
NUKE_NAMESPACE=true ./deploy/kind/cleanup.sh
```

---

## Manifest Structure

The deployment uses the following manifests:
- `/deploy/kind/deplyment/moonlink_deployment.yaml` - Main deployment with moonlink, nginx, and postgres containers
- `/deploy/kind/service/moonlink_service.yaml` - Service to expose the deployment ports