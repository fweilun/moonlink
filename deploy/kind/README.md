## Moonlink on kind — Deployment Guide

### Overview
This code and configuration deploy Moonlink to a local kind cluster. It is revised from `deploy/gcp/dev` for a local kind environment.

### Setup
```bash
# Ensure kind cluster exists (default: kind-moonlink-dev)
kind get clusters | grep -qx kind-moonlink-dev || kind create cluster --name kind-moonlink-dev

# Ensure namespace exists (default: moonlink)
kubectl get ns moonlink >/dev/null 2>&1 || kubectl create ns moonlink

# Build and load the local image on macOS with arm (default image tag: moonlink:dev)
docker build -t moonlink:dev -f Dockerfile.aarch64 .
kind load docker-image moonlink:dev --name kind-moonlink-dev

# Apply manifests (default manifest dir: deploy/kind)
kubectl apply -f deploy/kind/deployment/moonlink_deployment.yaml -n moonlink
kubectl apply -f deploy/kind/service/moonlink_service.yaml -n moonlink

# Check the deployment
kubectl get pods,svc -n moonlink
```

### Cleanup
```bash
# Option A: delete the entire kind cluster
kind delete cluster --name kind-moonlink-dev

# Option B: delete only the 'moonlink' namespace
kubectl delete ns moonlink --ignore-not-found

# Option C: delete only the deployed resources (keep namespace)
kubectl delete -n moonlink -f deploy/kind/deployment/moonlink_deployment.yaml --ignore-not-found --wait=true
[ -f deploy/kind/service/moonlink_service.yaml ] && kubectl delete -n moonlink -f deploy/kind/service/moonlink_service.yaml --ignore-not-found --wait=true

# Show what's left (if any)
kubectl get all,cm,secret,pvc -n moonlink || true
```